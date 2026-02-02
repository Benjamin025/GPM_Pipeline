"""
NASA GPM HALF-HOURLY TO MONTHLY PROCESSING WORKFLOW - CORRECTED STRUCTURE
Downloads 30-minute HDF5 files (1999-2026), processes to monthly GeoTIFFs
Properly handles year/day_of_year/ directory structure
FIXED: Correct filename generation with proper end minutes
"""

import os
import urllib.request
import datetime
import time
import http.cookiejar
import urllib.error
from pathlib import Path
import getpass
import h5py
import numpy as np
import calendar
from datetime import datetime as dt, timedelta
import traceback
import warnings
import xarray as xr
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
warnings.filterwarnings('ignore')

# Try to import processing dependencies
try:
    import rasterio
    from rasterio.transform import from_origin
    from rasterio.crs import CRS
    HAS_RASTERIO = True
except ImportError:
    print("⚠️ rasterio not installed. Install with: pip install rasterio")
    HAS_RASTERIO = False

try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

class GPMHalfHourlyToMonthlyWorkflow:
    """Process GPM half-hourly data (1999-2026) to monthly aggregates"""
    
    def __init__(self, base_dir=None):
        # Configuration for HALF-HOURLY (Early Run) product
        self.BASE_URL = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHHE.07"
        self.VERSION = "07B"
        
        # Base directory structure
        if base_dir is None:
            self.base_dir = Path.home() / "Documents" / "Benjamin" / "GPM" / "GPM_gee" / "GPM_NASA_HalfHourly_1999_2026"
        else:
            self.base_dir = Path(base_dir)
        
        # Create directory structure (mirroring NASA's structure)
        self.dirs = {
            'raw_halfhourly': self.base_dir / "raw_halfhourly",
            'geotiffs': self.base_dir / "geotiffs",
            'previews': self.base_dir / "previews",
            'metadata': self.base_dir / "metadata",
            'logs': self.base_dir / "logs",
            'temp': self.base_dir / "temp"
        }
        
        for name, path in self.dirs.items():
            path.mkdir(parents=True, exist_ok=True)
            print(f"  Created: {name}/")
        
        # Authentication
        self.username = None
        self.password = None
        self.opener = None
        
        print(f"\n🌧️ NASA GPM HALF-HOURLY TO MONTHLY WORKFLOW (1999-2026)")
        print(f"📁 Base directory: {self.base_dir}")
        print("="*70)
    
    # =========================================================================
    # AUTHENTICATION METHODS
    # =========================================================================
    
    def _setup_authentication(self):
        """Setup authentication for NASA Earthdata"""
        print("\n🔐 NASA EARTHDATA AUTHENTICATION")
        print("="*40)
        
        # Try to get credentials from .netrc
        netrc_path = Path.home() / ".netrc"
        if netrc_path.exists():
            try:
                with open(netrc_path, 'r') as f:
                    content = f.read()
                    if 'urs.earthdata.nasa.gov' in content:
                        lines = content.split('\n')
                        for i, line in enumerate(lines):
                            if 'urs.earthdata.nasa.gov' in line:
                                if i+1 < len(lines) and 'login' in lines[i+1]:
                                    self.username = lines[i+1].split()[1]
                                if i+2 < len(lines) and 'password' in lines[i+2]:
                                    self.password = lines[i+2].split()[1]
                                break
                
                if self.username and self.password:
                    print(f"✅ Using credentials from .netrc: {self.username}")
            except:
                pass
        
        # If not in .netrc, ask user
        if not self.username or not self.password:
            print("\nEnter NASA Earthdata credentials:")
            print("Create account: https://urs.earthdata.nasa.gov/users/new")
            print("-" * 40)
            
            self.username = input("Username: ").strip()
            self.password = getpass.getpass("Password: ")
        
        # Create authentication handler
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        
        # Add credentials for all NASA domains
        domains = [
            'gpm1.gesdisc.eosdis.nasa.gov',
            'urs.earthdata.nasa.gov',
            'disc.gsfc.nasa.gov'
        ]
        
        for domain in domains:
            password_mgr.add_password(None, f'https://{domain}/', self.username, self.password)
        
        # Create handlers
        auth_handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        cookie_jar = http.cookiejar.CookieJar()
        cookie_handler = urllib.request.HTTPCookieProcessor(cookie_jar)
        
        # Create opener
        self.opener = urllib.request.build_opener(auth_handler, cookie_handler)
        self.opener.addheaders = [
            ('User-Agent', 'Mozilla/5.0 (NASA-GPM-HalfHourly-Workflow/1.0)'),
            ('Accept', '*/*'),
        ]
        
        # Install as default
        urllib.request.install_opener(self.opener)
        
        print("✅ Authentication setup complete")
    
    def _test_authentication(self):
        """Test authentication"""
        print("\n🔍 Testing authentication...")
        
        test_url = f"{self.BASE_URL}/1999/001/"  # Test with 1999-01-01
        
        try:
            request = urllib.request.Request(test_url)
            response = urllib.request.urlopen(request, timeout=10)
            
            if response.getcode() == 200:
                print("✅ Authentication successful")
                return True
            else:
                print(f"⚠️ HTTP {response.getcode()}")
                return False
                
        except urllib.error.HTTPError as e:
            if e.code == 401:
                print("❌ Authentication failed (401)")
                return False
            else:
                print(f"⚠️ HTTP Error {e.code}")
                return False
        except Exception as e:
            print(f"⚠️ Connection error: {e}")
            return False
    
    # =========================================================================
    # HALF-HOURLY FILE DOWNLOAD METHODS - CORRECTED STRUCTURE
    # =========================================================================
    
    def get_halfhourly_filename(self, year, day_of_year, halfhour_index):
        """Generate correct filename for half-hourly GPM Early Run files"""
        # Calculate date from day_of_year
        date_obj = dt(year, 1, 1) + timedelta(days=day_of_year - 1)
        month = date_obj.month
        day = date_obj.day
        
        # Convert month and day to zero-padded strings
        month_str = f"{month:02d}"
        day_str = f"{day:02d}"
        
        # Calculate start time (every 30 minutes: 00, 30)
        hour = halfhour_index // 2
        minute_start = "00" if (halfhour_index % 2) == 0 else "30"
        
        # Calculate end time (30 minutes after start)
        # End hour is same as start hour for 30-minute periods
        end_hour = hour
        
        # Calculate end minute:
        # If start at :00, end at :29
        # If start at :30, end at :59
        if minute_start == "00":
            minute_end = "29"
        else:  # minute_start == "30"
            minute_end = "59"
        
        # Special case: If starting at 23:30, end hour should still be 23 (not 24)
        # Actually, end_hour is already correct since it's same as start_hour
        
        # Create minutes code (0000, 0030, 0060, 0090, etc.)
        minutes_code = f"{halfhour_index * 30:04d}"
        
        # Create filename with correct pattern
        filename = (
            f"3B-HHR-E.MS.MRG.3IMERG."
            f"{year}{month_str}{day_str}-"
            f"S{hour:02d}{minute_start}00-"
            f"E{end_hour:02d}{minute_end}59."
            f"{minutes_code}.V{self.VERSION}.HDF5"
        )
        return filename
    
    def debug_filename_generation(self, year=1999, day_of_year=1):
        """Debug function to test filename generation"""
        print(f"\n🔍 DEBUGGING FILENAME GENERATION FOR {year}-{day_of_year:03d}")
        print("="*70)
        
        # Expected filenames from actual NASA URLs
        expected_filenames = [
            "3B-HHR-E.MS.MRG.3IMERG.19990101-S000000-E002959.0000.V07B.HDF5",
            "3B-HHR-E.MS.MRG.3IMERG.19990101-S003000-E005959.0030.V07B.HDF5", 
            "3B-HHR-E.MS.MRG.3IMERG.19990101-S010000-E012959.0060.V07B.HDF5",
            "3B-HHR-E.MS.MRG.3IMERG.19990101-S013000-E015959.0090.V07B.HDF5",
            "3B-HHR-E.MS.MRG.3IMERG.19990101-S020000-E022959.0120.V07B.HDF5",
            "3B-HHR-E.MS.MRG.3IMERG.19990101-S023000-E025959.0150.V07B.HDF5",
            "3B-HHR-E.MS.MRG.3IMERG.19990101-S230000-E232959.1380.V07B.HDF5",
            "3B-HHR-E.MS.MRG.3IMERG.19990101-S233000-E235959.1410.V07B.HDF5"
        ]
        
        # Corresponding halfhour indices for the above
        test_indices = [0, 1, 2, 3, 4, 5, 46, 47]
        
        print("Comparing generated filenames with expected NASA URLs:")
        print("-" * 80)
        
        all_match = True
        for i, halfhour_index in enumerate(test_indices):
            generated = self.get_halfhourly_filename(year, day_of_year, halfhour_index)
            expected = expected_filenames[i]
            
            if generated == expected:
                print(f"✅ Period {halfhour_index:2d}: MATCH")
                print(f"   Generated: {generated}")
            else:
                print(f"❌ Period {halfhour_index:2d}: MISMATCH")
                print(f"   Generated: {generated}")
                print(f"   Expected:  {expected}")
                
                # Show differences
                for j in range(min(len(generated), len(expected))):
                    if generated[j] != expected[j]:
                        start = max(0, j - 10)
                        end = min(len(generated), j + 10)
                        print(f"   Difference at position {j}:")
                        print(f"     Generated: ...{generated[start:end]}...")
                        print(f"     Expected:  ...{expected[start:end]}...")
                        print(f"                 {' ' * (j-start + 3)}^")
                        break
                all_match = False
            print()
        
        if all_match:
            print("🎉 ALL FILENAMES MATCH CORRECTLY!")
        else:
            print("⚠️ SOME FILENAMES DO NOT MATCH")
        
        return all_match
    
    def download_halfhourly_file(self, year, day_of_year, halfhour_index, max_retries=3):
        """Download a single half-hourly file from year/day_of_year/ directory"""
        
        # Create directory structure: year/day_of_year/
        year_dir = self.dirs['raw_halfhourly'] / str(year)
        day_dir = year_dir / f"{day_of_year:03d}"
        day_dir.mkdir(parents=True, exist_ok=True)
        
        # Get filename and URL
        filename = self.get_halfhourly_filename(year, day_of_year, halfhour_index)
        url = f"{self.BASE_URL}/{year}/{day_of_year:03d}/{filename}"
        local_path = day_dir / filename
        
        # Check if already exists
        if local_path.exists():
            size_mb = local_path.stat().st_size / (1024 * 1024)
            if size_mb > 0.5:  # HDF5 files are typically >0.5MB
                print(f"    Already exists: {filename} ({size_mb:.1f} MB)")
                return local_path
            else:
                print(f"    File too small, re-downloading: {filename}")
                local_path.unlink(missing_ok=True)
        
        # Download with retries
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = 2 ** attempt
                    print(f"    Retry {attempt}/{max_retries} after {wait_time}s...")
                    time.sleep(wait_time)
                
                # First check if file exists
                try:
                    head_req = urllib.request.Request(url, method='HEAD')
                    head_response = urllib.request.urlopen(head_req, timeout=10)
                    if head_response.getcode() != 200:
                        print(f"    ❌ File not found (HTTP {head_response.getcode()}): {filename}")
                        return None
                except urllib.error.HTTPError as e:
                    if e.code == 404:
                        print(f"    ❌ File not found (404): {filename}")
                        return None
                    else:
                        print(f"    ❌ HTTP Error {e.code}: {e.reason}")
                        return None
                
                # Download the file with progress
                print(f"    Downloading {filename}...")
                
                def progress_callback(count, block_size, total_size):
                    if total_size > 0:
                        percent = min(100, int(count * block_size * 100 / total_size))
                        if percent % 25 == 0 or percent == 100:
                            mb_downloaded = count * block_size / (1024 * 1024)
                            mb_total = total_size / (1024 * 1024) if total_size > 0 else 0
                            print(f"      {percent}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end='\r')
                
                temp_path = local_path.with_suffix('.downloading')
                urllib.request.urlretrieve(url, temp_path, reporthook=progress_callback)
                
                # Rename to final
                temp_path.rename(local_path)
                
                if local_path.exists():
                    size_mb = local_path.stat().st_size / (1024 * 1024)
                    if size_mb > 0.5:
                        print(f"\n    ✅ Downloaded ({size_mb:.1f} MB)")
                        return local_path
                    else:
                        print(f"\n    ❌ File too small ({size_mb:.1f} MB)")
                        local_path.unlink(missing_ok=True)
                        return None
                    
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print(f"    ❌ File not found (404): {filename}")
                    return None
                elif e.code == 401:
                    print(f"\n    ❌ Authentication failed (401)")
                    break
                elif e.code == 403:
                    print(f"\n    ❌ Access forbidden (403)")
                    break
                else:
                    print(f"\n    ❌ HTTP Error {e.code}: {e.reason}")
            except Exception as e:
                print(f"\n    ❌ Error: {str(e)[:100]}")
        
        return None
    
    def _get_alternative_filename(self, year, day_of_year, halfhour_index):
        """Try alternative filename patterns"""
        date_obj = dt(year, 1, 1) + timedelta(days=day_of_year - 1)
        month = date_obj.month
        day = date_obj.day
        
        month_str = f"{month:02d}"
        day_str = f"{day:02d}"
        hour = halfhour_index // 2
        minute_start = "00" if (halfhour_index % 2) == 0 else "30"
        
        # Calculate end time
        end_hour = hour
        if minute_start == "00":
            minute_end = "29"
        else:
            minute_end = "59"
        
        # Pattern 1: Without leading zeros in minutes code
        minutes_code = f"{halfhour_index * 30:04d}"
        alt_minutes_code = minutes_code.lstrip('0')
        if alt_minutes_code == '':
            alt_minutes_code = '0'
        
        filename = (
            f"3B-HHR-E.MS.MRG.3IMERG."
            f"{year}{month_str}{day_str}-"
            f"S{hour:02d}{minute_start}00-"
            f"E{end_hour:02d}{minute_end}59."
            f"{alt_minutes_code}.V{self.VERSION}.HDF5"
        )
        return filename
    
    def test_download_single_file(self, year=1999, day_of_year=1, halfhour_index=0):
        """Test downloading a single file to debug"""
        print(f"\n🧪 TESTING SINGLE FILE DOWNLOAD")
        print("="*70)
        
        self._setup_authentication()
        
        if not self._test_authentication():
            print("❌ Authentication failed")
            return
        
        # Get filename
        filename = self.get_halfhourly_filename(year, day_of_year, halfhour_index)
        url = f"{self.BASE_URL}/{year}/{day_of_year:03d}/{filename}"
        
        print(f"Testing download for: {filename}")
        print(f"URL: {url}")
        print("-" * 80)
        
        result = self.download_halfhourly_file(year, day_of_year, halfhour_index)
        
        if result:
            print(f"\n✅ SUCCESS: Downloaded to {result}")
            size_mb = result.stat().st_size / (1024 * 1024)
            print(f"File size: {size_mb:.1f} MB")
            
            # Try to open the file
            try:
                with h5py.File(result, 'r') as h5_file:
                    print("\n📊 HDF5 File Structure:")
                    # List all groups
                    def print_structure(name, obj):
                        if isinstance(obj, h5py.Group):
                            print(f"  Group: {name}")
                        elif isinstance(obj, h5py.Dataset):
                            print(f"  Dataset: {name} - Shape: {obj.shape}, Dtype: {obj.dtype}")
                    
                    h5_file.visititems(print_structure)
                    
                    # Try to find precipitation data
                    if 'Grid' in h5_file:
                        print(f"\n  Found 'Grid' group")
                        grid_group = h5_file['Grid']
                        for key in grid_group.keys():
                            print(f"    {key}")
                            if key in ['precipitation', 'precipitationCal']:
                                dataset = grid_group[key]
                                print(f"      Shape: {dataset.shape}")
                                print(f"      Dtype: {dataset.dtype}")
                                if '_FillValue' in dataset.attrs:
                                    print(f"      Fill value: {dataset.attrs['_FillValue']}")
                                if 'units' in dataset.attrs:
                                    print(f"      Units: {dataset.attrs['units']}")
                    else:
                        # Try alternative paths
                        for key in h5_file.keys():
                            print(f"  Top-level key: {key}")
                            if isinstance(h5_file[key], h5py.Group):
                                for subkey in h5_file[key].keys():
                                    print(f"    {subkey}")
                    
            except Exception as e:
                print(f"  ❌ Error reading HDF5: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"\n❌ FAILED: Could not download file")
            
            # Try to check if the URL exists
            try:
                head_req = urllib.request.Request(url, method='HEAD')
                head_response = urllib.request.urlopen(head_req, timeout=10)
                print(f"  URL exists (HTTP {head_response.getcode()})")
            except urllib.error.HTTPError as e:
                print(f"  HTTP Error {e.code}: {e.reason}")
                # List directory to see what's available
                dir_url = f"{self.BASE_URL}/{year}/{day_of_year:03d}/"
                try:
                    print(f"\n  Checking directory listing: {dir_url}")
                    dir_req = urllib.request.Request(dir_url)
                    dir_response = urllib.request.urlopen(dir_req, timeout=10)
                    html_content = dir_response.read().decode('utf-8')
                    
                    # Extract links from HTML
                    import re
                    links = re.findall(r'href="([^"]+\.HDF5[^"]*)"', html_content)
                    if links:
                        print(f"  Found {len(links)} HDF5 files in directory:")
                        for link in links[:10]:  # Show first 10
                            print(f"    {link}")
                        if len(links) > 10:
                            print(f"    ... and {len(links) - 10} more")
                    else:
                        print("  No HDF5 files found in directory")
                        
                except Exception as e2:
                    print(f"  Could not list directory: {e2}")
            except Exception as e:
                print(f"  Error: {e}")
    
    def download_day_halfhourly_files(self, year, day_of_year, max_workers=5):
        """Download all 48 half-hourly files for a single day"""
        
        # Calculate date for logging
        date_obj = dt(year, 1, 1) + timedelta(days=day_of_year - 1)
        month = date_obj.month
        day = date_obj.day
        
        print(f"  Day {day:02d} ({day_of_year:03d}): ", end='', flush=True)
        
        downloaded_files = []
        
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit download tasks for each half-hour (0-47)
            future_to_index = {
                executor.submit(self.download_halfhourly_file, year, day_of_year, i): i 
                for i in range(48)  # 48 half-hour periods
            }
            
            # Process completed downloads
            completed = 0
            for future in as_completed(future_to_index):
                try:
                    result = future.result(timeout=300)
                    if result:
                        downloaded_files.append(result)
                    completed += 1
                    
                    # Show progress
                    if completed % 10 == 0:
                        print(f"{completed}/48", end=' ', flush=True)
                        
                except Exception as e:
                    completed += 1
                    pass
        
        if downloaded_files:
            print(f"→ Downloaded: {len(downloaded_files)}/48 files")
        else:
            print(f"→ No files downloaded")
        
        return downloaded_files
    
    def download_month_halfhourly_files(self, year, month, max_workers=5):
        """Download all half-hourly files for a month"""
        
        print(f"\n📥 Downloading half-hourly files for {year}-{month:02d}")
        print("-" * 50)
        
        # Get days in month
        days_in_month = calendar.monthrange(year, month)[1]
        
        all_downloaded = []
        
        for day in range(1, days_in_month + 1):
            # Calculate day of year
            day_of_year = dt(year, month, day).timetuple().tm_yday
            
            day_files = self.download_day_halfhourly_files(year, day_of_year, max_workers)
            all_downloaded.extend(day_files)
        
        print(f"  Month total: {len(all_downloaded)}/{days_in_month * 48} files")
        return all_downloaded
    
    def check_year_availability(self, year):
        """Check what months/days are available for a given year"""
        print(f"\n🔍 Checking availability for {year}")
        
        available_days = []
        
        # Check first few days of the year
        test_days = [1, 15, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 365]
        
        for day_of_year in test_days:
            url = f"{self.BASE_URL}/{year}/{day_of_year:03d}/"
            
            try:
                request = urllib.request.Request(url, method='HEAD')
                response = urllib.request.urlopen(request, timeout=5)
                if response.getcode() == 200:
                    available_days.append(day_of_year)
                    date_obj = dt(year, 1, 1) + timedelta(days=day_of_year - 1)
                    print(f"  ✓ {date_obj.strftime('%Y-%m-%d')} (day {day_of_year:03d}): Available")
            except:
                pass
        
        if available_days:
            return True
        else:
            print(f"  No data found for {year}")
            return False
    
    # =========================================================================
    # PROCESSING METHODS: HALF-HOURLY TO MONTHLY - UPDATED WITH FIXES
    # =========================================================================
    
    def process_halfhourly_file_fixed(self, hdf5_path):
        """Process a single half-hourly HDF5 file with proper fill value handling"""
        try:
            # Check file
            if not hdf5_path.exists():
                return None
            
            # Check file size
            file_size = hdf5_path.stat().st_size / (1024 * 1024)
            if file_size < 0.5:  # Skip files that are too small
                return None
            
            # Open HDF5 file
            with h5py.File(hdf5_path, 'r') as h5_file:
                # Get precipitation data
                if 'Grid' in h5_file:
                    # Try different possible dataset names
                    # precipitationCal is the CALIBRATED version (preferred)
                    # precipitation is the UNCALIBRATED version
                    # precipitationUncal is another uncalibrated version
                    precip_datasets = ['precipitationCal', 'precipitation', 'precipitationUncal']
                    precip_data = None
                    dataset_name = None
                    
                    for dataset in precip_datasets:
                        if dataset in h5_file['Grid']:
                            try:
                                precip_data = h5_file['Grid'][dataset][:]
                                dataset_name = dataset
                                print(f"    Using dataset: {dataset_name}")
                                break
                            except:
                                continue
                    
                    if precip_data is None:
                        return None
                    
                    try:
                        lat = h5_file['Grid/lat'][:]
                        lon = h5_file['Grid/lon'][:]
                    except:
                        # Alternative location
                        lat = h5_file['Grid']['lat'][:]
                        lon = h5_file['Grid']['lon'][:]
                    
                    # Get fill value and scaling attributes - CRITICAL!
                    dataset_obj = h5_file['Grid'][dataset_name]
                    
                    # Check for fill value
                    fill_value = dataset_obj.attrs.get('_FillValue', -9999.9)
                    
                    # Check for scale factor and offset (common in HDF5)
                    scale_factor = dataset_obj.attrs.get('scale_factor', 1.0)
                    offset = dataset_obj.attrs.get('offset', 0.0)
                    
                    # Check for valid range
                    valid_min = dataset_obj.attrs.get('valid_min', -9999.9)
                    valid_max = dataset_obj.attrs.get('valid_max', 9999.9)
                    
                    print(f"    Fill value: {fill_value}, Scale: {scale_factor}, Offset: {offset}")
                    
                    # Data shape is typically (1, 1800, 3600) - remove first dimension
                    if len(precip_data.shape) == 3 and precip_data.shape[0] == 1:
                        precip_2d = precip_data[0, :, :]
                    else:
                        precip_2d = precip_data
                    
                    # Apply scale factor and offset if they exist
                    if scale_factor != 1.0 or offset != 0.0:
                        print(f"    Applying scale factor {scale_factor} and offset {offset}")
                        precip_2d = precip_2d * scale_factor + offset
                    
                    # Convert from mm/hr to mm per 30-minute period
                    precip_mm_30min = precip_2d * 0.5
                    
                    # Handle fill values - IMPORTANT FIX!
                    # Method 1: Replace fill values with NaN
                    precip_clean = np.where(precip_mm_30min == fill_value, np.nan, precip_mm_30min)
                    
                    # Method 2: Also check for extreme values that might be fill values
                    # Some datasets use very large negative numbers as fill
                    precip_clean = np.where(precip_clean < -1000, np.nan, precip_clean)
                    precip_clean = np.where(precip_clean > 10000, np.nan, precip_clean)  # 10,000 mm/month is extreme
                    
                    # Check data orientation and transpose if needed
                    # GPM data is typically lat x lon (1800 x 3600)
                    if precip_clean.shape[0] == len(lon) and precip_clean.shape[1] == len(lat):
                        precip_clean = precip_clean.T
                    
                    # Quick sanity check
                    valid_data = precip_clean[~np.isnan(precip_clean)]
                    if len(valid_data) > 0:
                        print(f"    Valid data range: {np.nanmin(precip_clean):.2f} to {np.nanmax(precip_clean):.2f} mm/30min")
                    
                    return {
                        'data': precip_clean,
                        'lat': lat,
                        'lon': lon,
                        'filename': hdf5_path.name,
                        'dataset': dataset_name,
                        'fill_value': fill_value,
                        'scale_factor': scale_factor,
                        'offset': offset
                    }
                else:
                    print(f"  No 'Grid' group found in {hdf5_path.name}")
                    return None
                    
        except Exception as e:
            print(f"  Error processing {hdf5_path.name}: {str(e)[:100]}")
            import traceback
            traceback.print_exc()
            return None
    
    def process_halfhourly_file(self, hdf5_path):
        """Process a single half-hourly HDF5 file and extract precipitation"""
        try:
            # Check file
            if not hdf5_path.exists():
                return None
            
            # Check file size
            file_size = hdf5_path.stat().st_size / (1024 * 1024)
            if file_size < 0.5:  # Skip files that are too small
                return None
            
            # Open HDF5 file
            with h5py.File(hdf5_path, 'r') as h5_file:
                # Get precipitation data
                if 'Grid' in h5_file:
                    # Try different possible dataset names
                    precip_datasets = ['precipitationCal', 'precipitation', 'precipitationUncal']
                    precip_data = None
                    dataset_name = None
                    
                    for dataset in precip_datasets:
                        if dataset in h5_file['Grid']:
                            try:
                                precip_data = h5_file['Grid'][dataset][:]
                                dataset_name = dataset
                                break
                            except:
                                continue
                    
                    if precip_data is None:
                        return None
                    
                    try:
                        lat = h5_file['Grid/lat'][:]
                        lon = h5_file['Grid/lon'][:]
                    except:
                        # Alternative location
                        lat = h5_file['Grid']['lat'][:]
                        lon = h5_file['Grid']['lon'][:]
                    
                    # Get fill value and scaling attributes
                    dataset_obj = h5_file['Grid'][dataset_name]
                    fill_value = dataset_obj.attrs.get('_FillValue', -9999.9)
                    scale_factor = dataset_obj.attrs.get('scale_factor', 1.0)
                    offset = dataset_obj.attrs.get('offset', 0.0)
                    
                    # Data shape is typically (1, 1800, 3600) - remove first dimension
                    if len(precip_data.shape) == 3 and precip_data.shape[0] == 1:
                        precip_2d = precip_data[0, :, :]
                    else:
                        precip_2d = precip_data
                    
                    # Apply scale factor and offset if they exist
                    if scale_factor != 1.0 or offset != 0.0:
                        precip_2d = precip_2d * scale_factor + offset
                    
                    # Convert from mm/hr to mm per 30-minute period
                    precip_mm_30min = precip_2d * 0.5
                    
                    # Handle fill values - use the fixed version
                    precip_clean = np.where(precip_mm_30min == fill_value, np.nan, precip_mm_30min)
                    
                    # Also check for extreme values that might be fill values
                    precip_clean = np.where(precip_clean < -1000, np.nan, precip_clean)
                    precip_clean = np.where(precip_clean > 10000, np.nan, precip_clean)
                    
                    # Check data orientation and transpose if needed
                    if precip_clean.shape[0] == len(lon) and precip_clean.shape[1] == len(lat):
                        precip_clean = precip_clean.T
                    
                    return {
                        'data': precip_clean,
                        'lat': lat,
                        'lon': lon,
                        'filename': hdf5_path.name,
                        'dataset': dataset_name
                    }
                else:
                    return None
                    
        except Exception as e:
            print(f"  Error processing {hdf5_path.name}: {str(e)[:100]}")
            return None
    
    def process_day_from_halfhourly(self, year, month, day, halfhourly_files=None):
        """Process all half-hourly files for a day and create daily total"""
        
        # If halfhourly_files not provided, find them
        if halfhourly_files is None:
            day_of_year = dt(year, month, day).timetuple().tm_yday
            day_dir = self.dirs['raw_halfhourly'] / str(year) / f"{day_of_year:03d}"
            if not day_dir.exists():
                return None
            
            halfhourly_files = list(day_dir.glob("*.HDF5"))
            if not halfhourly_files:
                return None
        
        # Process each file
        halfhour_results = []
        for hdf5_file in sorted(halfhourly_files):
            result = self.process_halfhourly_file(hdf5_file)
            if result:
                halfhour_results.append(result)
        
        if len(halfhour_results) == 0:
            return None
        
        # Get reference coordinates from first valid file
        lat = halfhour_results[0]['lat']
        lon = halfhour_results[0]['lon']
        
        # Create daily aggregate (sum of half-hourly values)
        daily_total = np.zeros((len(lat), len(lon)))
        valid_count = np.zeros((len(lat), len(lon)))
        
        for result in halfhour_results:
            data = result['data']
            mask = ~np.isnan(data)
            daily_total[mask] += data[mask]
            valid_count[mask] += 1
        
        # Set pixels with no valid data to NaN
        daily_total[valid_count == 0] = np.nan
        
        # Statistics for logging
        valid_pixels = np.sum(valid_count > 0)
        total_pixels = daily_total.size
        
        print(f"    Day {day:02d}: {len(halfhour_results)} files, {valid_pixels/total_pixels*100:.1f}% coverage")
        
        return {
            'data': daily_total,
            'lat': lat,
            'lon': lon,
            'files_used': len(halfhour_results)
        }
    
    def process_month_from_halfhourly(self, year, month, halfhourly_files=None):
        """Process all half-hourly files for a month and create monthly aggregate"""
        
        print(f"\n🔄 Processing month {year}-{month:02d} from half-hourly files")
        print("-" * 50)
        
        # If files not provided, find them for each day
        if halfhourly_files is None:
            days_in_month = calendar.monthrange(year, month)[1]
            all_daily_results = []
            
            print(f"  Processing {days_in_month} days:")
            
            for day in range(1, days_in_month + 1):
                daily_result = self.process_day_from_halfhourly(year, month, day)
                if daily_result:
                    all_daily_results.append(daily_result)
                    print(f"    Day {day:02d}: Processed {daily_result['files_used']} files")
            
            if len(all_daily_results) == 0:
                print(f"  ❌ No daily data processed")
                return None
            
            print(f"\n  Processed {len(all_daily_results)}/{days_in_month} days")
            
            # Get reference coordinates from first day
            lat = all_daily_results[0]['lat']
            lon = all_daily_results[0]['lon']
            
            # Sum all daily data
            monthly_total = np.zeros((len(lat), len(lon)))
            valid_days_count = np.zeros((len(lat), len(lon)))
            
            for idx, daily in enumerate(all_daily_results):
                data = daily['data']
                mask = ~np.isnan(data)
                monthly_total[mask] += data[mask]
                valid_days_count[mask] += 1
                
                # Clear memory
                if idx % 5 == 0:
                    del data
            
            # Set pixels with no valid days to NaN
            monthly_total[valid_days_count == 0] = np.nan
            
            source_count = len(all_daily_results)
            
        else:
            # Process files in batches to avoid memory issues
            print(f"  Processing {len(halfhourly_files)} half-hourly files")
            
            # Get first valid file to get dimensions
            lat, lon = None, None
            for hdf5_file in halfhourly_files[:10]:  # Check first 10 files
                result = self.process_halfhourly_file(hdf5_file)
                if result:
                    lat = result['lat']
                    lon = result['lon']
                    break
            
            if lat is None or lon is None:
                print(f"  ❌ Could not determine grid dimensions")
                return None
            
            # Initialize accumulators
            monthly_total = np.zeros((len(lat), len(lon)))
            valid_periods_count = np.zeros((len(lat), len(lon)))
            
            # Process files in smaller batches
            batch_size = 100
            processed_count = 0
            
            for i in range(0, len(halfhourly_files), batch_size):
                batch = halfhourly_files[i:i + batch_size]
                print(f"    Processing batch {i//batch_size + 1}/{(len(halfhourly_files)-1)//batch_size + 1}")
                
                for hdf5_file in batch:
                    result = self.process_halfhourly_file(hdf5_file)
                    if result:
                        data = result['data']
                        mask = ~np.isnan(data)
                        monthly_total[mask] += data[mask]
                        valid_periods_count[mask] += 1
                        processed_count += 1
                
                # Clear memory
                import gc
                gc.collect()
            
            # Set pixels with no valid data to NaN
            monthly_total[valid_periods_count == 0] = np.nan
            
            print(f"  Successfully processed {processed_count}/{len(halfhourly_files)} files")
            source_count = processed_count
        
        # Check if we have any valid data
        if np.all(np.isnan(monthly_total)):
            print(f"  ❌ No valid data for {year}-{month:02d}")
            return None
        
        # Statistics
        valid_mask = ~np.isnan(monthly_total)
        valid_count = np.sum(valid_mask)
        total_pixels = monthly_total.size
        
        print(f"\n  📊 MONTHLY STATISTICS:")
        print(f"    Min: {np.nanmin(monthly_total):.1f} mm")
        print(f"    Max: {np.nanmax(monthly_total):.1f} mm")
        print(f"    Mean: {np.nanmean(monthly_total):.1f} mm")
        print(f"    Valid pixels: {valid_count:,} ({valid_count/total_pixels*100:.1f}%)")
        
        # Prepare data for export
        data_for_export = monthly_total
        
        # Flip latitude if needed (GPM typically has North to South)
        if len(lat) > 1 and lat[0] > lat[-1]:
            data_for_export = np.flipud(data_for_export)
            lat = np.flip(lat)
        
        # Create output directories
        year_geotiff_dir = self.dirs['geotiffs'] / str(year)
        year_geotiff_dir.mkdir(parents=True, exist_ok=True)
        
        # Create output filename
        output_filename = f"GPM_{year}_{month:02d}_monthly_from_halfhourly.tif"
        output_path = year_geotiff_dir / output_filename
        
        # Delete existing file if it's empty
        if output_path.exists():
            existing_size = output_path.stat().st_size / (1024 * 1024)
            if existing_size < 1.0:
                output_path.unlink(missing_ok=True)
        
        # Save as GeoTIFF
        if HAS_RASTERIO:
            success = self._save_monthly_geotiff(
                data=data_for_export,
                lat=lat,
                lon=lon,
                output_path=output_path,
                year=year,
                month=month,
                source_count=source_count
            )
            
            if success:
                # Create preview
                if HAS_MATPLOTLIB:
                    preview_dir = self.dirs['previews'] / str(year)
                    preview_dir.mkdir(exist_ok=True)
                    preview_path = preview_dir / f"GPM_{year}_{month:02d}_preview.png"
                    self._create_preview(
                        data=data_for_export,
                        lat=lat,
                        lon=lon,
                        output_path=preview_path,
                        year=year,
                        month=month
                    )
                
                # Create metadata with data interpretation
                metadata_dir = self.dirs['metadata'] / str(year)
                metadata_dir.mkdir(exist_ok=True)
                metadata_path = metadata_dir / f"GPM_{year}_{month:02d}_metadata.txt"
                self._create_monthly_metadata(
                    metadata_path=metadata_path,
                    year=year,
                    month=month,
                    stats={
                        'min': float(np.nanmin(monthly_total)),
                        'max': float(np.nanmax(monthly_total)),
                        'mean': float(np.nanmean(monthly_total)),
                        'valid_pixels': int(valid_count),
                        'total_pixels': int(total_pixels)
                    }
                )
                
                # Verify file was created
                if output_path.exists():
                    file_size = output_path.stat().st_size / (1024 * 1024)
                    print(f"\n  ✅ Monthly GeoTIFF created: {output_path.name} ({file_size:.1f} MB)")
                    return output_path
                else:
                    print(f"\n  ❌ GeoTIFF file was not created")
                    return None
            else:
                print(f"\n  ❌ Failed to save GeoTIFF")
                return None
        
        return None
    
    def _save_monthly_geotiff(self, data, lat, lon, output_path, year, month, source_count):
        """Save monthly aggregated data as GeoTIFF"""
        try:
            # Validate data
            if data.size == 0:
                return False
            
            # Calculate pixel size
            if len(lat) > 1:
                pixel_height = abs(lat[1] - lat[0])
            else:
                pixel_height = 0.1
            
            if len(lon) > 1:
                pixel_width = abs(lon[1] - lon[0])
            else:
                pixel_width = 0.1
            
            left = lon[0] - pixel_width/2
            right = lon[-1] + pixel_width/2
            bottom = lat[0] - pixel_height/2
            top = lat[-1] + pixel_height/2
            
            # Create transform
            transform = from_origin(left, top, pixel_width, pixel_height)
            
            # Prepare data
            nodata_value = -9999.0
            data_to_write = np.where(np.isnan(data), nodata_value, data).astype(np.float32)
            
            # Write GeoTIFF
            with rasterio.open(
                output_path,
                'w',
                driver='GTiff',
                height=data.shape[0],
                width=data.shape[1],
                count=1,
                dtype='float32',
                crs=CRS.from_epsg(4326),
                transform=transform,
                nodata=nodata_value,
                compress='deflate',
                tiled=True
            ) as dst:
                dst.write(data_to_write, 1)
                
                # Add metadata
                dst.update_tags(
                    TIFFTAG_DATETIME=dt.now().strftime('%Y:%m:%d %H:%M:%S'),
                    TIFFTAG_IMAGEDESCRIPTION=f'GPM Monthly Precipitation {year}-{month:02d}',
                    DATA_UNITS='mm/month',
                    SOURCE='NASA GPM IMERG Half-Hourly Early Run V07B',
                    SOURCE_FILES=str(source_count),
                    RESOLUTION=f'{pixel_width:.3f}° x {pixel_height:.3f}°',
                    PROCESSING_DATE=dt.now().strftime('%Y-%m-%d %H:%M:%S'),
                    PROCESSING_SOFTWARE='GPMHalfHourlyToMonthlyWorkflow',
                    PROCESSING_VERSION='1.0',
                    DATA_RANGE=f'{np.nanmin(data):.2f} to {np.nanmax(data):.2f} mm',
                    VALID_PIXELS=f'{np.sum(~np.isnan(data)):,}',
                    COMPRESSION='DEFLATE',
                    TILED='YES'
                )
            
            return True
            
        except Exception as e:
            print(f"  ❌ Error saving GeoTIFF: {e}")
            return False
    
    def _create_preview(self, data, lat, lon, output_path, year, month):
        """Create a preview image of the monthly data"""
        try:
            import matplotlib.pyplot as plt
            import matplotlib.colors as mcolors
            from matplotlib.cm import ScalarMappable
            
            # Create figure
            fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
            
            # Create colormap for precipitation
            cmap = plt.cm.viridis.copy()
            
            # Mask NaN values
            data_masked = np.ma.masked_invalid(data)
            
            # Create plot
            im = ax.imshow(data_masked, cmap=cmap, 
                          extent=[lon[0], lon[-1], lat[-1], lat[0]],
                          aspect='auto')
            
            # Add colorbar
            cbar = plt.colorbar(im, ax=ax, shrink=0.8, pad=0.02)
            cbar.set_label('Precipitation (mm/month)', fontsize=10)
            
            # Set title and labels
            ax.set_title(f'GPM Monthly Precipitation: {year}-{month:02d}', 
                        fontsize=14, fontweight='bold')
            ax.set_xlabel('Longitude', fontsize=10)
            ax.set_ylabel('Latitude', fontsize=10)
            
            # Add grid
            ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
            
            # Add coastlines if cartopy is available
            try:
                import cartopy.crs as ccrs
                import cartopy.feature as cfeature
                
                ax.coastlines(resolution='50m', color='white', linewidth=0.5)
                ax.add_feature(cfeature.BORDERS, linewidth=0.3, edgecolor='white')
                
            except ImportError:
                pass
            
            # Add text info
            stats_text = f'Min: {np.nanmin(data):.1f} mm\n' \
                        f'Max: {np.nanmax(data):.1f} mm\n' \
                        f'Mean: {np.nanmean(data):.1f} mm\n' \
                        f'Valid: {np.sum(~np.isnan(data)):,} pixels'
            
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                   verticalalignment='top', fontsize=8,
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
            
            # Save figure
            plt.tight_layout()
            plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
            plt.close(fig)
            
            print(f"    Preview saved: {output_path.name}")
            
        except Exception as e:
            print(f"    Warning: Could not create preview: {e}")
    
    def _create_monthly_metadata(self, metadata_path, year, month, stats):
        """Create metadata file for monthly GeoTIFF"""
        try:
            with open(metadata_path, 'w') as f:
                f.write(f"NASA GPM IMERG Monthly Precipitation Dataset\n")
                f.write(f"="*50 + "\n\n")
                f.write(f"Product: GPM_3IMERGHHE.07 (Half-Hourly Early Run)\n")
                f.write(f"Version: {self.VERSION}\n")
                f.write(f"Time Period: {year}-{month:02d}\n")
                f.write(f"Processing Date: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write(f"Data Interpretation:\n")
                f.write(f"  This GeoTIFF contains TOTAL monthly precipitation accumulation.\n")
                f.write(f"  Each pixel value represents the sum of precipitation from all\n")
                f.write(f"  half-hourly periods (48 per day) across the entire month.\n")
                f.write(f"  Values are in mm/month, representing total depth of precipitation.\n")
                f.write(f"  Conversion: hourly rate (mm/hr) * 0.5 hr * number of periods\n\n")
                
                f.write(f"Dataset Statistics:\n")
                f.write(f"  Minimum precipitation: {stats['min']:.2f} mm\n")
                f.write(f"  Maximum precipitation: {stats['max']:.2f} mm\n")
                f.write(f"  Mean precipitation: {stats['mean']:.2f} mm\n")
                f.write(f"  Valid pixels: {stats['valid_pixels']:,}\n")
                f.write(f"  Total pixels: {stats['total_pixels']:,}\n")
                f.write(f"  Coverage: {stats['valid_pixels']/stats['total_pixels']*100:.1f}%\n\n")
                
                f.write(f"Processing Information:\n")
                f.write(f"  Software: GPMHalfHourlyToMonthlyWorkflow v1.0\n")
                f.write(f"  Resolution: 0.1° x 0.1°\n")
                f.write(f"  Units: mm/month\n")
                f.write(f"  CRS: EPSG:4326 (WGS84)\n")
                f.write(f"  NoData value: -9999.0\n\n")
                
                f.write(f"Source Data:\n")
                f.write(f"  NASA GPM IMERG Half-Hourly Early Run\n")
                f.write(f"  Base URL: {self.BASE_URL}\n")
                
            print(f"    Metadata saved: {metadata_path.name}")
            
        except Exception as e:
            print(f"    Warning: Could not save metadata: {e}")
    
    # =========================================================================
    # DIAGNOSTIC AND FIX METHODS
    # =========================================================================
    
    def diagnose_data_issue(self, year=1999, month=1):
        """Diagnose why we're getting negative precipitation values"""
        print(f"\n🔍 DIAGNOSING DATA ISSUE FOR {year}-{month:02d}")
        print("="*70)
        
        # Find files for this month
        halfhourly_files = []
        for file_info in self.get_existing_raw_files():
            file_year = file_info['year']
            file_day = file_info['day_of_year']
            file_date = dt(file_year, 1, 1) + timedelta(days=file_day - 1)
            
            if file_date.year == year and file_date.month == month:
                halfhourly_files.append(file_info['path'])
        
        print(f"Found {len(halfhourly_files)} files for {year}-{month:02d}")
        
        if not halfhourly_files:
            print("No files found")
            return
        
        # Check first 5 files in detail
        for i, hdf5_file in enumerate(halfhourly_files[:5]):
            print(f"\n--- File {i+1}: {hdf5_file.name} ---")
            
            try:
                with h5py.File(hdf5_file, 'r') as h5_file:
                    # List all groups
                    print("HDF5 structure:")
                    h5_file.visititems(lambda name, obj: print(f"  {name}: {type(obj).__name__}"))
                    
                    if 'Grid' in h5_file:
                        print("\nGrid group contents:")
                        for key in h5_file['Grid'].keys():
                            print(f"  {key}:")
                            dataset = h5_file['Grid'][key]
                            if isinstance(dataset, h5py.Dataset):
                                print(f"    Shape: {dataset.shape}")
                                print(f"    Dtype: {dataset.dtype}")
                                
                                # Check attributes
                                print(f"    Attributes:")
                                for attr_name in dataset.attrs:
                                    attr_value = dataset.attrs[attr_name]
                                    print(f"      {attr_name}: {attr_value}")
                                
                                # Sample first few values
                                if len(dataset.shape) >= 2:
                                    sample = dataset[:3, :3]
                                    print(f"    Sample values: {sample.ravel()[:9]}")
                    
            except Exception as e:
                print(f"Error reading file: {e}")
    
    def test_single_file_processing(self, year=1999, day_of_year=1, halfhour_index=0):
        """Test processing a single file to debug the negative values"""
        print(f"\n🧪 TESTING SINGLE FILE PROCESSING")
        print("="*70)
        
        # Get the file
        day_dir = self.dirs['raw_halfhourly'] / str(year) / f"{day_of_year:03d}"
        filename = self.get_halfhourly_filename(year, day_of_year, halfhour_index)
        hdf5_path = day_dir / filename
        
        if not hdf5_path.exists():
            print(f"File not found: {hdf5_path}")
            return
        
        print(f"Processing: {filename}")
        
        # Use the fixed processing function
        result = self.process_halfhourly_file_fixed(hdf5_path)
        
        if result:
            print(f"\n✅ Processing successful")
            print(f"Dataset used: {result['dataset']}")
            print(f"Fill value: {result['fill_value']}")
            print(f"Scale factor: {result['scale_factor']}")
            print(f"Offset: {result['offset']}")
            
            data = result['data']
            valid_data = data[~np.isnan(data)]
            
            if len(valid_data) > 0:
                print(f"\nData statistics (mm/30min):")
                print(f"  Min: {np.nanmin(data):.4f}")
                print(f"  Max: {np.nanmax(data):.4f}")
                print(f"  Mean: {np.nanmean(data):.4f}")
                print(f"  Valid pixels: {len(valid_data):,}")
                print(f"  NaN pixels: {np.sum(np.isnan(data)):,}")
                
                # Check for negative values
                negative_count = np.sum(valid_data < 0)
                if negative_count > 0:
                    print(f"  ⚠️ WARNING: {negative_count} negative values found!")
                    print(f"  Negative range: {np.min(valid_data[valid_data < 0]):.4f} to {np.max(valid_data[valid_data < 0]):.4f}")
            else:
                print("⚠️ No valid data found!")
        else:
            print("❌ Failed to process file")
    
    def test_monthly_concept(self):
        """Test what the monthly GeoTIFF actually represents"""
        print("\n🔍 Testing monthly concept...")
        
        # Simulate: 1 mm/hour rate for all half-hour periods
        half_hour_value = 1.0 * 0.5  # mm/hour * 0.5 hour = 0.5 mm per half-hour
        
        # One day: 48 half-hour periods
        daily_total = half_hour_value * 48  # = 24 mm/day
        
        # One month (30 days)
        monthly_total = daily_total * 30  # = 720 mm/month
        
        print(f"  If constant rain at 1 mm/hour:")
        print(f"  - Half-hour value: {half_hour_value:.2f} mm")
        print(f"  - Daily total: {daily_total:.2f} mm")
        print(f"  - Monthly total: {monthly_total:.2f} mm")
        
        # Check if this matches what you see in your GeoTIFFs
        print(f"\n  Expected values in mm/month if summing rates:")
        print(f"  - Light rain month: 50-100 mm/month")
        print(f"  - Moderate rain month: 100-300 mm/month") 
        print(f"  - Heavy rain month: 300-1000+ mm/month")
    
    def process_existing_month(self, year, month):
        """Process existing downloaded files for a specific month"""
        print(f"\n🔄 Processing existing files for {year}-{month:02d}")
        print("-" * 50)
        
        # Find files for this month
        halfhourly_files = []
        for file_info in self.get_existing_raw_files():
            file_year = file_info['year']
            file_day = file_info['day_of_year']
            file_date = dt(file_year, 1, 1) + timedelta(days=file_day - 1)
            
            if file_date.year == year and file_date.month == month:
                halfhourly_files.append(file_info['path'])
        
        print(f"  Found {len(halfhourly_files)} files for {year}-{month:02d}")
        
        if not halfhourly_files:
            print(f"  ❌ No files found for {year}-{month:02d}")
            return None
        
        # Process month using the fixed processing
        print("\n  Using fixed processing method...")
        result = self.process_month_from_halfhourly(year, month, halfhourly_files)
        
        if result:
            print(f"\n✅ SUCCESS: Processed {year}-{month:02d}")
            return result
        else:
            print(f"\n❌ FAILED: Could not process {year}-{month:02d}")
            return None
    
    def get_existing_months(self):
        """Get list of months that already have processed GeoTIFFs"""
        processed_months = []
        
        for year_dir in self.dirs['geotiffs'].iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                year = int(year_dir.name)
                for geotiff_file in year_dir.glob("*.tif"):
                    match = re.search(r'GPM_(\d{4})_(\d{2})_monthly', geotiff_file.name)
                    if match:
                        y = int(match.group(1))
                        m = int(match.group(2))
                        processed_months.append((y, m))
        
        return sorted(set(processed_months))
    
    def get_existing_raw_files(self):
        """Get list of all existing raw half-hourly files"""
        raw_files = []
        
        for year_dir in self.dirs['raw_halfhourly'].iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                year = int(year_dir.name)
                for day_dir in year_dir.iterdir():
                    if day_dir.is_dir() and day_dir.name.isdigit():
                        day_of_year = int(day_dir.name)
                        for hdf5_file in day_dir.glob("*.HDF5"):
                            raw_files.append({
                                'year': year,
                                'day_of_year': day_of_year,
                                'path': hdf5_file,
                                'size_mb': hdf5_file.stat().st_size / (1024 * 1024)
                            })
        
        return raw_files
    
    def create_processing_log(self, year, month, success=True, message=""):
        """Create a log entry for processing"""
        log_dir = self.dirs['logs']
        log_file = log_dir / f"processing_{year}_{month:02d}.txt"
        
        try:
            with open(log_file, 'w') as f:
                f.write(f"Processing Log: {year}-{month:02d}\n")
                f.write(f"Timestamp: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Status: {'SUCCESS' if success else 'FAILED'}\n")
                if message:
                    f.write(f"Message: {message}\n")
                
                # Add system information
                f.write(f"\nSystem Information:\n")
                f.write(f"  Python: {sys.version}\n")
                f.write(f"  Numpy: {np.__version__}\n")
                f.write(f"  H5py: {h5py.__version__}\n")
                
                if HAS_RASTERIO:
                    f.write(f"  Rasterio: {rasterio.__version__}\n")
            
            return True
        except:
            return False
    
    # =========================================================================
    # MAIN WORKFLOW METHODS
    # =========================================================================
    
    def run_full_workflow(self, start_year=1999, end_year=2026, 
                         start_month=1, end_month=12,
                         download_only=False, process_only=False):
        """Run the complete workflow for specified date range"""
        
        print(f"\n🚀 STARTING COMPLETE WORKFLOW")
        print(f"Date Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        print("="*70)
        
        # Setup authentication
        self._setup_authentication()
        if not self._test_authentication():
            print("❌ Cannot proceed without authentication")
            return
        
        # Get existing processed months
        existing_months = self.get_existing_months()
        print(f"Found {len(existing_months)} previously processed months")
        
        # Get existing raw files
        existing_raw = self.get_existing_raw_files()
        print(f"Found {len(existing_raw)} existing raw HDF5 files")
        
        # Process each month
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                # Check if we should skip this month
                if year == start_year and month < start_month:
                    continue
                if year == end_year and month > end_month:
                    continue
                
                # Check if already processed
                if (year, month) in existing_months and not download_only:
                    print(f"\n⏭️ Skipping {year}-{month:02d} (already processed)")
                    continue
                
                # Download data
                if not process_only:
                    print(f"\n{'='*60}")
                    print(f"📥 DOWNLOADING: {year}-{month:02d}")
                    print(f"{'='*60}")
                    
                    downloaded_files = self.download_month_halfhourly_files(year, month, max_workers=5)
                    
                    if not downloaded_files:
                        print(f"⚠️ No files downloaded for {year}-{month:02d}")
                        continue
                
                # Process data
                if not download_only:
                    print(f"\n{'='*60}")
                    print(f"🔄 PROCESSING: {year}-{month:02d}")
                    print(f"{'='*60}")
                    
                    # Find files for this month
                    halfhourly_files = []
                    for file_info in existing_raw:
                        file_year = file_info['year']
                        file_day = file_info['day_of_year']
                        file_date = dt(file_year, 1, 1) + timedelta(days=file_day - 1)
                        
                        if file_date.year == year and file_date.month == month:
                            halfhourly_files.append(file_info['path'])
                    
                    # Also add newly downloaded files
                    if 'downloaded_files' in locals() and downloaded_files:
                        halfhourly_files.extend(downloaded_files)
                    
                    # Remove duplicates
                    halfhourly_files = list(set(halfhourly_files))
                    
                    # Process month
                    result = self.process_month_from_halfhourly(year, month, halfhourly_files)
                    
                    if result:
                        print(f"\n✅ SUCCESS: Processed {year}-{month:02d}")
                        self.create_processing_log(year, month, success=True, 
                                                  message=f"Processed successfully")
                    else:
                        print(f"\n❌ FAILED: Could not process {year}-{month:02d}")
                        self.create_processing_log(year, month, success=False, 
                                                  message="Processing failed")
        
        print(f"\n{'='*70}")
        print(f"🎉 WORKFLOW COMPLETE!")
        print(f"{'='*70}")
        
        # Summary
        processed_geotiffs = list(self.dirs['geotiffs'].rglob("*.tif"))
        print(f"📊 Summary:")
        print(f"  Total GeoTIFFs created: {len(processed_geotiffs)}")
        
        total_size_mb = sum(f.stat().st_size / (1024 * 1024) for f in processed_geotiffs)
        print(f"  Total storage used: {total_size_mb:.1f} MB")
    
    def run_test_workflow(self, test_year=1999, test_month=1):
        """Run a test workflow for a single month"""
        print(f"\n🧪 TEST WORKFLOW FOR {test_year}-{test_month:02d}")
        print("="*70)
        
        self._setup_authentication()
        if not self._test_authentication():
            return
        
        # Test filename generation
        print("\n🔧 Testing filename generation...")
        self.debug_filename_generation(test_year, 1)
        
        # Test single file download
        print("\n🔧 Testing single file download...")
        self.test_download_single_file(test_year, 1, 0)
        
        # Download and process test month
        print(f"\n📥 Downloading test month...")
        downloaded_files = self.download_month_halfhourly_files(test_year, test_month, max_workers=3)
        
        if downloaded_files:
            print(f"\n🔄 Processing test month...")
            result = self.process_month_from_halfhourly(test_year, test_month, downloaded_files)
            
            if result:
                print(f"\n✅ TEST SUCCESSFUL!")
                print(f"Created: {result}")
            else:
                print(f"\n❌ TEST FAILED: Could not process data")
        else:
            print(f"\n❌ TEST FAILED: Could not download files")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    print("\n" + "="*70)
    print("NASA GPM HALF-HOURLY TO MONTHLY PROCESSING WORKFLOW")
    print("="*70)
    
    # Create workflow instance
    workflow = GPMHalfHourlyToMonthlyWorkflow()
    
    # Parse command line arguments if provided
    import argparse
    
    parser = argparse.ArgumentParser(description='GPM Half-Hourly to Monthly Processing')
    parser.add_argument('--test', action='store_true', help='Run test workflow')
    parser.add_argument('--debug', action='store_true', help='Debug mode')
    parser.add_argument('--download-only', action='store_true', help='Download only')
    parser.add_argument('--process-only', action='store_true', help='Process existing files only')
    parser.add_argument('--start-year', type=int, default=1999, help='Start year')
    parser.add_argument('--end-year', type=int, default=2026, help='End year')
    parser.add_argument('--start-month', type=int, default=1, help='Start month')
    parser.add_argument('--end-month', type=int, default=12, help='End month')
    parser.add_argument('--diagnose', action='store_true', help='Diagnose data issues')
    parser.add_argument('--fix-process', action='store_true', help='Use fixed processing')
    parser.add_argument('--process-month', type=int, nargs=2, metavar=('YEAR', 'MONTH'), 
                       help='Process specific month (e.g., --process-month 1999 1)')
    
    # For simplicity, use sys.argv
    if len(sys.argv) > 1:
        args = parser.parse_args()
        
        if args.test:
            workflow.run_test_workflow()
        elif args.debug:
            workflow.debug_filename_generation()
        elif args.diagnose:
            if args.process_month:
                workflow.diagnose_data_issue(args.process_month[0], args.process_month[1])
            else:
                workflow.diagnose_data_issue()
        elif args.fix_process:
            if args.process_month:
                workflow.process_existing_month(args.process_month[0], args.process_month[1])
            else:
                workflow.test_single_file_processing()
        elif args.process_month:
            workflow.process_existing_month(args.process_month[0], args.process_month[1])
        else:
            workflow.run_full_workflow(
                start_year=args.start_year,
                end_year=args.end_year,
                start_month=args.start_month,
                end_month=args.end_month,
                download_only=args.download_only,
                process_only=args.process_only
            )
    else:
        # Interactive mode
        print("\nSelect operation mode:")
        print("  1. Test workflow (single month)")
        print("  2. Debug filename generation")
        print("  3. Run complete workflow (1999-2026)")
        print("  4. Custom workflow")
        print("  5. Check existing data")
        print("  6. Diagnose data issues")
        print("  7. Test single file processing")
        print("  8. Process existing month")
        
        choice = input("\nEnter choice (1-8): ").strip()
        
        if choice == '1':
            workflow.run_test_workflow()
        elif choice == '2':
            workflow.debug_filename_generation()
        elif choice == '3':
            workflow.run_full_workflow()
        elif choice == '4':
            print("\nEnter custom parameters:")
            start_year = int(input("Start year (1999-2026): "))
            end_year = int(input("End year (1999-2026): "))
            start_month = int(input("Start month (1-12): "))
            end_month = int(input("End month (1-12): "))
            
            print("\nSelect mode:")
            print("  1. Download and process")
            print("  2. Download only")
            print("  3. Process only")
            mode_choice = input("Enter choice (1-3): ").strip()
            
            download_only = (mode_choice == '2')
            process_only = (mode_choice == '3')
            
            workflow.run_full_workflow(
                start_year=start_year,
                end_year=end_year,
                start_month=start_month,
                end_month=end_month,
                download_only=download_only,
                process_only=process_only
            )
        elif choice == '5':
            existing_months = workflow.get_existing_months()
            existing_raw = workflow.get_existing_raw_files()
            
            print(f"\n📊 EXISTING DATA SUMMARY:")
            print(f"  Processed months: {len(existing_months)}")
            for y, m in sorted(existing_months)[:10]:  # Show first 10
                print(f"    {y}-{m:02d}")
            if len(existing_months) > 10:
                print(f"    ... and {len(existing_months) - 10} more")
            
            print(f"\n  Raw HDF5 files: {len(existing_raw)}")
            if existing_raw:
                total_size = sum(f['size_mb'] for f in existing_raw)
                print(f"  Total raw data size: {total_size:.1f} MB")
            
            # Count by year
            years = {}
            for f in existing_raw:
                y = f['year']
                years[y] = years.get(y, 0) + 1
            
            print(f"\n  Files by year:")
            for y in sorted(years.keys()):
                print(f"    {y}: {years[y]} files")
        elif choice == '6':
            year = int(input("Enter year to diagnose (e.g., 1999): "))
            month = int(input("Enter month to diagnose (1-12): "))
            workflow.diagnose_data_issue(year, month)
        elif choice == '7':
            year = int(input("Enter year (e.g., 1999): "))
            day = int(input("Enter day of year (1-365): "))
            halfhour = int(input("Enter half-hour index (0-47): "))
            workflow.test_single_file_processing(year, day, halfhour)
        elif choice == '8':
            year = int(input("Enter year to process (e.g., 1999): "))
            month = int(input("Enter month to process (1-12): "))
            workflow.process_existing_month(year, month)
        else:
            print("Invalid choice")

if __name__ == "__main__":
    main()