"""
NASA GPM COMPLETE WORKFLOW: DOWNLOAD + PROCESSING PIPELINE
Downloads GPM data (1998-2027) and processes to monthly GeoTIFFs
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
from datetime import datetime as dt
import traceback
import warnings
warnings.filterwarnings('ignore')

# Try to import processing dependencies
try:
    import rasterio
    from rasterio.transform import from_origin
    HAS_RASTERIO = True
except ImportError:
    print("⚠️ rasterio not installed. Install with: pip install rasterio")
    HAS_RASTERIO = False

try:
    import matplotlib.pyplot as plt
    import matplotlib.colors as colors
    from matplotlib import cm
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

class GPMCompleteWorkflow:
    """Complete GPM workflow: download, process, organize"""
    
    def __init__(self, base_dir=None):
        # Configuration
        self.BASE_URL = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGM.07"
        self.VERSION = "07B"
        
        # Base directory structure
        if base_dir is None:
            self.base_dir = Path.home() / "Documents" / "Benjamin" / "GPM" / "GPM_gee" / "GPM_NASA"
        else:
            self.base_dir = Path(base_dir)
        
        # Create directory structure
        self.dirs = {
            'raw': self.base_dir / "netcdfs",
            'processed': self.base_dir / "processed",
            'geotiffs': self.base_dir / "geotiffs",
            'previews': self.base_dir / "previews",
            'metadata': self.base_dir / "metadata",
            'logs': self.base_dir / "logs"
        }
        
        for name, path in self.dirs.items():
            path.mkdir(parents=True, exist_ok=True)
            print(f"  Created: {name}/")
        
        # Authentication
        self.username = None
        self.password = None
        self.opener = None
        
        print(f"\n🌧️ NASA GPM COMPLETE WORKFLOW")
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
                        import re
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
            ('User-Agent', 'Mozilla/5.0 (NASA-GPM-Workflow/1.0)'),
            ('Accept', '*/*'),
        ]
        
        # Install as default
        urllib.request.install_opener(self.opener)
        
        print("✅ Authentication setup complete")
    
    def _test_authentication(self):
        """Test authentication"""
        print("\n🔍 Testing authentication...")
        
        test_url = f"{self.BASE_URL}/2009/"
        
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
    # DOWNLOAD METHODS
    # =========================================================================
    
    def build_filename(self, year, month):
        """Build GPM filename"""
        # Multiple possible patterns
        patterns = [
            f"3B-MO.MS.MRG.3IMERG.{year}{month:02d}01-S000000-E235959.{month:02d}.V{self.VERSION}.HDF5",
            f"3B-MO.MS.MRG.3IMERG.{year}{month:02d}01-S000000-E235959.01.V{self.VERSION}.HDF5",
            f"3B-MO.MS.MRG.3IMERG.{year}{month:02d}01-S000000-E235959.02.V{self.VERSION}.HDF5",
            f"3B-MO.MS.MRG.3IMERG.{year}{month:02d}-S000000-E235959.{month:02d}.V{self.VERSION}.HDF5",
        ]
        return patterns
    
    def get_file_url(self, year, month):
        """Get download URL for a specific month"""
        patterns = self.build_filename(year, month)
        
        for filename in patterns:
            url = f"{self.BASE_URL}/{year}/{filename}"
            try:
                # Check if file exists
                request = urllib.request.Request(url, method='HEAD')
                response = urllib.request.urlopen(request, timeout=5)
                if response.getcode() == 200:
                    print(f"  Found: {filename}")
                    return url, filename
            except:
                continue
        
        return None, None
    
    def download_single_file(self, year, month, max_retries=3):
        """Download a single monthly file"""
        
        # Create year-specific subfolder
        year_dir = self.dirs['raw'] / str(year)
        year_dir.mkdir(exist_ok=True)
        
        # Get URL and filename
        url, filename = self.get_file_url(year, month)
        
        if not url or not filename:
            print(f"  ❌ No file found for {year}-{month:02d}")
            return None
        
        local_path = year_dir / filename
        
        # Check if already exists
        if local_path.exists():
            size_mb = local_path.stat().st_size / (1024 * 1024)
            print(f"  ✓ Already exists: {size_mb:.1f} MB")
            return local_path
        
        # Download with retries
        for attempt in range(max_retries):
            try:
                print(f"  Downloading... (attempt {attempt + 1}/{max_retries})")
                
                if attempt > 0:
                    wait_time = 2 ** attempt
                    print(f"  Waiting {wait_time}s...")
                    time.sleep(wait_time)
                
                # Download with progress
                def progress_callback(count, block_size, total_size):
                    if total_size > 0:
                        percent = min(100, int(count * block_size * 100 / total_size))
                        mb_downloaded = count * block_size / (1024 * 1024)
                        mb_total = total_size / (1024 * 1024)
                        if mb_total > 0:
                            print(f"  Progress: {percent}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end='\r')
                
                temp_path = local_path.with_suffix('.downloading')
                urllib.request.urlretrieve(url, temp_path, reporthook=progress_callback)
                
                # Rename to final
                temp_path.rename(local_path)
                
                if local_path.exists():
                    size_mb = local_path.stat().st_size / (1024 * 1024)
                    print(f"\n  ✅ Downloaded: {size_mb:.1f} MB")
                    return local_path
                    
            except urllib.error.HTTPError as e:
                print(f"\n  ❌ HTTP Error {e.code}")
                if e.code == 401:
                    break
                elif e.code == 404:
                    break
            except Exception as e:
                print(f"\n  ❌ Error: {e}")
        
        print(f"  ❌ Failed after {max_retries} attempts")
        return None
    
    def download_year_range(self, start_year=1998, end_year=2027):
        """Download all files for a range of years"""
        
        print(f"\n📥 DOWNLOADING: {start_year} to {end_year}")
        print("="*70)
        
        # Setup authentication
        self._setup_authentication()
        
        # Test authentication
        if not self._test_authentication():
            print("❌ Authentication failed. Exiting.")
            return []
        
        downloaded_files = []
        failed_months = []
        
        for year in range(start_year, end_year + 1):
            print(f"\n📅 Year {year}:")
            
            # Determine months for this year
            if year == start_year:
                start_month = 1
            else:
                start_month = 1
            
            if year == end_year:
                end_month = min(12, dt.now().month)
            else:
                end_month = 12
            
            for month in range(start_month, end_month + 1):
                print(f"\n  Month {month:02d}:")
                
                result = self.download_single_file(year, month)
                
                if result:
                    downloaded_files.append((year, month, result))
                else:
                    failed_months.append((year, month))
                
                # Small delay between downloads
                time.sleep(0.5)
        
        # Summary
        print(f"\n" + "="*70)
        print("📊 DOWNLOAD SUMMARY")
        print("="*70)
        print(f"Successfully downloaded: {len(downloaded_files)} files")
        print(f"Failed: {len(failed_months)} files")
        
        if failed_months:
            print("\nFailed months:")
            for year, month in failed_months:
                print(f"  {year}-{month:02d}")
        
        # Create download log
        self._create_download_log(downloaded_files, failed_months)
        
        return downloaded_files
    
    def _create_download_log(self, downloaded_files, failed_months):
        """Create a log of downloaded files"""
        log_file = self.dirs['logs'] / f"download_log_{dt.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(log_file, 'w') as f:
            f.write("="*70 + "\n")
            f.write("GPM DOWNLOAD LOG\n")
            f.write("="*70 + "\n\n")
            
            f.write(f"Download date: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total downloaded: {len(downloaded_files)}\n")
            f.write(f"Total failed: {len(failed_months)}\n\n")
            
            f.write("DOWNLOADED FILES:\n")
            f.write("-"*50 + "\n")
            for year, month, filepath in downloaded_files:
                size_mb = filepath.stat().st_size / (1024 * 1024) if filepath.exists() else 0
                f.write(f"{year}-{month:02d}: {filepath.name} ({size_mb:.1f} MB)\n")
            
            if failed_months:
                f.write("\nFAILED DOWNLOADS:\n")
                f.write("-"*50 + "\n")
                for year, month in failed_months:
                    f.write(f"{year}-{month:02d}\n")
        
        print(f"\n📝 Download log saved: {log_file}")
    
    # =========================================================================
    # PROCESSING METHODS
    # =========================================================================
    
    def process_single_file(self, hdf5_path, create_preview=True):
        """Process a single HDF5 file"""
        
        hdf5_path = Path(hdf5_path)
        print(f"\n🔄 Processing: {hdf5_path.name}")
        print("-"*50)
        
        # Extract year and month from filename
        try:
            # Look for YYYYMM pattern in filename
            import re
            match = re.search(r'(\d{4})(\d{2})', hdf5_path.name)
            if match:
                year = int(match.group(1))
                month = int(match.group(2))
            else:
                # Fallback: try to extract from path
                year = int(hdf5_path.parent.name)
                month = 1  # Default
        except:
            print("  ⚠️ Could not extract date from filename")
            return None
        
        # Get hours in month
        days_in_month = calendar.monthrange(year, month)[1]
        hours_in_month = days_in_month * 24
        
        try:
            with h5py.File(hdf5_path, 'r') as h5f:
                # Get Grid group
                if 'Grid' not in h5f:
                    print("  ❌ No Grid group found")
                    return None
                
                grid = h5f['Grid']
                
                # Extract data
                precip = grid['precipitation'][:]  # Shape: (1, 3600, 1800)
                lat = grid['lat'][:]  # Shape: (1800,)
                lon = grid['lon'][:]  # Shape: (3600,)
                
                print(f"  Data shape: {precip.shape}")
                print(f"  Lat: {lat[0]:.1f} to {lat[-1]:.1f}")
                print(f"  Lon: {lon[0]:.1f} to {lon[-1]:.1f}")
                
                # Remove time dimension and handle fill value
                precip_2d = precip[0, :, :]
                fill_value = -9999.9
                precip_masked = np.where(precip_2d == fill_value, np.nan, precip_2d)
                
                # Convert to mm/month
                precip_monthly = precip_masked * hours_in_month
                
                # Prepare for export
                data_for_export = precip_monthly.T  # Transpose to [lat, lon]
                
                # Flip latitude if needed
                if lat[0] > lat[-1]:
                    data_for_export = np.flipud(data_for_export)
                    lat = np.flip(lat)
                
                # Create output directories
                year_processed_dir = self.dirs['geotiffs'] / str(year)
                year_processed_dir.mkdir(exist_ok=True)
                
                # Create output filename
                output_filename = f"GPM_{year}_{month:02d}_monthly.tif"
                output_path = year_processed_dir / output_filename
                
                # Save as GeoTIFF
                if HAS_RASTERIO:
                    success = self._save_as_geotiff(
                        data=data_for_export,
                        lat=lat,
                        lon=lon,
                        output_path=output_path,
                        year=year,
                        month=month,
                        hours_in_month=hours_in_month
                    )
                    
                    if success:
                        # Create preview
                        if create_preview and HAS_MATPLOTLIB:
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
                        
                        # Create metadata
                        metadata_dir = self.dirs['metadata'] / str(year)
                        metadata_dir.mkdir(exist_ok=True)
                        metadata_path = metadata_dir / f"GPM_{year}_{month:02d}_metadata.txt"
                        self._create_metadata(
                            metadata_path=metadata_path,
                            input_file=hdf5_path.name,
                            output_file=output_path.name,
                            year=year,
                            month=month,
                            hours_in_month=hours_in_month,
                            stats={
                                'min': float(np.nanmin(precip_monthly)),
                                'max': float(np.nanmax(precip_monthly)),
                                'mean': float(np.nanmean(precip_monthly)),
                                'valid_pixels': int(np.sum(~np.isnan(precip_monthly)))
                            }
                        )
                        
                        print(f"  ✅ Processed: {output_path.name}")
                        return output_path
                    else:
                        print("  ❌ Failed to save GeoTIFF")
                        return None
                else:
                    print("  ❌ rasterio not available")
                    return None
                    
        except Exception as e:
            print(f"  ❌ Processing error: {e}")
            traceback.print_exc()
            return None
    
    def _save_as_geotiff(self, data, lat, lon, output_path, year, month, hours_in_month):
        """Save data as GeoTIFF"""
        try:
            # Calculate pixel size and transform
            pixel_width = abs(lon[1] - lon[0])
            pixel_height = abs(lat[1] - lat[0])
            
            left = lon[0] - pixel_width/2
            right = lon[-1] + pixel_width/2
            bottom = lat[0] - pixel_height/2
            top = lat[-1] + pixel_height/2
            
            transform = from_origin(left, top, pixel_width, pixel_height)
            
            # Prepare data
            nodata_value = -9999.0
            data_to_write = np.where(np.isnan(data), nodata_value, data)
            
            # Write GeoTIFF
            with rasterio.open(
                output_path,
                'w',
                driver='GTiff',
                height=data.shape[0],
                width=data.shape[1],
                count=1,
                dtype=str(data.dtype),
                crs='EPSG:4326',
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
                    ORIGINAL_UNITS='mm/hr',
                    CONVERSION_FACTOR=str(hours_in_month),
                    SOURCE='NASA GPM IMERG V07B',
                    RESOLUTION='0.1 degree',
                    YEAR=str(year),
                    MONTH=str(month)
                )
                
                dst.set_band_description(1, f'Precipitation {year}-{month:02d}')
            
            return True
            
        except Exception as e:
            print(f"    ❌ GeoTIFF error: {e}")
            return False
    
    def _create_preview(self, data, lat, lon, output_path, year, month):
        """Create preview plot"""
        try:
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Mask zeros for log scale
            plot_data = np.where(data <= 0, np.nan, data)
            
            # Colormap
            cmap = cm.get_cmap('viridis').copy()
            cmap.set_bad('#cccccc', 1.0)
            
            # Create meshgrid and plot
            lon_grid, lat_grid = np.meshgrid(lon, lat)
            
            im = ax.pcolormesh(lon_grid, lat_grid, plot_data,
                              cmap=cmap,
                              norm=colors.LogNorm(vmin=1, vmax=np.nanmax(data)),
                              shading='auto')
            
            # Colorbar and labels
            cbar = plt.colorbar(im, ax=ax, extend='max', pad=0.02)
            cbar.set_label('Precipitation (mm/month)', fontsize=12)
            
            month_name = calendar.month_name[month]
            ax.set_title(f'NASA GPM IMERG - {month_name} {year}', fontsize=14, fontweight='bold')
            ax.set_xlabel('Longitude', fontsize=12)
            ax.set_ylabel('Latitude', fontsize=12)
            ax.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            plt.close(fig)
            
        except Exception as e:
            print(f"    ⚠️ Preview error: {e}")
    
    def _create_metadata(self, metadata_path, input_file, output_file, year, month, hours_in_month, stats):
        """Create metadata file"""
        try:
            with open(metadata_path, 'w') as f:
                f.write("="*60 + "\n")
                f.write("GPM MONTHLY PROCESSING METADATA\n")
                f.write("="*60 + "\n\n")
                
                f.write(f"Processing Date: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Input File: {input_file}\n")
                f.write(f"Output File: {output_file}\n\n")
                
                f.write(f"Year: {year}\n")
                f.write(f"Month: {month} ({calendar.month_name[month]})\n")
                f.write(f"Hours in month: {hours_in_month}\n\n")
                
                f.write("STATISTICS (mm/month):\n")
                f.write(f"  Minimum: {stats['min']:.1f}\n")
                f.write(f"  Maximum: {stats['max']:.1f}\n")
                f.write(f"  Mean: {stats['mean']:.1f}\n")
                f.write(f"  Valid pixels: {stats['valid_pixels']:,}\n\n")
                
                f.write("PROCESSING:\n")
                f.write("  1. Extract precipitation from HDF5\n")
                f.write("  2. Convert mm/hr → mm/month\n")
                f.write("  3. Apply georeferencing for QGIS\n")
                f.write("  4. Save as GeoTIFF with metadata\n")
                
        except Exception as e:
            print(f"    ⚠️ Metadata error: {e}")
    
    def process_all_files(self, start_year=1998, end_year=2027, create_previews=True):
        """Process all downloaded files"""
        
        print(f"\n🔄 PROCESSING ALL FILES: {start_year} to {end_year}")
        print("="*70)
        
        if not HAS_RASTERIO:
            print("❌ rasterio not available. Install with: pip install rasterio")
            return []
        
        processed_files = []
        failed_files = []
        
        for year in range(start_year, end_year + 1):
            year_raw_dir = self.dirs['raw'] / str(year)
            
            if not year_raw_dir.exists():
                print(f"\n📁 Year {year}: No raw data directory")
                continue
            
            hdf5_files = list(year_raw_dir.glob("*.HDF5"))
            
            if not hdf5_files:
                print(f"\n📁 Year {year}: No HDF5 files found")
                continue
            
            print(f"\n📁 Year {year}: {len(hdf5_files)} files to process")
            
            for hdf5_file in sorted(hdf5_files):
                result = self.process_single_file(hdf5_file, create_previews)
                
                if result:
                    processed_files.append(result)
                else:
                    failed_files.append(hdf5_file)
        
        # Summary
        print(f"\n" + "="*70)
        print("📊 PROCESSING SUMMARY")
        print("="*70)
        print(f"Successfully processed: {len(processed_files)} files")
        print(f"Failed: {len(failed_files)} files")
        
        if failed_files:
            print("\nFailed files:")
            for file in failed_files[:10]:  # Show first 10
                print(f"  {file.name}")
            if len(failed_files) > 10:
                print(f"  ... and {len(failed_files) - 10} more")
        
        # Create processing log
        self._create_processing_log(processed_files, failed_files)
        
        return processed_files
    
    def _create_processing_log(self, processed_files, failed_files):
        """Create processing log"""
        log_file = self.dirs['logs'] / f"processing_log_{dt.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(log_file, 'w') as f:
            f.write("="*70 + "\n")
            f.write("GPM PROCESSING LOG\n")
            f.write("="*70 + "\n\n")
            
            f.write(f"Processing date: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total processed: {len(processed_files)}\n")
            f.write(f"Total failed: {len(failed_files)}\n\n")
            
            f.write("PROCESSED FILES:\n")
            f.write("-"*50 + "\n")
            for filepath in processed_files:
                size_mb = filepath.stat().st_size / (1024 * 1024) if filepath.exists() else 0
                f.write(f"{filepath.name} ({size_mb:.1f} MB)\n")
        
        print(f"\n📝 Processing log saved: {log_file}")
    
    # =========================================================================
    # COMPLETE WORKFLOW
    # =========================================================================
    
    def run_complete_workflow(self, start_year=1998, end_year=2027, skip_download=False):
        """Run complete workflow: download and process"""
        
        print("\n" + "="*70)
        print("🚀 NASA GPM COMPLETE WORKFLOW")
        print("="*70)
        
        # Check dependencies
        if not HAS_RASTERIO:
            print("❌ Missing dependency: rasterio")
            print("Install with: pip install rasterio")
            return
        
        # Step 1: Download
        downloaded_files = []
        if not skip_download:
            print("\n📥 STEP 1: DOWNLOADING DATA")
            print("-"*70)
            downloaded_files = self.download_year_range(start_year, end_year)
        else:
            print("\n📥 STEP 1: SKIPPING DOWNLOAD (using existing files)")
        
        # Step 2: Process
        print("\n🔄 STEP 2: PROCESSING DATA")
        print("-"*70)
        processed_files = self.process_all_files(start_year, end_year)
        
        # Final summary
        print("\n" + "="*70)
        print("🎉 WORKFLOW COMPLETE!")
        print("="*70)
        
        # Count files
        total_geotiffs = sum(1 for _ in self.dirs['geotiffs'].rglob("*.tif"))
        total_previews = sum(1 for _ in self.dirs['previews'].rglob("*.png"))
        
        print(f"\n📁 DIRECTORY STRUCTURE:")
        for name, path in self.dirs.items():
            if name == 'logs':
                continue
            file_count = len(list(path.rglob("*")))
            print(f"  {name}/ - {file_count} files")
        
        print(f"\n📊 OUTPUT STATISTICS:")
        print(f"  GeoTIFF files: {total_geotiffs}")
        print(f"  Preview images: {total_previews}")
        
        print(f"\n🎯 TO USE IN QGIS:")
        print(f"  1. Open QGIS")
        print(f"  2. Add basemap (XYZ Tiles → OpenStreetMap)")
        print(f"  3. Load GeoTIFFs from: {self.dirs['geotiffs']}")
        print(f"  4. They should overlay correctly with basemap")
        
        print(f"\n📁 Full output at: {self.base_dir}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    
    print("\n" + "="*70)
    print("🌧️ NASA GPM COMPLETE WORKFLOW MANAGER")
    print("="*70)
    print("Downloads GPM data (1998-2027) and processes to monthly GeoTIFFs")
    print("="*70)
    
    # Create workflow instance
    workflow = GPMCompleteWorkflow()
    
    while True:
        print("\n📋 MAIN MENU:")
        print("1. Run complete workflow (download + process)")
        print("2. Download only (1998-2027)")
        print("3. Process only (using existing downloads)")
        print("4. Check directory structure")
        print("5. Test authentication")
        print("6. Exit")
        
        choice = input("\nChoice (1-6): ").strip()
        
        if choice == '1':
            print("\n🚀 COMPLETE WORKFLOW")
            print("="*70)
            
            start_year = input("Start year [default: 1998]: ").strip()
            start_year = int(start_year) if start_year.isdigit() else 1998
            
            end_year = input("End year [default: 2027]: ").strip()
            end_year = int(end_year) if end_year.isdigit() else 2027
            
            # Confirm for large downloads
            total_months = (end_year - start_year + 1) * 12
            print(f"\n⚠️ This will process up to {total_months} months")
            print(f"   Approximate total download size: {total_months * 200 / 1024:.1f} GB")
            
            confirm = input("Continue? (y/n): ").strip().lower()
            if confirm == 'y':
                workflow.run_complete_workflow(start_year, end_year)
            else:
                print("Operation cancelled")
        
        elif choice == '2':
            print("\n📥 DOWNLOAD ONLY")
            print("="*70)
            
            start_year = input("Start year [default: 1998]: ").strip()
            start_year = int(start_year) if start_year.isdigit() else 1998
            
            end_year = input("End year [default: 2027]: ").strip()
            end_year = int(end_year) if end_year.isdigit() else 2027
            
            workflow.download_year_range(start_year, end_year)
        
        elif choice == '3':
            print("\n🔄 PROCESS ONLY")
            print("="*70)
            
            start_year = input("Start year [default: 1998]: ").strip()
            start_year = int(start_year) if start_year.isdigit() else 1998
            
            end_year = input("End year [default: 2027]: ").strip()
            end_year = int(end_year) if end_year.isdigit() else 2027
            
            create_previews = input("Create preview images? (y/n) [default: y]: ").strip().lower()
            create_previews = create_previews != 'n'
            
            workflow.process_all_files(start_year, end_year, create_previews)
        
        elif choice == '4':
            print("\n📁 DIRECTORY STRUCTURE")
            print("="*70)
            
            for name, path in workflow.dirs.items():
                if path.exists():
                    file_count = len(list(path.rglob("*")))
                    dir_count = len(list(path.rglob("*/")))
                    print(f"{name}/ - {file_count} files, {dir_count} subdirectories")
                else:
                    print(f"{name}/ - NOT CREATED")
        
        elif choice == '5':
            print("\n🔐 TEST AUTHENTICATION")
            print("="*70)
            workflow._setup_authentication()
            workflow._test_authentication()
        
        elif choice == '6':
            print("\n👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice")

# =============================================================================
# QUICK START (for testing)
# =============================================================================

def quick_start():
    """Quick start for testing with small range"""
    
    print("\n" + "="*70)
    print("🚀 GPM WORKFLOW QUICK START")
    print("="*70)
    print("Testing with 2009-2010 (2 years)")
    print("="*70)
    
    workflow = GPMCompleteWorkflow()
    
    # Run for test range
    workflow.run_complete_workflow(start_year=2009, end_year=2010)
    
    print("\n✅ Quick start complete!")
    print(f"Check output in: {workflow.base_dir}")

# =============================================================================
# DIRECT EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Check for required dependencies
    missing_deps = []
    try:
        import rasterio
    except ImportError:
        missing_deps.append("rasterio")
    
    try:
        import matplotlib
    except ImportError:
        missing_deps.append("matplotlib")
    
    if missing_deps:
        print(f"❌ Missing dependencies: {', '.join(missing_deps)}")
        print("\nInstall with:")
        print("  pip install rasterio matplotlib h5py numpy")
        print("\nThen run the script again.")
        exit(1)
    
    # Run the workflow
    print("\nSelect execution mode:")
    print("1. Complete workflow (1998-2027)")
    print("2. Quick start (test with 2009-2010)")
    print("3. Interactive menu")
    
    mode = input("\nMode (1-3): ").strip()
    
    if mode == '1':
        workflow = GPMCompleteWorkflow()
        workflow.run_complete_workflow(start_year=1998, end_year=2027)
    elif mode == '2':
        quick_start()
    elif mode == '3':
        main()
    else:
        print("❌ Invalid mode. Using interactive menu.")
        main()