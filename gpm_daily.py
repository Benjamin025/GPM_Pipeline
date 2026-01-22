"""
NASA GPM DAILY TO MONTHLY PROCESSING WORKFLOW - FIXED TIME DIMENSION
Downloads daily NetCDF files, processes to monthly GeoTIFFs
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
import xarray as xr
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
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

class GPMDailyToMonthlyWorkflow:
    """Process GPM daily data to monthly aggregates"""
    
    def __init__(self, base_dir=None):
        # Configuration for DAILY product
        self.BASE_URL = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDF.07"
        self.VERSION = "07B"
        
        # Base directory structure
        if base_dir is None:
            self.base_dir = Path.home() / "Documents" / "Benjamin" / "GPM" / "GPM_gee" / "GPM_NASA_Daily"
        else:
            self.base_dir = Path(base_dir)
        
        # Create directory structure
        self.dirs = {
            'raw_daily': self.base_dir / "raw_daily_nc",
            'processed_monthly': self.base_dir / "processed_monthly",
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
        
        print(f"\n🌧️ NASA GPM DAILY TO MONTHLY WORKFLOW")
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
            ('User-Agent', 'Mozilla/5.0 (NASA-GPM-Daily-Workflow/1.0)'),
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
    # DAILY FILE DOWNLOAD METHODS
    # =========================================================================
    
    def download_daily_file(self, year, month, day, max_retries=3):
        """Download a single daily file"""
        
        # Create year/month directory structure
        year_dir = self.dirs['raw_daily'] / str(year)
        month_dir = year_dir / f"{month:02d}"
        month_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"3B-DAY.MS.MRG.3IMERG.{year}{month:02d}{day:02d}-S000000-E235959.V{self.VERSION}.nc4"
        url = f"{self.BASE_URL}/{year}/{month:02d}/{filename}"
        local_path = month_dir / filename
        
        # Check if already exists
        if local_path.exists():
            size_mb = local_path.stat().st_size / (1024 * 1024)
            if size_mb > 0.1:
                print(f"    ✓ Day {day:02d}: Already exists ({size_mb:.1f} MB)")
                return local_path
            else:
                print(f"    ⚠️ Day {day:02d}: Corrupt file ({size_mb:.1f} MB), re-downloading")
                local_path.unlink(missing_ok=True)
        
        # Download with retries
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = 2 ** attempt
                    print(f"    Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                
                print(f"    Downloading day {day:02d}...")
                
                def progress_callback(count, block_size, total_size):
                    if total_size > 0:
                        percent = min(100, int(count * block_size * 100 / total_size))
                        if percent % 25 == 0 or percent == 100:
                            mb_downloaded = count * block_size / (1024 * 1024)
                            mb_total = total_size / (1024 * 1024) if total_size > 0 else 0
                            print(f"    Day {day:02d}: {percent}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end='\r')
                
                temp_path = local_path.with_suffix('.downloading')
                urllib.request.urlretrieve(url, temp_path, reporthook=progress_callback)
                
                # Rename to final
                temp_path.rename(local_path)
                
                if local_path.exists():
                    size_mb = local_path.stat().st_size / (1024 * 1024)
                    if size_mb > 0.1:
                        print(f"\n    ✅ Day {day:02d}: Downloaded ({size_mb:.1f} MB)")
                        return local_path
                    else:
                        print(f"\n    ❌ Day {day:02d}: Downloaded but file is empty")
                        local_path.unlink(missing_ok=True)
                        return None
                    
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print(f"\n    ⚠️ Day {day:02d}: Not available (404)")
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
                print(f"\n    ❌ Day {day:02d}: Error: {str(e)[:100]}...")
        
        print(f"    ❌ Day {day:02d}: Failed after {max_retries} attempts")
        return None
    
    def download_month_daily_files(self, year, month, max_workers=5):
        """Download all daily files for a month in parallel"""
        
        print(f"\n📥 Downloading daily files for {year}-{month:02d}")
        print("-" * 50)
        
        # Get days in month
        days_in_month = calendar.monthrange(year, month)[1]
        
        downloaded_files = []
        
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit download tasks for each day
            future_to_day = {
                executor.submit(self.download_daily_file, year, month, day): day 
                for day in range(1, days_in_month + 1)
            }
            
            # Process completed downloads
            for future in as_completed(future_to_day):
                day = future_to_day[future]
                try:
                    result = future.result(timeout=300)
                    if result:
                        downloaded_files.append(result)
                except Exception as e:
                    print(f"    ❌ Day {day:02d}: Exception: {e}")
        
        print(f"    Downloaded: {len(downloaded_files)}/{days_in_month} files")
        return downloaded_files
    
    # =========================================================================
    # PROCESSING METHODS: DAILY TO MONTHLY - FIXED TIME DIMENSION
    # =========================================================================
    
    def process_daily_file(self, nc4_path):
        """Process a single daily NetCDF file and extract precipitation - FIXED"""
        try:
            print(f"  Opening: {nc4_path.name}")
            
            # Check file
            if not nc4_path.exists():
                print(f"  ❌ File does not exist")
                return None
            
            file_size = nc4_path.stat().st_size / (1024 * 1024)
            if file_size < 0.1:
                print(f"  ❌ File is too small ({file_size:.1f} MB)")
                return None
            
            # Open NetCDF file
            try:
                ds = xr.open_dataset(nc4_path, engine='h5netcdf')
            except:
                try:
                    ds = xr.open_dataset(nc4_path, engine='netcdf4')
                except Exception as e:
                    print(f"  ❌ Could not open NetCDF: {e}")
                    return None
            
            print(f"  Variables: {list(ds.data_vars)}")
            print(f"  Dimensions: {dict(ds.dims)}")
            
            # Find precipitation variable
            precip_vars_to_try = [
                'precipitationCal',  # Most common
                'precipitation',
                'precip',
                'precipitationRate',
                'HQprecipitation',
                'IRprecipitation',
                'precipCal'
            ]
            
            precip_var = None
            for var in precip_vars_to_try:
                if var in ds:
                    precip_var = var
                    print(f"  ✅ Found: {precip_var}")
                    break
            
            if precip_var is None:
                print(f"  ❌ No precipitation variable found")
                print(f"    Available: {list(ds.data_vars)}")
                ds.close()
                return None
            
            # Extract precipitation data - handle time dimension
            precip_data = ds[precip_var].values
            print(f"  Raw data shape: {precip_data.shape}")
            
            # FIX: Remove time dimension if present (shape: [time, lon, lat] or [time, lat, lon])
            if len(precip_data.shape) == 3:
                # Squeeze out time dimension
                precip_2d = np.squeeze(precip_data, axis=0)
                print(f"  Removed time dimension, new shape: {precip_2d.shape}")
            else:
                precip_2d = precip_data
            
            # Get coordinates
            lat = None
            lon = None
            
            # Try different coordinate names
            coord_attempts = [
                ('lat', 'lon'),
                ('latitude', 'longitude'),
                ('Latitude', 'Longitude'),
                ('y', 'x'),
                ('YDim', 'XDim')
            ]
            
            for lat_name, lon_name in coord_attempts:
                if lat_name in ds.coords or lat_name in ds:
                    if lon_name in ds.coords or lon_name in ds:
                        lat = ds[lat_name].values if lat_name in ds else ds.coords[lat_name].values
                        lon = ds[lon_name].values if lon_name in ds else ds.coords[lon_name].values
                        print(f"  ✅ Coords: {lat_name}({len(lat)}), {lon_name}({len(lon)})")
                        break
            
            if lat is None or lon is None:
                print(f"  ❌ Could not find coordinates")
                print(f"    Available coords: {list(ds.coords)}")
                ds.close()
                return None
            
            # Get fill value and units
            fill_value = ds[precip_var].attrs.get('_FillValue', -9999.9)
            units = ds[precip_var].attrs.get('units', 'unknown')
            
            ds.close()
            
            # Handle fill values
            precip_clean = np.where(precip_2d == fill_value, np.nan, precip_2d)
            
            # Convert units if needed
            # GPM daily is typically in mm/hr, need to convert to mm/day
            precip_mm_day = precip_clean
            conversion_note = ""
            
            if 'mm/hr' in str(units).lower() or 'mm h-1' in str(units).lower():
                precip_mm_day = precip_clean * 24
                conversion_note = " (converted mm/hr→mm/day)"
                print(f"  Converted: mm/hr → mm/day")
            elif 'mm/day' in str(units).lower() or 'mm d-1' in str(units).lower():
                print(f"  Already mm/day")
            elif 'mm' in str(units).lower():
                print(f"  Assuming mm/day")
            else:
                print(f"  ⚠️ Unknown units: {units}, assuming mm/hr")
                precip_mm_day = precip_clean * 24
                conversion_note = " (assumed mm/hr→mm/day)"
            
            # Check data orientation
            print(f"  Precipitation shape: {precip_mm_day.shape}")
            print(f"  Lat count: {len(lat)}, Lon count: {len(lon)}")
            
            # Determine if we need to transpose
            # GPM data is typically [lon, lat] but we need [lat, lon] for GeoTIFF
            data_for_export = precip_mm_day
            
            # Check orientation and transpose if needed
            if precip_mm_day.shape == (len(lon), len(lat)):
                # Shape is [lon, lat] - transpose to [lat, lon]
                print(f"  Transposing [lon, lat] → [lat, lon]")
                data_for_export = precip_mm_day.T
            elif precip_mm_day.shape == (len(lat), len(lon)):
                # Already [lat, lon] - good
                print(f"  Already [lat, lon] orientation")
            else:
                print(f"  ⚠️ Unexpected shape, attempting auto-fix")
                # Try to infer orientation
                if precip_mm_day.shape[0] == len(lon) or precip_mm_day.shape[1] == len(lat):
                    data_for_export = precip_mm_day.T
                else:
                    print(f"  ❌ Cannot determine orientation")
                    return None
            
            # Statistics
            valid_count = np.sum(~np.isnan(data_for_export))
            total_pixels = data_for_export.size
            
            print(f"  Valid pixels: {valid_count}/{total_pixels} ({valid_count/total_pixels*100:.1f}%)")
            print(f"  Daily range: {np.nanmin(data_for_export):.2f} to {np.nanmax(data_for_export):.2f} mm/day")
            
            return {
                'data': data_for_export,
                'lat': lat,
                'lon': lon,
                'fill_value': fill_value,
                'filename': nc4_path.name,
                'units': units,
                'conversion_note': conversion_note,
                'precip_var': precip_var,
                'valid_pixels': valid_count,
                'original_shape': precip_data.shape
            }
            
        except Exception as e:
            print(f"  ❌ Error processing {nc4_path.name}: {e}")
            traceback.print_exc()
            return None
    
    def process_month_from_daily(self, year, month, daily_files=None):
        """Process all daily files for a month and create monthly aggregate - FIXED"""
        
        print(f"\n🔄 Processing month {year}-{month:02d} from daily files")
        print("-" * 50)
        
        # If daily_files not provided, find them
        if daily_files is None:
            month_dir = self.dirs['raw_daily'] / str(year) / f"{month:02d}"
            if not month_dir.exists():
                print(f"  ❌ No daily files found for {year}-{month:02d}")
                return None
            
            daily_files = list(month_dir.glob(f"*.V{self.VERSION}.nc4"))
            if not daily_files:
                print(f"  ❌ No daily files found in {month_dir}")
                return None
        
        print(f"  Found {len(daily_files)} daily files")
        
        # Process each file
        daily_results = []
        for i, daily_file in enumerate(sorted(daily_files)):
            print(f"\n  [{i+1}/{len(daily_files)}] Processing: {daily_file.name}")
            result = self.process_daily_file(daily_file)
            if result:
                daily_results.append(result)
                print(f"  ✅ Processed successfully")
            else:
                print(f"  ❌ Failed to process")
        
        if len(daily_results) == 0:
            print(f"\n  ❌ No files successfully processed")
            return None
        
        print(f"\n  Successfully processed {len(daily_results)}/{len(daily_files)} files")
        
        # Get reference coordinates from first valid file
        lat = daily_results[0]['lat']
        lon = daily_results[0]['lon']
        
        print(f"  Reference grid: {len(lat)}×{len(lon)} points")
        print(f"  Expected data shape: {len(lat)}×{len(lon)}")
        
        # Verify all files have same dimensions
        all_shapes_match = True
        for i, result in enumerate(daily_results):
            if result['data'].shape != (len(lat), len(lon)):
                print(f"  ⚠️ File {i+1} shape mismatch: {result['data'].shape} != ({len(lat)}, {len(lon)})")
                all_shapes_match = False
        
        if not all_shapes_match:
            print(f"  ⚠️ Shape mismatches found, attempting to continue")
        
        # Create monthly aggregate
        print(f"\n  Creating monthly aggregate...")
        
        try:
            # Stack all daily data
            daily_arrays = [result['data'] for result in daily_results]
            daily_stack = np.stack(daily_arrays, axis=0)
            print(f"  Daily stack shape: {daily_stack.shape} [days, lat, lon]")
            
            # Sum across time dimension (monthly total)
            monthly_total = np.nansum(daily_stack, axis=0)
            print(f"  Monthly total shape: {monthly_total.shape}")
            
            # Count valid days per pixel
            valid_days_count = np.sum(~np.isnan(daily_stack), axis=0)
            # Set pixels with no valid days to NaN
            monthly_total[valid_days_count == 0] = np.nan
            
            # Statistics
            valid_mask = ~np.isnan(monthly_total)
            valid_count = np.sum(valid_mask)
            total_pixels = monthly_total.size
            
            print(f"\n  📊 MONTHLY STATISTICS:")
            print(f"    Min: {np.nanmin(monthly_total):.1f} mm")
            print(f"    Max: {np.nanmax(monthly_total):.1f} mm")
            print(f"    Mean: {np.nanmean(monthly_total):.1f} mm")
            print(f"    Valid pixels: {valid_count:,} ({valid_count/total_pixels*100:.1f}%)")
            print(f"    Days with data: {len(daily_arrays)}")
            
            # Prepare data for export
            data_for_export = monthly_total
            
            # Flip latitude if needed (GPM typically has North to South)
            if len(lat) > 1 and lat[0] > lat[-1]:
                print(f"  Flipping latitude (North→South to South→North)")
                data_for_export = np.flipud(data_for_export)
                lat = np.flip(lat)
            
            # Create output directories
            year_geotiff_dir = self.dirs['geotiffs'] / str(year)
            year_geotiff_dir.mkdir(parents=True, exist_ok=True)
            
            # Create output filename
            output_filename = f"GPM_{year}_{month:02d}_monthly_from_daily.tif"
            output_path = year_geotiff_dir / output_filename
            
            # Delete existing file if it's empty
            if output_path.exists():
                existing_size = output_path.stat().st_size / (1024 * 1024)
                if existing_size < 1.0:
                    print(f"  Deleting existing empty file ({existing_size:.1f} MB)")
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
                    daily_count=len(daily_arrays)
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
                    
                    # Create metadata
                    metadata_dir = self.dirs['metadata'] / str(year)
                    metadata_dir.mkdir(exist_ok=True)
                    metadata_path = metadata_dir / f"GPM_{year}_{month:02d}_metadata.txt"
                    self._create_monthly_metadata(
                        metadata_path=metadata_path,
                        year=year,
                        month=month,
                        daily_files_processed=len(daily_arrays),
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
            
        except Exception as e:
            print(f"\n  ❌ Error during aggregation: {e}")
            traceback.print_exc()
            return None
    
    def _save_monthly_geotiff(self, data, lat, lon, output_path, year, month, daily_count):
        """Save monthly aggregated data as GeoTIFF"""
        try:
            # Validate data
            if data.size == 0:
                print(f"    ❌ Data array is empty")
                return False
            
            if np.all(np.isnan(data)):
                print(f"    ⚠️ All data is NaN")
            
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
            
            print(f"    Data shape: {data.shape}")
            print(f"    Extent: {left:.3f}, {bottom:.3f}, {right:.3f}, {top:.3f}")
            print(f"    Pixel size: {pixel_width:.3f}° × {pixel_height:.3f}°")
            print(f"    Data range: {np.nanmin(data):.1f} to {np.nanmax(data):.1f} mm")
            
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
                    SOURCE='NASA GPM IMERG Daily V07B',
                    DAILY_FILES_USED=str(daily_count),
                    RESOLUTION=f'{pixel_width:.3f} degree',
                    YEAR=str(year),
                    MONTH=str(month),
                    PROCESSING_DATE=dt.now().isoformat()
                )
                
                dst.set_band_description(1, f'Monthly Precipitation {year}-{month:02d}')
            
            # Verify
            if output_path.exists():
                file_size = output_path.stat().st_size / (1024 * 1024)
                if file_size > 0.1:
                    print(f"    ✅ GeoTIFF saved ({file_size:.1f} MB)")
                    return True
                else:
                    print(f"    ❌ GeoTIFF is too small ({file_size:.1f} MB)")
                    output_path.unlink(missing_ok=True)
                    return False
            else:
                print(f"    ❌ GeoTIFF was not created")
                return False
            
        except Exception as e:
            print(f"    ❌ GeoTIFF error: {e}")
            traceback.print_exc()
            if output_path.exists():
                output_path.unlink(missing_ok=True)
            return False
    
    def _create_preview(self, data, lat, lon, output_path, year, month):
        """Create preview plot of monthly data"""
        try:
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Prepare data for plotting
            plot_data = np.where(np.isnan(data), np.nan, data)
            
            if np.all(np.isnan(plot_data)) or np.nanmax(plot_data) <= 0:
                print(f"    ⚠️ No valid data for preview")
                plt.close(fig)
                return
            
            # Create meshgrid
            lon_grid, lat_grid = np.meshgrid(lon, lat)
            
            # Create plot
            im = ax.pcolormesh(lon_grid, lat_grid, plot_data,
                              cmap='viridis',
                              shading='auto',
                              vmin=0,
                              vmax=np.nanpercentile(plot_data, 95))
            
            # Colorbar
            cbar = plt.colorbar(im, ax=ax, extend='max')
            cbar.set_label('Precipitation (mm/month)', fontsize=12)
            
            # Title and labels
            month_name = calendar.month_name[month]
            ax.set_title(f'NASA GPM IMERG - {month_name} {year}\nMonthly Precipitation from Daily Data', 
                        fontsize=14, fontweight='bold')
            ax.set_xlabel('Longitude', fontsize=12)
            ax.set_ylabel('Latitude', fontsize=12)
            ax.grid(True, alpha=0.3)
            
            # Add stats text
            stats_text = f'Min: {np.nanmin(plot_data):.0f} mm\nMax: {np.nanmax(plot_data):.0f} mm\nMean: {np.nanmean(plot_data):.0f} mm'
            ax.text(0.02, 0.98, stats_text,
                   transform=ax.transAxes,
                   verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
            
            plt.tight_layout()
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            plt.close(fig)
            
            print(f"    Preview saved: {output_path.name}")
            
        except Exception as e:
            print(f"    ⚠️ Preview error: {e}")
    
    def _create_monthly_metadata(self, metadata_path, year, month, daily_files_processed, stats):
        """Create metadata file for monthly product"""
        try:
            with open(metadata_path, 'w') as f:
                f.write("="*60 + "\n")
                f.write("GPM MONTHLY PROCESSING METADATA\n")
                f.write("="*60 + "\n\n")
                
                f.write(f"Processing Date: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Year: {year}\n")
                f.write(f"Month: {month} ({calendar.month_name[month]})\n")
                f.write(f"Daily files processed: {daily_files_processed}\n")
                f.write(f"Source: NASA GPM IMERG Daily V07B\n\n")
                
                f.write("STATISTICS (mm/month):\n")
                f.write(f"  Minimum: {stats['min']:.1f}\n")
                f.write(f"  Maximum: {stats['max']:.1f}\n")
                f.write(f"  Mean: {stats['mean']:.1f}\n")
                f.write(f"  Valid pixels: {stats['valid_pixels']:,}\n")
                f.write(f"  Total pixels: {stats['total_pixels']:,}\n")
                f.write(f"  Coverage: {stats['valid_pixels']/stats['total_pixels']*100:.1f}%\n\n")
                
                f.write("PROCESSING STEPS:\n")
                f.write("  1. Download daily GPM NetCDF files\n")
                f.write("  2. Extract precipitation data (precipitationCal variable)\n")
                f.write("  3. Remove time dimension (squeeze)\n")
                f.write("  4. Convert mm/hr to mm/day (if needed)\n")
                f.write("  5. Transpose [lon, lat] → [lat, lon]\n")
                f.write("  6. Sum daily precipitation to monthly total\n")
                f.write("  7. Apply WGS84 georeferencing\n")
                f.write("  8. Save as compressed GeoTIFF\n")
                
            print(f"    Metadata saved: {metadata_path.name}")
            
        except Exception as e:
            print(f"    ⚠️ Metadata error: {e}")
    
    # =========================================================================
    # MAIN WORKFLOW METHODS
    # =========================================================================
    
    def download_year_range(self, start_year=1998, end_year=2025):
        """Download daily files for a range of years - FIXED END YEAR LOGIC"""
        
        print(f"\n📥 DOWNLOADING DAILY FILES: {start_year} to {end_year}")
        print("="*70)
        
        # Setup authentication
        self._setup_authentication()
        
        # Test authentication
        if not self._test_authentication():
            print("❌ Authentication failed. Exiting.")
            return []
        
        all_downloaded = []
        current_year = dt.now().year
        current_month = dt.now().month
        
        for year in range(start_year, end_year + 1):
            print(f"\n📅 Year {year}:")
            
            # Determine months for this year
            if year == start_year:
                start_month = 1
            else:
                start_month = 1
            
            if year == end_year:
                # CRITICAL FIX: If end_year is 2025, we want ALL of 2025
                # Only limit if we're trying to download the current year
                if end_year == current_year:
                    # For current year, data might not be complete
                    end_month = current_month  # Only up to current month
                    print(f"  ⚠️  Current year - will download up to month {end_month}")
                    print(f"     (NASA data typically has 2-3 month latency)")
                else:
                    # For past years (like 2025), try all 12 months
                    end_month = 12
                    print(f"  Past year - attempting all 12 months")
            else:
                end_month = 12
            
            # Check what's actually available
            print(f"  Checking availability for months {start_month} to {end_month}...")
            
            for month in range(start_month, end_month + 1):
                print(f"\n  Month {month:02d}:")
                
                # First check if the month directory exists on server
                month_url = f"{self.BASE_URL}/{year}/{month:02d}/"
                try:
                    request = urllib.request.Request(month_url, method='HEAD')
                    response = urllib.request.urlopen(request, timeout=5)
                    if response.getcode() != 200:
                        print(f"  ⚠️  Month directory not available (HTTP {response.getcode()})")
                        continue
                except urllib.error.HTTPError as e:
                    if e.code == 404:
                        print(f"  ⚠️  Month directory not found (404)")
                        continue
                    elif e.code == 403:
                        print(f"  ⚠️  Access denied (403)")
                        continue
                    else:
                        print(f"  ⚠️  HTTP Error {e.code}")
                        continue
                except Exception:
                    # Try anyway
                    pass
                
                # Check if monthly GeoTIFF already exists
                geotiff_path = self.dirs['geotiffs'] / str(year) / f"GPM_{year}_{month:02d}_monthly_from_daily.tif"
                if geotiff_path.exists():
                    size_mb = geotiff_path.stat().st_size / (1024 * 1024) if geotiff_path.exists() else 0
                    if size_mb > 1.0:
                        print(f"  ⚠️ Monthly GeoTIFF already exists ({size_mb:.1f} MB), skipping download")
                        continue
                    else:
                        print(f"  ⚠️ Corrupt/incomplete GeoTIFF ({size_mb:.1f} MB), re-processing")
                        geotiff_path.unlink(missing_ok=True)
                
                downloaded = self.download_month_daily_files(year, month)
                all_downloaded.extend(downloaded)
                
                # Process immediately after downloading month
                if downloaded and len(downloaded) >= 15:
                    print(f"  Processing {len(downloaded)} files...")
                    result = self.process_month_from_daily(year, month, downloaded)
                    if result and result.exists():
                        result_size = result.stat().st_size / (1024 * 1024)
                        print(f"  ✅ Created monthly GeoTIFF ({result_size:.1f} MB)")
                elif downloaded:
                    print(f"  ⚠️ Only {len(downloaded)} files downloaded, not enough for processing")
        
        return all_downloaded
    
    def fix_empty_geotiffs(self):
        """Find and fix empty GeoTIFF files"""
        print(f"\n🔍 CHECKING FOR EMPTY GEOTIFF FILES")
        print("="*70)
        
        empty_files = []
        total_files = 0
        
        for geotiff_path in self.dirs['geotiffs'].rglob("*.tif"):
            total_files += 1
            if geotiff_path.exists():
                file_size = geotiff_path.stat().st_size / (1024 * 1024)
                if file_size < 1.0:
                    empty_files.append((geotiff_path, file_size))
                    print(f"  ❌ {geotiff_path.relative_to(self.base_dir)}: {file_size:.1f} MB")
        
        if empty_files:
            print(f"\nFound {len(empty_files)} empty/small files:")
            for file_path, size in empty_files:
                print(f"  • {file_path.name}: {size:.1f} MB")
            
            confirm = input(f"\nDelete these {len(empty_files)} files? (y/n): ").strip().lower()
            if confirm == 'y':
                for file_path, _ in empty_files:
                    file_path.unlink(missing_ok=True)
                    print(f"  Deleted: {file_path.name}")
                print(f"\n✅ Deleted {len(empty_files)} empty files")
        else:
            print(f"✅ All {total_files} GeoTIFF files appear valid (>1MB)")
    
    def reprocess_failed_months(self, start_year=1998, end_year=2025):
        """Reprocess months that failed previously"""
        print(f"\n🔄 REPROCESSING FAILED MONTHS: {start_year} to {end_year}")
        print("="*70)
        
        for year in range(start_year, end_year + 1):
            year_dir = self.dirs['raw_daily'] / str(year)
            if not year_dir.exists():
                continue
            
            month_dirs = [d for d in year_dir.iterdir() if d.is_dir()]
            for month_dir in sorted(month_dirs):
                try:
                    month = int(month_dir.name)
                except:
                    continue
                
                # Check GeoTIFF
                geotiff_path = self.dirs['geotiffs'] / str(year) / f"GPM_{year}_{month:02d}_monthly_from_daily.tif"
                
                if geotiff_path.exists():
                    file_size = geotiff_path.stat().st_size / (1024 * 1024)
                    if file_size > 1.0:
                        continue
                
                # Reprocess this month
                print(f"\n  Reprocessing {year}-{month:02d}")
                daily_files = list(month_dir.glob(f"*.V{self.VERSION}.nc4"))
                
                if daily_files:
                    print(f"  Found {len(daily_files)} daily files")
                    result = self.process_month_from_daily(year, month, daily_files)
                    
                    if result and result.exists():
                        new_size = result.stat().st_size / (1024 * 1024)
                        print(f"  ✅ Created: {new_size:.1f} MB")
                    else:
                        print(f"  ❌ Failed to reprocess")
                else:
                    print(f"  ⚠️ No daily files found")
    
    def run_complete_workflow(self, start_year=1998, end_year=2025, skip_download=False):
        """Run complete workflow: download daily and process to monthly"""
        
        print("\n" + "="*70)
        print("🚀 NASA GPM DAILY TO MONTHLY COMPLETE WORKFLOW")
        print("="*70)
        
        # Check dependencies
        if not HAS_RASTERIO:
            print("❌ Missing dependency: rasterio")
            print("Install with: pip install rasterio")
            return
        
        try:
            import xarray as xr
        except ImportError:
            print("❌ Missing dependency: xarray")
            print("Install with: pip install xarray h5netcdf netcdf4")
            return
        
        # Step 0: Fix any existing empty files
        print("\n🔧 STEP 0: CHECKING EXISTING FILES")
        print("-"*70)
        self.fix_empty_geotiffs()
        
        # Step 1: Download
        if not skip_download:
            print("\n📥 STEP 1: DOWNLOADING DAILY DATA")
            print("-"*70)
            self.download_year_range(start_year, end_year)
        else:
            print("\n📥 STEP 1: SKIPPING DOWNLOAD (using existing files)")
        
        # Step 2: Process to monthly
        print("\n🔄 STEP 2: PROCESSING DAILY TO MONTHLY")
        print("-"*70)
        
        # Process each month
        for year in range(start_year, end_year + 1):
            year_dir = self.dirs['raw_daily'] / str(year)
            if not year_dir.exists():
                continue
            
            month_dirs = [d for d in year_dir.iterdir() if d.is_dir()]
            for month_dir in sorted(month_dirs):
                try:
                    month = int(month_dir.name)
                except:
                    continue
                
                # Check if we should process
                geotiff_path = self.dirs['geotiffs'] / str(year) / f"GPM_{year}_{month:02d}_monthly_from_daily.tif"
                if geotiff_path.exists():
                    file_size = geotiff_path.stat().st_size / (1024 * 1024)
                    if file_size > 1.0:
                        print(f"  {year}-{month:02d}: Already processed ({file_size:.1f} MB)")
                        continue
                
                # Process month
                print(f"\n  Processing {year}-{month:02d}")
                daily_files = list(month_dir.glob(f"*.V{self.VERSION}.nc4"))
                
                if daily_files:
                    result = self.process_month_from_daily(year, month, daily_files)
                    if result:
                        result_size = result.stat().st_size / (1024 * 1024) if result.exists() else 0
                        print(f"  Result: {result_size:.1f} MB")
                else:
                    print(f"  ⚠️ No daily files found")
        
        # Step 3: Final check
        print("\n🔍 STEP 3: FINAL CHECK")
        print("-"*70)
        self.fix_empty_geotiffs()
        
        # Final summary
        print("\n" + "="*70)
        print("🎉 WORKFLOW COMPLETE!")
        print("="*70)
        
        # Count files
        total_geotiffs = 0
        total_geotiffs_size = 0
        for geotiff_path in self.dirs['geotiffs'].rglob("*.tif"):
            if geotiff_path.exists():
                total_geotiffs += 1
                total_geotiffs_size += geotiff_path.stat().st_size / (1024 * 1024)
        
        total_daily = sum(1 for _ in self.dirs['raw_daily'].rglob("*.nc4"))
        
        print(f"\n📊 OUTPUT STATISTICS:")
        print(f"  Daily NetCDF files: {total_daily}")
        print(f"  Monthly GeoTIFFs: {total_geotiffs}")
        print(f"  Total GeoTIFF size: {total_geotiffs_size:.1f} MB")
        
        print(f"\n🎯 TO USE IN QGIS:")
        print(f"  1. Open QGIS")
        print(f"  2. Add basemap (XYZ Tiles → OpenStreetMap)")
        print(f"  3. Load monthly GeoTIFFs from: {self.dirs['geotiffs']}")
        print(f"  4. Files are organized by year: {self.dirs['geotiffs']}/YYYY/")
        
        print(f"\n📁 Full output at: {self.base_dir}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    
    # Check for required dependencies
    missing_deps = []
    try:
        import rasterio
    except ImportError:
        missing_deps.append("rasterio")
    
    try:
        import xarray
    except ImportError:
        missing_deps.append("xarray")
    
    if missing_deps:
        print(f"❌ Missing dependencies: {', '.join(missing_deps)}")
        print("\nInstall with:")
        print("  pip install rasterio xarray h5netcdf netcdf4 h5py numpy matplotlib")
        print("\nThen run the script again.")
        exit(1)
    
    print("\n" + "="*70)
    print("🌧️ NASA GPM DAILY TO MONTHLY WORKFLOW - FIXED TIME DIMENSION")
    print("="*70)
    print("Now handles shape: (1, 3600, 1800) → (3600, 1800) → (1800, 3600)")
    print("="*70)
    
    # Create workflow instance
    workflow = GPMDailyToMonthlyWorkflow()
    
    print("\nSelect workflow mode:")
    print("1. Complete workflow (download + process, 1998-2025)")
    print("2. Fix empty GeoTIFF files")
    print("3. Reprocess failed months")
    print("4. Test single month (January 1998)")
    print("5. Download only (no processing)")
    
    choice = input("\nChoice (1-5): ").strip()
    
    if choice == '1':
        start_year = input("Start year [default: 1998]: ").strip()
        start_year = int(start_year) if start_year.isdigit() else 1998
        
        end_year = input("End year [default: 2025]: ").strip()
        end_year = int(end_year) if end_year.isdigit() else 2025
        
        # Warn about disk space
        total_months = (end_year - start_year + 1) * 12
        total_days = total_months * 30
        est_size_gb = (total_days * 5 / 1024) + (total_months * 25 / 1024)
        print(f"\n⚠️ Estimated disk space needed: {est_size_gb:.1f} GB")
        
        confirm = input("Continue? (y/n): ").strip().lower()
        if confirm == 'y':
            workflow.run_complete_workflow(start_year, end_year)
        else:
            print("Operation cancelled")
    
    elif choice == '2':
        workflow.fix_empty_geotiffs()
    
    elif choice == '3':
        start_year = input("Start year [default: 1998]: ").strip()
        start_year = int(start_year) if start_year.isdigit() else 1998
        
        end_year = input("End year [default: 2025]: ").strip()
        end_year = int(end_year) if end_year.isdigit() else 2025
        
        workflow.reprocess_failed_months(start_year, end_year)
    
    elif choice == '4':
        print("\n🧪 TESTING JANUARY 1998")
        print("="*70)
        
        # First check and fix any empty files
        workflow.fix_empty_geotiffs()
        
        # Process January 1998
        year, month = 1998, 1
        month_dir = workflow.dirs['raw_daily'] / str(year) / f"{month:02d}"
        
        if month_dir.exists():
            daily_files = list(month_dir.glob("*.V07B.nc4"))
            if daily_files:
                print(f"\nFound {len(daily_files)} daily files for {year}-{month:02d}")
                result = workflow.process_month_from_daily(year, month, daily_files)
                if result and result.exists():
                    file_size = result.stat().st_size / (1024 * 1024)
                    print(f"\n✅ Created: {result.name} ({file_size:.1f} MB)")
                else:
                    print(f"\n❌ Processing failed")
            else:
                print(f"\n❌ No daily files found. Try downloading first.")
        else:
            print(f"\n❌ No data directory found for {year}-{month:02d}")
    
    elif choice == '5':
        start_year = input("Start year [default: 1998]: ").strip()
        start_year = int(start_year) if start_year.isdigit() else 1998
        
        end_year = input("End year [default: 2025]: ").strip()
        end_year = int(end_year) if end_year.isdigit() else 2025
        
        workflow._setup_authentication()
        if workflow._test_authentication():
            workflow.download_year_range(start_year, end_year)
    
    else:
        print("❌ Invalid choice")