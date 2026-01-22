"""
NASA GPM Daily to Monthly Processing Workflow - Adapted for Airflow
"""
import os
import urllib.request
import datetime
import time
import http.cookiejar
import urllib.error
from pathlib import Path
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

# Import processing dependencies
import rasterio
from rasterio.transform import from_origin
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from matplotlib import cm


class GPMDailyToMonthlyWorkflow:
    """Process GPM daily data to monthly aggregates - Airflow version"""
    
    def __init__(self, base_dir=None, log=None):
        # Configuration for DAILY product
        self.BASE_URL = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDF.07"
        self.VERSION = "07B"
        
        # Airflow logging
        self.log = log
        
        # Base directory structure
        if base_dir is None:
            self.base_dir = Path("/opt/airflow/data/gpm")
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
            if self.log:
                self.log.info(f"Created directory: {name}/")
    
    def _log(self, message, level='info'):
        """Log message using Airflow logger or print"""
        if self.log:
            if level == 'info':
                self.log.info(message)
            elif level == 'warning':
                self.log.warning(message)
            elif level == 'error':
                self.log.error(message)
            elif level == 'debug':
                self.log.debug(message)
        else:
            print(f"[{level.upper()}] {message}")
    
    def setup_authentication(self):
        """Setup authentication for NASA Earthdata"""
        self._log("Setting up NASA Earthdata authentication", 'info')
        
        # Try to get credentials from .netrc
        netrc_path = Path("/opt/airflow/.netrc")
        if netrc_path.exists():
            try:
                with open(netrc_path, 'r') as f:
                    content = f.read()
                    if 'urs.earthdata.nasa.gov' in content:
                        self._log("Using credentials from .netrc", 'info')
                        return True
            except Exception as e:
                self._log(f"Error reading .netrc: {e}", 'warning')
        
        self._log("No valid credentials found in .netrc", 'error')
        return False
    
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
                self._log(f"Day {day:02d}: Already exists ({size_mb:.1f} MB)", 'debug')
                return local_path
            else:
                self._log(f"Day {day:02d}: Corrupt file ({size_mb:.1f} MB), re-downloading", 'warning')
                local_path.unlink(missing_ok=True)
        
        # Download with retries
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = 2 ** attempt
                    self._log(f"Waiting {wait_time}s before retry for day {day:02d}", 'debug')
                    time.sleep(wait_time)
                
                self._log(f"Downloading day {day:02d}...", 'info')
                
                temp_path = local_path.with_suffix('.downloading')
                urllib.request.urlretrieve(url, temp_path)
                
                # Rename to final
                temp_path.rename(local_path)
                
                if local_path.exists():
                    size_mb = local_path.stat().st_size / (1024 * 1024)
                    if size_mb > 0.1:
                        self._log(f"Day {day:02d}: Downloaded ({size_mb:.1f} MB)", 'info')
                        return local_path
                    else:
                        self._log(f"Day {day:02d}: Downloaded but file is empty", 'warning')
                        local_path.unlink(missing_ok=True)
                        return None
                    
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    self._log(f"Day {day:02d}: Not available (404)", 'warning')
                    return None
                elif e.code == 401:
                    self._log(f"Authentication failed (401)", 'error')
                    break
                elif e.code == 403:
                    self._log(f"Access forbidden (403)", 'error')
                    break
                else:
                    self._log(f"HTTP Error {e.code}: {e.reason}", 'error')
            except Exception as e:
                self._log(f"Day {day:02d}: Error: {str(e)[:100]}...", 'error')
        
        self._log(f"Day {day:02d}: Failed after {max_retries} attempts", 'error')
        return None
    
    def download_month_daily_files(self, year, month, max_workers=5):
        """Download all daily files for a month in parallel"""
        
        self._log(f"Downloading daily files for {year}-{month:02d}", 'info')
        
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
                    self._log(f"Day {day:02d}: Exception: {e}", 'error')
        
        self._log(f"Downloaded: {len(downloaded_files)}/{days_in_month} files", 'info')
        return downloaded_files
    
    def process_daily_file(self, nc4_path):
        """Process a single daily NetCDF file and extract precipitation"""
        try:
            self._log(f"Opening: {nc4_path.name}", 'debug')
            
            # Check file
            if not nc4_path.exists():
                self._log("File does not exist", 'error')
                return None
            
            file_size = nc4_path.stat().st_size / (1024 * 1024)
            if file_size < 0.1:
                self._log(f"File is too small ({file_size:.1f} MB)", 'error')
                return None
            
            # Open NetCDF file
            try:
                ds = xr.open_dataset(nc4_path, engine='h5netcdf')
            except:
                try:
                    ds = xr.open_dataset(nc4_path, engine='netcdf4')
                except Exception as e:
                    self._log(f"Could not open NetCDF: {e}", 'error')
                    return None
            
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
                    self._log(f"Found precipitation variable: {precip_var}", 'debug')
                    break
            
            if precip_var is None:
                self._log("No precipitation variable found", 'error')
                ds.close()
                return None
            
            # Extract precipitation data
            precip_data = ds[precip_var].values
            
            # Remove time dimension if present
            if len(precip_data.shape) == 3:
                precip_2d = np.squeeze(precip_data, axis=0)
            else:
                precip_2d = precip_data
            
            # Get coordinates
            lat = None
            lon = None
            
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
                        self._log(f"Coordinates: {lat_name}({len(lat)}), {lon_name}({len(lon)})", 'debug')
                        break
            
            if lat is None or lon is None:
                self._log("Could not find coordinates", 'error')
                ds.close()
                return None
            
            # Get fill value and units
            fill_value = ds[precip_var].attrs.get('_FillValue', -9999.9)
            units = ds[precip_var].attrs.get('units', 'unknown')
            
            ds.close()
            
            # Handle fill values
            precip_clean = np.where(precip_2d == fill_value, np.nan, precip_2d)
            
            # Convert units if needed
            precip_mm_day = precip_clean
            
            if 'mm/hr' in str(units).lower() or 'mm h-1' in str(units).lower():
                precip_mm_day = precip_clean * 24
                self._log("Converted: mm/hr → mm/day", 'debug')
            elif 'mm/day' in str(units).lower() or 'mm d-1' in str(units).lower():
                self._log("Already mm/day", 'debug')
            else:
                self._log(f"Unknown units: {units}, assuming mm/hr", 'warning')
                precip_mm_day = precip_clean * 24
            
            # Check orientation and transpose if needed
            data_for_export = precip_mm_day
            
            if precip_mm_day.shape == (len(lon), len(lat)):
                data_for_export = precip_mm_day.T
                self._log("Transposed [lon, lat] → [lat, lon]", 'debug')
            elif precip_mm_day.shape == (len(lat), len(lon)):
                self._log("Already [lat, lon] orientation", 'debug')
            else:
                self._log("Unexpected shape, attempting auto-fix", 'warning')
                if precip_mm_day.shape[0] == len(lon) or precip_mm_day.shape[1] == len(lat):
                    data_for_export = precip_mm_day.T
                else:
                    self._log("Cannot determine orientation", 'error')
                    return None
            
            # Statistics
            valid_count = np.sum(~np.isnan(data_for_export))
            total_pixels = data_for_export.size
            
            self._log(f"Valid pixels: {valid_count}/{total_pixels} ({valid_count/total_pixels*100:.1f}%)", 'debug')
            
            return {
                'data': data_for_export,
                'lat': lat,
                'lon': lon,
                'filename': nc4_path.name,
                'valid_pixels': valid_count,
                'original_shape': precip_data.shape
            }
            
        except Exception as e:
            self._log(f"Error processing {nc4_path.name}: {e}", 'error')
            traceback.print_exc()
            return None
    
    def process_month_from_daily(self, year, month, daily_files=None):
        """Process all daily files for a month and create monthly aggregate"""
        
        self._log(f"Processing month {year}-{month:02d} from daily files", 'info')
        
        # If daily_files not provided, find them
        if daily_files is None:
            month_dir = self.dirs['raw_daily'] / str(year) / f"{month:02d}"
            if not month_dir.exists():
                self._log(f"No daily files found for {year}-{month:02d}", 'error')
                return None
            
            daily_files = list(month_dir.glob(f"*.V{self.VERSION}.nc4"))
            if not daily_files:
                self._log(f"No daily files found in {month_dir}", 'error')
                return None
        
        self._log(f"Found {len(daily_files)} daily files", 'info')
        
        # Process each file
        daily_results = []
        for i, daily_file in enumerate(sorted(daily_files)):
            self._log(f"[{i+1}/{len(daily_files)}] Processing: {daily_file.name}", 'debug')
            result = self.process_daily_file(daily_file)
            if result:
                daily_results.append(result)
                self._log("Processed successfully", 'debug')
            else:
                self._log("Failed to process", 'warning')
        
        if len(daily_results) == 0:
            self._log("No files successfully processed", 'error')
            return None
        
        self._log(f"Successfully processed {len(daily_results)}/{len(daily_files)} files", 'info')
        
        # Get reference coordinates from first valid file
        lat = daily_results[0]['lat']
        lon = daily_results[0]['lon']
        
        # Create monthly aggregate
        try:
            # Stack all daily data
            daily_arrays = [result['data'] for result in daily_results]
            daily_stack = np.stack(daily_arrays, axis=0)
            
            # Sum across time dimension (monthly total)
            monthly_total = np.nansum(daily_stack, axis=0)
            
            # Count valid days per pixel
            valid_days_count = np.sum(~np.isnan(daily_stack), axis=0)
            monthly_total[valid_days_count == 0] = np.nan
            
            # Statistics
            valid_mask = ~np.isnan(monthly_total)
            valid_count = np.sum(valid_mask)
            total_pixels = monthly_total.size
            
            self._log(f"Monthly statistics - Min: {np.nanmin(monthly_total):.1f}mm, "
                     f"Max: {np.nanmax(monthly_total):.1f}mm, "
                     f"Mean: {np.nanmean(monthly_total):.1f}mm, "
                     f"Valid pixels: {valid_count:,}", 'info')
            
            # Prepare data for export
            data_for_export = monthly_total
            
            # Flip latitude if needed
            if len(lat) > 1 and lat[0] > lat[-1]:
                data_for_export = np.flipud(data_for_export)
                lat = np.flip(lat)
                self._log("Flipped latitude (North→South to South→North)", 'debug')
            
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
                    self._log(f"Deleting existing empty file ({existing_size:.1f} MB)", 'warning')
                    output_path.unlink(missing_ok=True)
            
            # Save as GeoTIFF
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
                self._log(f"Monthly GeoTIFF created: {output_path.name}", 'info')
                return output_path
            else:
                self._log("Failed to save GeoTIFF", 'error')
                return None
            
        except Exception as e:
            self._log(f"Error during aggregation: {e}", 'error')
            traceback.print_exc()
            return None
    
    def _save_monthly_geotiff(self, data, lat, lon, output_path, year, month, daily_count):
        """Save monthly aggregated data as GeoTIFF"""
        try:
            # Validate data
            if data.size == 0:
                self._log("Data array is empty", 'error')
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
            
            # Verify
            if output_path.exists():
                file_size = output_path.stat().st_size / (1024 * 1024)
                if file_size > 0.1:
                    self._log(f"GeoTIFF saved ({file_size:.1f} MB)", 'info')
                    return True
                else:
                    self._log(f"GeoTIFF is too small ({file_size:.1f} MB)", 'error')
                    output_path.unlink(missing_ok=True)
                    return False
            else:
                self._log("GeoTIFF was not created", 'error')
                return False
            
        except Exception as e:
            self._log(f"GeoTIFF error: {e}", 'error')
            traceback.print_exc()
            if output_path.exists():
                output_path.unlink(missing_ok=True)
            return False