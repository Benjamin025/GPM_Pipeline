# GPM pipeline using Google Earth  Engine collections
"""
GPM V7 PIPELINE WITH NETCDF EXPORT - ORGANIZED RASTERS
Organizes raster files into year-specific folders
"""

import ee
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import json
from tqdm import tqdm
import warnings
import xarray as xr
import requests
import zipfile
import io
import rasterio
import time
import shutil
warnings.filterwarnings('ignore')

# ============================================================================
# 1. CONFIGURATION
# ============================================================================

class GPMConfig:
    """Configuration for GPM pipeline with NetCDF support"""
    
    def __init__(self):
        # Data source
        self.DATASET = 'NASA/GPM_L3/IMERG_MONTHLY_V07'
        self.PRECIP_BAND = 'precipitation'  # mm/hour
        
        # Time period
        self.START_DATE = '2020-01-01'
        self.END_DATE = '2024-12-31'
        
        # Africa bounding box
        self.AFRICA_BBOX = ee.Geometry.Rectangle([-25, -35, 60, 38])
        
        # Resolution and export settings
        self.SCALE = 27830  # ~0.25 degree for better performance
        self.MAX_PIXELS = 1e9
        
        # Output directories
        self.OUTPUT_BASE = './gpm_netcdf_output'
        self.CSV_DIR = os.path.join(self.OUTPUT_BASE, 'csv')
        self.NETCDF_DIR = os.path.join(self.OUTPUT_BASE, 'netcdf')
        self.RASTER_DIR = os.path.join(self.OUTPUT_BASE, 'raster')
        self.LOG_DIR = os.path.join(self.OUTPUT_BASE, 'logs')
        self.SUMMARY_DIR = os.path.join(self.OUTPUT_BASE, 'summaries')
        self.TEMP_DIR = os.path.join(self.OUTPUT_BASE, 'temp')
        self.PLOTS_DIR = os.path.join(self.OUTPUT_BASE, 'plots')
        
        # Create directories
        self.create_directories()
        
        # Regional groups
        self.REGIONS = {
            'east_africa': ['Kenya', 'Tanzania', 'Uganda', 'Ethiopia', 'Rwanda'],
            'west_africa': ['Nigeria', 'Ghana', 'Ivory Coast', 'Senegal'],
            'north_africa': ['Egypt', 'Algeria', 'Morocco', 'Sudan'],
            'southern_africa': ['South Africa', 'Namibia', 'Mozambique', 'Zimbabwe'],
            'central_africa': ['DR Congo', 'Cameroon', 'Gabon', 'Congo'],
            'test': ['Kenya', 'Nigeria']
        }
        
        # Country name mappings
        self.COUNTRY_ALIASES = {
            'DR Congo': 'Democratic Republic of the Congo',
            'Ivory Coast': "Côte d'Ivoire",
            'Tanzania': 'United Republic of Tanzania',
            'Congo': 'Republic of the Congo'
        }
    
    def create_directories(self):
        """Create necessary output directories"""
        dirs = [
            self.OUTPUT_BASE, 
            self.CSV_DIR, 
            self.NETCDF_DIR,
            self.RASTER_DIR,
            self.LOG_DIR,
            self.SUMMARY_DIR,
            self.TEMP_DIR,
            self.PLOTS_DIR
        ]
        
        for dir_path in dirs:
            try:
                os.makedirs(dir_path, exist_ok=True)
                print(f"📁 Created directory: {dir_path}")
            except Exception as e:
                print(f"❌ Error creating directory {dir_path}: {e}")
        
        return True
    
    def get_raster_year_dir(self, region_name: str, year: int):
        """Get the raster directory for a specific region and year"""
        # Clean region name for directory name
        region_dir = region_name.lower().replace(' ', '_')
        
        # Create year-specific directory
        year_dir = os.path.join(self.RASTER_DIR, region_dir, str(year))
        
        try:
            os.makedirs(year_dir, exist_ok=True)
            return year_dir
        except Exception as e:
            print(f"❌ Error creating year directory {year_dir}: {e}")
            return os.path.join(self.RASTER_DIR, region_dir)

def initialize_ee():
    """Initialize Earth Engine"""
    try:
        ee.Initialize(project="ee-my-ndungu")
        print("✅ Earth Engine initialized")
        return True
    except Exception as e:
        print(f"⚠️ Trying authentication: {e}")
        try:
            ee.Authenticate()
            ee.Initialize(project="ee-my-ndungu")
            print("✅ Earth Engine authenticated and initialized")
            return True
        except Exception as auth_error:
            print(f"❌ Authentication failed: {auth_error}")
            return False

# ============================================================================
# 2. CORE PROCESSING CLASS
# ============================================================================

class GPMNetCDFProcessor:
    """Processor for GPM data with NetCDF export capability"""
    
    def __init__(self, config: GPMConfig = None):
        self.config = config or GPMConfig()
        self.log_file = os.path.join(
            self.config.LOG_DIR, 
            f"gpm_processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        
        # Initialize log
        with open(self.log_file, 'w') as f:
            f.write(f"GPM Processor Log - Started at {datetime.now()}\n")
        
        print(f"✅ GPM NetCDF Processor initialized")
        print(f"📄 Log file: {self.log_file}")
        
        # Cache for country geometries
        self._geometry_cache = {}
    
    def log_message(self, message: str, level: str = "INFO"):
        """Log messages to file and console"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] [{level}] {message}"
        
        # Print to console
        if level in ["INFO", "ERROR", "WARNING"]:
            print(log_entry)
        
        # Write to log file
        with open(self.log_file, 'a') as f:
            f.write(log_entry + '\n')
    
    # ============================================================================
    # 3. GEOMETRY HANDLING
    # ============================================================================
    
    def get_africa_geometry(self):
        """Get Africa continent geometry"""
        return self.config.AFRICA_BBOX
    
    def get_country_geometry(self, country_name: str):
        """Get geometry for a specific country"""
        # Check cache first
        if country_name in self._geometry_cache:
            return self._geometry_cache[country_name]
        
        # Handle aliases
        actual_name = self.config.COUNTRY_ALIASES.get(country_name, country_name)
        
        try:
            # Use FAO/GAUL dataset
            countries = ee.FeatureCollection('FAO/GAUL/2015/level0')
            filtered = countries.filter(ee.Filter.eq('ADM0_NAME', actual_name))
            
            # Check if we have any features
            count = filtered.size().getInfo()
            
            if count > 0:
                # Get the first feature
                country_feature = filtered.first()
                geometry = country_feature.geometry()
                
                # Cache it
                self._geometry_cache[country_name] = geometry
                return geometry
            else:
                self.log_message(f"⚠️ Country {country_name} not found, using Africa bbox", "WARNING")
                return self.config.AFRICA_BBOX
                
        except Exception as e:
            self.log_message(f"Error getting geometry for {country_name}: {e}", "ERROR")
            return self.config.AFRICA_BBOX
    
    def get_geometry_bounds(self, geometry):
        """Get bounds from geometry - handles different geometry types"""
        try:
            bounds_info = geometry.bounds().getInfo()
            
            # Handle different return formats
            if isinstance(bounds_info, dict) and 'coordinates' in bounds_info:
                # Geometry object format
                coords = bounds_info['coordinates'][0]
                lons = [coord[0] for coord in coords]
                lats = [coord[1] for coord in coords]
                west = min(lons)
                east = max(lons)
                south = min(lats)
                north = max(lats)
            elif isinstance(bounds_info, list):
                # Simple list format [west, south, east, north]
                if len(bounds_info) == 4:
                    west, south, east, north = bounds_info
                else:
                    # Try to extract from nested lists
                    flattened = self._flatten_list(bounds_info)
                    if len(flattened) >= 4:
                        west = min(flattened[0::2])  # Odd indices are longitudes
                        east = max(flattened[0::2])
                        south = min(flattened[1::2])  # Even indices are latitudes
                        north = max(flattened[1::2])
                    else:
                        # Default Africa bounds
                        west, south, east, north = -25, -35, 60, 38
            else:
                # Default Africa bounds
                west, south, east, north = -25, -35, 60, 38
            
            return west, south, east, north
            
        except Exception as e:
            self.log_message(f"Error getting bounds: {e}, using default", "ERROR")
            return -25, -35, 60, 38  # Default Africa bounds
    
    def _flatten_list(self, nested_list):
        """Flatten a nested list"""
        flat_list = []
        for item in nested_list:
            if isinstance(item, list):
                flat_list.extend(self._flatten_list(item))
            else:
                flat_list.append(item)
        return flat_list
    
    # ============================================================================
    # 4. DATA PROCESSING METHODS
    # ============================================================================
    
    def load_gpm_data(self, start_date: str = None, end_date: str = None):
        """Load GPM monthly precipitation data"""
        start_date = start_date or self.config.START_DATE
        end_date = end_date or self.config.END_DATE
        
        self.log_message(f"Loading GPM data from {start_date} to {end_date}")
        
        try:
            gpm_collection = ee.ImageCollection(self.config.DATASET) \
                .filterDate(start_date, end_date) \
                .filterBounds(self.config.AFRICA_BBOX) \
                .select(self.config.PRECIP_BAND)
            
            # Get collection size
            size = gpm_collection.size().getInfo()
            self.log_message(f"✅ Loaded {size} monthly images")
            
            return gpm_collection
            
        except Exception as e:
            self.log_message(f"❌ Error loading GPM data: {e}", "ERROR")
            raise
    
    def convert_to_monthly_accumulated(self, image):
        """Convert GPM precipitation from mm/hour to mm/month"""
        date = ee.Date(image.get('system:time_start'))
        
        # Calculate hours in the month
        days_in_month = date.advance(1, 'month').difference(date, 'day')
        hours_in_month = days_in_month.multiply(24)
        
        # Convert: mm/hour * hours_in_month = mm/month
        accumulated = image \
            .select(self.config.PRECIP_BAND) \
            .multiply(hours_in_month) \
            .rename('precipitation_mm')
        
        return accumulated.set({
            'year': date.get('year'),
            'month': date.get('month'),
            'date': date.format('YYYY-MM-dd'),
            'hours_in_month': hours_in_month,
            'original_units': 'mm/hour',
            'converted_units': 'mm/month'
        })
    
    # ============================================================================
    # 5. DOWNLOAD METHODS WITH ORGANIZED FOLDERS
    # ============================================================================
    
    def download_raster(self, image, geometry, region_name: str, year: int, month: int):
        """
        Download raster data to organized folder structure
        Saves to: ./gpm_netcdf_output/raster/{region_name}/{year}/{region_name}_{year}_{month:02d}.tif
        """
        # Get the year-specific directory
        year_dir = self.config.get_raster_year_dir(region_name, year)
        
        # Create filename
        filename = f"{region_name.lower().replace(' ', '_')}_{year}_{month:02d}.tif"
        local_path = os.path.join(year_dir, filename)
        
        try:
            # Get download URL
            self.log_message(f"  Requesting download URL for {region_name} {year}-{month:02d}")
            
            url = image.getDownloadURL({
                'scale': self.config.SCALE,
                'region': geometry,
                'format': 'GEO_TIFF',
                'crs': 'EPSG:4326'
            })
            
            self.log_message(f"  Downloading from URL...")
            
            # Download with timeout and headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=180)
            
            if response.status_code != 200:
                raise Exception(f"Download failed with status {response.status_code}")
            
            content = response.content
            
            # Check if it's a zip file
            is_zip = False
            try:
                # Try to read as zip
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    is_zip = True
            except:
                is_zip = False
            
            if is_zip:
                # Extract from zip
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    # Find tif file
                    tif_files = [f for f in z.namelist() if f.endswith('.tif') or f.endswith('.tiff')]
                    if tif_files:
                        tif_filename = tif_files[0]
                        tif_data = z.read(tif_filename)
                        
                        # Save to file
                        with open(local_path, 'wb') as f:
                            f.write(tif_data)
                    else:
                        raise Exception("No GeoTIFF file found in zip")
            else:
                # Direct GeoTIFF download
                with open(local_path, 'wb') as f:
                    f.write(content)
            
            # Verify it's a valid GeoTIFF
            try:
                with rasterio.open(local_path) as test:
                    pass  # File is valid
            except:
                raise Exception("Downloaded file is not a valid GeoTIFF")
            
            self.log_message(f"  ✅ Raster saved to: {local_path}")
            return local_path
            
        except Exception as e:
            self.log_message(f"  ❌ Download failed: {str(e)[:200]}", "ERROR")
            # Clean up any partial files
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except:
                    pass
            return None
    
    # ============================================================================
    # 6. NETCDF EXPORT WITH ORGANIZED RASTERS
    # ============================================================================
    
    def create_proper_annual_netcdf(self, year: int, region_name: str = "Africa"):
        """
        Create proper NetCDF file for a specific year
        Contains 12 bands (months) with proper metadata
        Raster files saved to organized year-specific folders
        """
        self.log_message(f"📅 Creating annual NetCDF for {region_name} {year}")
        
        # Get geometry
        if region_name == "Africa":
            geometry = self.get_africa_geometry()
        else:
            geometry = self.get_country_geometry(region_name)
        
        if not geometry:
            self.log_message(f"❌ No geometry for {region_name}", "ERROR")
            return None
        
        # Get bounds from geometry
        west, south, east, north = self.get_geometry_bounds(geometry)
        self.log_message(f"  Using bounds: west={west}, south={south}, east={east}, north={north}")
        
        # Create arrays to store monthly data
        monthly_data = []
        month_dates = []
        raster_files = []  # Store downloaded raster file paths
        
        self.log_message(f"  Downloading 12 months for {year}...")
        self.log_message(f"  Rasters will be saved to: {self.config.get_raster_year_dir(region_name, year)}")
        
        # Download all monthly rasters first
        for month in tqdm(range(1, 13), desc=f"Downloading {year}"):
            try:
                # Create date range for the month
                month_start = f"{year}-{month:02d}-01"
                month_end = (datetime(year, month, 1) + timedelta(days=31)).strftime('%Y-%m-%d')
                
                # Get GPM data for the month
                gpm_data = ee.ImageCollection(self.config.DATASET) \
                    .filterDate(month_start, month_end) \
                    .filterBounds(geometry) \
                    .select(self.config.PRECIP_BAND) \
                    .first()
                
                if not gpm_data:
                    self.log_message(f"  ⚠️ No data for {year}-{month:02d}", "WARNING")
                    monthly_data.append(None)
                    month_dates.append(datetime(year, month, 15))
                    continue
                
                # Convert to monthly accumulated
                converted = self.convert_to_monthly_accumulated(gpm_data)
                clipped = converted.clip(geometry)
                
                # Download raster to organized folder
                raster_path = self.download_raster(clipped, geometry, region_name, year, month)
                
                if raster_path:
                    raster_files.append(raster_path)
                    
                    # Read raster data
                    with rasterio.open(raster_path) as src:
                        data = src.read(1)  # Read first band
                        
                        # Store for NetCDF creation
                        monthly_data.append(data)
                        month_dates.append(datetime(year, month, 15))
                        
                        self.log_message(f"  ✅ Downloaded {year}-{month:02d}: shape={data.shape}", "DEBUG")
                else:
                    monthly_data.append(None)
                    month_dates.append(datetime(year, month, 15))
                
                # Small delay
                time.sleep(1)
                
            except Exception as e:
                self.log_message(f"  ❌ Error downloading {year}-{month:02d}: {e}", "ERROR")
                monthly_data.append(None)
                month_dates.append(datetime(year, month, 15))
                continue
        
        # Create NetCDF from all monthly data
        netcdf_file = self._create_12band_netcdf(year, region_name, monthly_data, month_dates, west, south, east, north)
        
        if netcdf_file:
            self.log_message(f"  💾 Raster files organized in: {self.config.get_raster_year_dir(region_name, year)}")
            self._create_raster_readme(region_name, year, raster_files)
        
        return netcdf_file
    
    def _create_12band_netcdf(self, year: int, region_name: str, 
                            monthly_data: List[np.ndarray], 
                            month_dates: List[datetime],
                            west: float, south: float, east: float, north: float):
        """Create NetCDF file with 12 bands (months)"""
        # Create NetCDF filename
        filename = f"gpm_{region_name.lower().replace(' ', '_')}_{year}.nc"
        filepath = os.path.join(self.config.NETCDF_DIR, filename)
        
        try:
            # Find first valid data array to get dimensions
            valid_data = [d for d in monthly_data if d is not None]
            if not valid_data:
                self.log_message(f"❌ No valid data for {region_name} {year}", "ERROR")
                return None
            
            # Get dimensions from first valid data
            first_valid = valid_data[0]
            n_rows, n_cols = first_valid.shape
            
            self.log_message(f"  Creating NetCDF with shape: (12, {n_rows}, {n_cols})")
            
            # Create lat/lon arrays
            lat = np.linspace(north, south, n_rows)  # North to south
            lon = np.linspace(west, east, n_cols)
            
            # Create 3D array with NaN for missing months
            data_3d = np.full((12, n_rows, n_cols), np.nan)
            
            for i, month_data in enumerate(monthly_data):
                if month_data is not None:
                    # Ensure dimensions match
                    if month_data.shape == (n_rows, n_cols):
                        data_3d[i] = month_data
                    else:
                        self.log_message(f"  ⚠️ Shape mismatch for month {i+1}: {month_data.shape} vs ({n_rows}, {n_cols})", "WARNING")
                        # Try to resize if scipy is available
                        try:
                            from scipy.ndimage import zoom
                            zoom_factors = (n_rows/month_data.shape[0], n_cols/month_data.shape[1])
                            resized = zoom(month_data, zoom_factors, order=1)
                            data_3d[i] = resized
                        except ImportError:
                            self.log_message(f"  ⚠️ Scipy not available for resizing", "WARNING")
                        except Exception as e:
                            self.log_message(f"  ⚠️ Resizing failed: {e}", "WARNING")
            
            # Create xarray Dataset with 12 bands
            ds = xr.Dataset(
                {
                    'precipitation': (['time', 'lat', 'lon'], data_3d)
                },
                coords={
                    'time': month_dates,
                    'lat': lat,
                    'lon': lon
                },
                attrs={
                    'title': f'GPM Monthly Precipitation - {region_name} {year}',
                    'description': 'Monthly accumulated precipitation (mm/month). 12 time bands represent January to December.',
                    'source': self.config.DATASET,
                    'units': 'mm/month',
                    'conversion': 'Converted from mm/hour to mm/month',
                    'region': region_name,
                    'year': str(year),
                    'scale_meters': self.config.SCALE,
                    'crs': 'EPSG:4326',
                    'bbox': f"{west},{south},{east},{north}",
                    'band_names': ['January', 'February', 'March', 'April', 'May', 'June', 
                                  'July', 'August', 'September', 'October', 'November', 'December'],
                    'created': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'raster_files_location': f"./raster/{region_name.lower().replace(' ', '_')}/{year}/"
                }
            )
            
            # Save to NetCDF
            ds.to_netcdf(filepath)
            
            # Validate the file
            with xr.open_dataset(filepath) as test:
                self.log_message(f"✅ NetCDF created: {filepath}")
                self.log_message(f"   Shape: {test.precipitation.shape}")
                self.log_message(f"   Time dimension: {len(test.time)} points")
                
                # Check first band (January)
                jan_data = test.precipitation[0].values
                valid_mask = ~np.isnan(jan_data)
                if np.any(valid_mask):
                    self.log_message(f"   Band 1 (January) range: {np.nanmin(jan_data):.1f} to {np.nanmax(jan_data):.1f}")
                else:
                    self.log_message(f"   Band 1 (January): All NaN values")
            
            return filepath
            
        except Exception as e:
            self.log_message(f"❌ Error creating NetCDF: {e}", "ERROR")
            import traceback
            self.log_message(traceback.format_exc(), "DEBUG")
            return None
    
    def _create_raster_readme(self, region_name: str, year: int, raster_files: List[str]):
        """Create a README file describing the raster files"""
        if not raster_files:
            return
        
        # Get the year directory
        year_dir = self.config.get_raster_year_dir(region_name, year)
        readme_file = os.path.join(year_dir, "README.txt")
        
        try:
            with open(readme_file, 'w') as f:
                f.write("="*60 + "\n")
                f.write(f"GPM RASTER FILES - {region_name.upper()} {year}\n")
                f.write("="*60 + "\n\n")
                
                f.write("📁 FILE ORGANIZATION:\n")
                f.write(f"   Region: {region_name}\n")
                f.write(f"   Year: {year}\n")
                f.write(f"   Total files: {len(raster_files)}\n")
                f.write(f"   Format: GeoTIFF (.tif)\n\n")
                
                f.write("📊 FILE LIST (Monthly):\n")
                for i, raster_file in enumerate(sorted(raster_files), 1):
                    filename = os.path.basename(raster_file)
                    month_num = filename.split('_')[-1].replace('.tif', '')
                    month_name = datetime(year, int(month_num), 1).strftime('%B')
                    f.write(f"   {i:2d}. {filename} - {month_name} {year}\n")
                
                f.write("\n📋 FILE INFORMATION:\n")
                f.write("   • Each file represents monthly accumulated precipitation\n")
                f.write("   • Units: mm/month (converted from mm/hour)\n")
                f.write("   • Coordinate system: EPSG:4326 (WGS84)\n")
                f.write("   • Resolution: ~0.25 degrees (~27.8 km)\n")
                f.write(f"   • Source: {self.config.DATASET}\n")
                f.write(f"   • Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("🔧 HOW TO USE:\n")
                f.write("   • Open with GIS software: QGIS, ArcGIS\n")
                f.write("   • Process with Python: rasterio, GDAL\n")
                f.write("   • Combine into NetCDF using provided pipeline\n")
                
            self.log_message(f"📄 Raster README created: {readme_file}")
            
        except Exception as e:
            self.log_message(f"⚠️ Error creating raster README: {e}", "WARNING")
    
    def export_annual_netcdf(self, years: List[int], region_name: str = "Africa"):
        """Export NetCDF files for specified years"""
        self.log_message(f"🚀 Exporting NetCDF files for {region_name}")
        self.log_message(f"   Years: {years}")
        self.log_message(f"   Output: {self.config.NETCDF_DIR}")
        self.log_message(f"   Rasters organized in: {self.config.RASTER_DIR}/{region_name.lower().replace(' ', '_')}/[YEAR]/")
        
        netcdf_files = []
        
        for year in years:
            try:
                self.log_message(f"Processing year {year}...")
                netcdf_file = self.create_proper_annual_netcdf(year, region_name)
                
                if netcdf_file:
                    netcdf_files.append(netcdf_file)
                    self.log_message(f"✅ Year {year} completed: {netcdf_file}")
                    
                    # Show raster organization
                    year_dir = self.config.get_raster_year_dir(region_name, year)
                    if os.path.exists(year_dir):
                        tif_files = [f for f in os.listdir(year_dir) if f.endswith('.tif')]
                        self.log_message(f"   📁 Rasters saved: {len(tif_files)} files in {year_dir}")
                else:
                    self.log_message(f"⚠️ No NetCDF created for {year}", "WARNING")
                
                # Delay between years
                time.sleep(2)
                
            except Exception as e:
                self.log_message(f"❌ Error processing year {year}: {e}", "ERROR")
                continue
        
        self.log_message(f"✅ Created {len(netcdf_files)} annual NetCDF files")
        
        return netcdf_files
    
    # ============================================================================
    # 7. CSV STATISTICS EXPORT
    # ============================================================================
    
    def calculate_monthly_statistics(self, region_name: str = "Africa"):
        """
        Calculate comprehensive monthly statistics
        Returns DataFrame with monthly stats
        """
        self.log_message(f"📊 Calculating monthly statistics for {region_name}")
        
        # Get geometry
        if region_name == "Africa":
            geometry = self.get_africa_geometry()
        else:
            geometry = self.get_country_geometry(region_name)
        
        if not geometry:
            self.log_message(f"❌ No geometry for {region_name}", "ERROR")
            return pd.DataFrame()
        
        # Process all years
        all_data = []
        
        for year in range(2020, 2025):
            for month in range(1, 13):
                try:
                    month_start = f"{year}-{month:02d}-01"
                    month_end = (datetime(year, month, 1) + timedelta(days=31)).strftime('%Y-%m-%d')
                    
                    # Get GPM data for the month
                    gpm_data = ee.ImageCollection(self.config.DATASET) \
                        .filterDate(month_start, month_end) \
                        .filterBounds(geometry) \
                        .select(self.config.PRECIP_BAND) \
                        .first()
                    
                    if not gpm_data:
                        self.log_message(f"  ⚠️ No data for {year}-{month:02d}", "WARNING")
                        all_data.append({
                            'region': region_name,
                            'year': year,
                            'month': month,
                            'date': f"{year}-{month:02d}-01",
                            'mean_precip_mm': np.nan,
                            'max_precip_mm': np.nan,
                            'min_precip_mm': np.nan,
                            'median_precip_mm': np.nan,
                            'std_precip_mm': np.nan,
                            'pixel_count': 0,
                            'area_km2': geometry.area().divide(1e6).getInfo(),
                            'total_volume_km3': 0
                        })
                        continue
                    
                    # Convert to monthly accumulated
                    converted = self.convert_to_monthly_accumulated(gpm_data)
                    
                    # Calculate statistics
                    stats = converted.reduceRegion(
                        reducer=ee.Reducer.mean() \
                            .combine(ee.Reducer.max(), sharedInputs=True) \
                            .combine(ee.Reducer.min(), sharedInputs=True) \
                            .combine(ee.Reducer.median(), sharedInputs=True) \
                            .combine(ee.Reducer.stdDev(), sharedInputs=True) \
                            .combine(ee.Reducer.count(), sharedInputs=True),
                        geometry=geometry,
                        scale=self.config.SCALE,
                        maxPixels=self.config.MAX_PIXELS,
                        bestEffort=True
                    ).getInfo()
                    
                    # Extract values
                    mean_precip = stats.get('precipitation_mm_mean', np.nan)
                    max_precip = stats.get('precipitation_mm_max', np.nan)
                    min_precip = stats.get('precipitation_mm_min', np.nan)
                    median_precip = stats.get('precipitation_mm_median', np.nan)
                    std_precip = stats.get('precipitation_mm_stdDev', np.nan)
                    pixel_count = stats.get('precipitation_mm_count', 0)
                    
                    # Calculate area and volume
                    area_km2 = geometry.area().divide(1e6).getInfo()
                    pixel_area_km2 = (self.config.SCALE * self.config.SCALE) / 1e6
                    total_volume_km3 = (mean_precip * pixel_count * pixel_area_km2) / 1e9
                    
                    all_data.append({
                        'region': region_name,
                        'year': year,
                        'month': month,
                        'date': f"{year}-{month:02d}-01",
                        'mean_precip_mm': mean_precip,
                        'max_precip_mm': max_precip,
                        'min_precip_mm': min_precip,
                        'median_precip_mm': median_precip,
                        'std_precip_mm': std_precip,
                        'pixel_count': pixel_count,
                        'area_km2': area_km2,
                        'total_volume_km3': total_volume_km3
                    })
                    
                    self.log_message(f"  ✅ Processed {year}-{month:02d}: {mean_precip:.1f} mm", "DEBUG")
                    
                    # Small delay
                    time.sleep(0.1)
                    
                except Exception as e:
                    self.log_message(f"  ❌ Error processing {year}-{month:02d}: {e}", "ERROR")
                    all_data.append({
                        'region': region_name,
                        'year': year,
                        'month': month,
                        'date': f"{year}-{month:02d}-01",
                        'mean_precip_mm': np.nan,
                        'max_precip_mm': np.nan,
                        'min_precip_mm': np.nan,
                        'median_precip_mm': np.nan,
                        'std_precip_mm': np.nan,
                        'pixel_count': 0,
                        'area_km2': geometry.area().divide(1e6).getInfo() if 'geometry' in locals() else 0,
                        'total_volume_km3': 0
                    })
                    continue
        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        
        if not df.empty:
            # Sort by date
            df = df.sort_values(['year', 'month']).reset_index(drop=True)
            
            # Create month name column
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            df['month_name'] = df['month'].apply(lambda x: month_names[x-1])
            
            # Calculate classification columns
            df['is_dry'] = df['mean_precip_mm'] < 50
            df['is_moderate'] = (df['mean_precip_mm'] >= 50) & (df['mean_precip_mm'] < 100)
            df['is_wet'] = (df['mean_precip_mm'] >= 100) & (df['mean_precip_mm'] < 200)
            df['is_very_wet'] = df['mean_precip_mm'] >= 200
            
            self.log_message(f"✅ Calculated statistics for {len(df)} months")
            
            # Calculate summary statistics
            valid_data = df['mean_precip_mm'].dropna()
            if len(valid_data) > 0:
                self.log_message(f"📊 Summary for {region_name}:")
                self.log_message(f"   Mean: {valid_data.mean():.1f} mm/month")
                self.log_message(f"   Max: {valid_data.max():.1f} mm/month")
                self.log_message(f"   Min: {valid_data.min():.1f} mm/month")
                self.log_message(f"   Std: {valid_data.std():.1f} mm/month")
        
        return df
    
    def export_statistics_csv(self, region_name: str = "Africa", df: pd.DataFrame = None):
        """Export statistics to CSV file"""
        if df is None:
            df = self.calculate_monthly_statistics(region_name)
        
        if df.empty:
            self.log_message(f"❌ No data to export for {region_name}", "WARNING")
            return None
        
        # Create filename
        filename = f"gpm_{region_name.lower().replace(' ', '_')}_statistics.csv"
        filepath = os.path.join(self.config.CSV_DIR, filename)
        
        # Export to CSV
        df.to_csv(filepath, index=False)
        
        self.log_message(f"✅ CSV exported: {filepath}")
        self.log_message(f"   Records: {len(df)}")
        self.log_message(f"   Period: {df['date'].min()} to {df['date'].max()}")
        
        # Create summary report
        self.create_summary_report(df, region_name)
        
        return filepath
    
    def create_summary_report(self, df: pd.DataFrame, region_name: str):
        """Create comprehensive summary report"""
        if df.empty:
            return
        
        summary_file = os.path.join(
            self.config.SUMMARY_DIR, 
            f"gpm_{region_name.lower().replace(' ', '_')}_summary.txt"
        )
        
        with open(summary_file, 'w') as f:
            f.write("="*60 + "\n")
            f.write(f"GPM PRECIPITATION SUMMARY - {region_name.upper()}\n")
            f.write("="*60 + "\n\n")
            
            # Basic info
            f.write("📅 PERIOD:\n")
            f.write(f"   {df['date'].min()} to {df['date'].max()}\n")
            f.write(f"   Total months: {len(df)}\n")
            f.write(f"   Valid months: {df['mean_precip_mm'].notna().sum()}\n\n")
            
            # Annual statistics
            annual_stats = df.groupby('year').agg({
                'mean_precip_mm': 'mean',
                'total_volume_km3': 'sum'
            }).round(1)
            
            f.write("📊 ANNUAL STATISTICS:\n")
            f.write("   Year | Avg Precip (mm) | Total Volume (km³)\n")
            f.write("   " + "-"*40 + "\n")
            for year, row in annual_stats.iterrows():
                f.write(f"   {year} | {row['mean_precip_mm']:14.1f} | {row['total_volume_km3']:16.3f}\n")
            f.write("\n")
            
            # Monthly climatology
            monthly_avg = df.groupby('month_name')['mean_precip_mm'].mean().reindex([
                'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
            ])
            
            f.write("📈 MONTHLY CLIMATOLOGY (mm/month):\n")
            for month, avg in monthly_avg.items():
                if not pd.isna(avg):
                    f.write(f"   {month}: {avg:.1f}\n")
            f.write("\n")
            
            # Precipitation classification
            dry_months = df['is_dry'].sum() if 'is_dry' in df.columns else 0
            moderate_months = df['is_moderate'].sum() if 'is_moderate' in df.columns else 0
            wet_months = df['is_wet'].sum() if 'is_wet' in df.columns else 0
            very_wet_months = df['is_very_wet'].sum() if 'is_very_wet' in df.columns else 0
            
            f.write("🌦️ PRECIPITATION CLASSIFICATION:\n")
            f.write(f"   Dry months (<50mm): {dry_months:.0f}\n")
            f.write(f"   Moderate months (50-100mm): {moderate_months:.0f}\n")
            f.write(f"   Wet months (100-200mm): {wet_months:.0f}\n")
            f.write(f"   Very wet months (>200mm): {very_wet_months:.0f}\n\n")
            
            # Data quality
            f.write("✅ DATA QUALITY:\n")
            f.write(f"   Missing values: {df['mean_precip_mm'].isna().sum()}\n")
            f.write(f"   Data source: {self.config.DATASET}\n")
            f.write(f"   Units: mm/month (converted from mm/hour)\n")
            f.write(f"   Resolution: {self.config.SCALE} meters\n")
            f.write(f"   Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"   Raster files organized in: ./raster/{region_name.lower().replace(' ', '_')}/[YEAR]/\n")
        
        self.log_message(f"✅ Summary report created: {summary_file}")
        
        return summary_file
    
    # ============================================================================
    # 8. MAIN PROCESSING METHODS
    # ============================================================================
    
    def process_complete(self, region_name: str = "Africa", 
                       years: List[int] = None,
                       export_csv: bool = True,
                       export_netcdf: bool = True):
        """Complete processing for a region"""
        if years is None:
            years = list(range(2020, 2025))
        
        self.log_message(f"🚀 Starting complete processing for: {region_name}")
        
        results = {
            'csv_file': None,
            'netcdf_files': [],
            'summary_file': None
        }
        
        # Export CSV statistics
        if export_csv:
            self.log_message(f"📊 Calculating and exporting statistics...")
            csv_file = self.export_statistics_csv(region_name)
            results['csv_file'] = csv_file
        
        # Export NetCDF files
        if export_netcdf:
            self.log_message(f"📁 Creating NetCDF files...")
            netcdf_files = self.export_annual_netcdf(years, region_name)
            results['netcdf_files'] = netcdf_files
        
        # Save results manifest
        manifest = {
            'region': region_name,
            'years_processed': years,
            'csv_file': results['csv_file'],
            'netcdf_files': results['netcdf_files'],
            'raster_organization': f"./raster/{region_name.lower().replace(' ', '_')}/[YEAR]/",
            'netcdf_folder': self.config.NETCDF_DIR,
            'csv_folder': self.config.CSV_DIR,
            'created': datetime.now().isoformat(),
            'data_source': self.config.DATASET
        }
        
        manifest_file = os.path.join(
            self.config.LOG_DIR,
            f"manifest_{region_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        self.log_message(f"✅ Complete processing finished for {region_name}")
        self.log_message(f"📄 Manifest saved: {manifest_file}")
        
        # Print organization summary
        self._print_organization_summary(region_name, years, results)
        
        return results
    
    def _print_organization_summary(self, region_name: str, years: List[int], results: dict):
        """Print summary of file organization"""
        print("\n" + "="*60)
        print("📁 FILE ORGANIZATION SUMMARY")
        print("="*60)
        
        print(f"\n🌍 Region: {region_name}")
        print(f"📅 Years: {years}")
        
        if results['csv_file']:
            print(f"\n📊 CSV Statistics:")
            print(f"   • {os.path.basename(results['csv_file'])}")
            print(f"   • Location: {os.path.dirname(results['csv_file'])}")
        
        if results['netcdf_files']:
            print(f"\n📁 NetCDF Files:")
            for netcdf_file in results['netcdf_files']:
                print(f"   • {os.path.basename(netcdf_file)}")
            print(f"   • Location: {self.config.NETCDF_DIR}")
        
        print(f"\n🗺️ Raster Files (GeoTIFF):")
        region_dir = region_name.lower().replace(' ', '_')
        for year in years:
            year_dir = os.path.join(self.config.RASTER_DIR, region_dir, str(year))
            if os.path.exists(year_dir):
                tif_files = [f for f in os.listdir(year_dir) if f.endswith('.tif')]
                if tif_files:
                    print(f"   • {year}: {len(tif_files)} files in raster/{region_dir}/{year}/")
                    print(f"     Example: {tif_files[0]}")
        
        print(f"\n📄 Documentation:")
        print(f"   • Logs: {self.config.LOG_DIR}/")
        print(f"   • Summaries: {self.config.SUMMARY_DIR}/")
        
        print("\n" + "="*60)

# ============================================================================
# 9. MAIN INTERFACE
# ============================================================================

def main():
    """Main interactive interface"""
    
    print("\n" + "="*60)
    print("🌧️ GPM V7 NETCDF PIPELINE - ORGANIZED RASTERS")
    print("="*60)
    print("Features:")
    print("  • Raster files organized in year-specific folders")
    print("  • Proper NetCDF files with 12 bands (months)")
    print("  • CSV statistics with comprehensive analysis")
    print("  • All files saved locally")
    print("  • Automatic README files for raster organization")
    print("="*60)
    
    if not initialize_ee():
        print("❌ Could not initialize Earth Engine")
        return
    
    # Create processor
    processor = GPMNetCDFProcessor()
    
    while True:
        print("\n📋 MAIN MENU:")
        print("1. Complete processing (CSV + NetCDF)")
        print("2. Export NetCDF files only")
        print("3. Export CSV statistics only")
        print("4. Test single year NetCDF")
        print("5. List raster organization")
        print("6. Exit")
        
        choice = input("\nEnter choice (1-6): ")
        
        if choice == '1':
            print("\n🌍 Complete processing...")
            
            region = input("Enter region name (or press Enter for Africa): ")
            if not region:
                region = "Africa"
            
            years = input("Enter years (comma-separated or press Enter for 2020-2024): ")
            if years:
                try:
                    years_list = [int(y.strip()) for y in years.split(',')]
                except:
                    years_list = list(range(2020, 2025))
            else:
                years_list = list(range(2020, 2025))
            
            print(f"\n📋 Processing Summary:")
            print(f"   Region: {region}")
            print(f"   Years: {years_list}")
            print(f"   Output organization:")
            print(f"     • CSV: {processor.config.CSV_DIR}/")
            print(f"     • NetCDF: {processor.config.NETCDF_DIR}/")
            print(f"     • Rasters: {processor.config.RASTER_DIR}/{region.lower().replace(' ', '_')}/[YEAR]/")
            
            confirm = input("\nContinue? (y/n): ")
            if confirm.lower() == 'y':
                processor.process_complete(region, years_list, export_csv=True, export_netcdf=True)
        
        elif choice == '2':
            print("\n📁 NetCDF export only...")
            
            region = input("Enter region name (or press Enter for Africa): ")
            if not region:
                region = "Africa"
            
            years = input("Enter years (comma-separated or press Enter for 2020-2024): ")
            if years:
                try:
                    years_list = [int(y.strip()) for y in years.split(',')]
                except:
                    years_list = list(range(2020, 2025))
            else:
                years_list = list(range(2020, 2025))
            
            print(f"\n📋 NetCDF Export:")
            print(f"   Region: {region}")
            print(f"   Years: {years_list}")
            print(f"   Output: {processor.config.NETCDF_DIR}")
            print(f"   Rasters organized in: {processor.config.RASTER_DIR}/{region.lower().replace(' ', '_')}/[YEAR]/")
            
            confirm = input("\nContinue? (y/n): ")
            if confirm.lower() == 'y':
                processor.process_complete(region, years_list, export_csv=False, export_netcdf=True)
        
        elif choice == '3':
            print("\n📊 CSV statistics only...")
            
            region = input("Enter region name (or press Enter for Africa): ")
            if not region:
                region = "Africa"
            
            print(f"\n📋 CSV Export:")
            print(f"   Region: {region}")
            print(f"   Output: {processor.config.CSV_DIR}")
            print(f"   Period: 2020-2024")
            
            confirm = input("\nContinue? (y/n): ")
            if confirm.lower() == 'y':
                processor.process_complete(region, export_csv=True, export_netcdf=False)
        
        elif choice == '4':
            print("\n🧪 Test single year NetCDF...")
            
            region = input("Enter region name (or press Enter for Kenya): ")
            if not region:
                region = "Kenya"
            
            year = input("Enter year (e.g., 2024): ")
            
            try:
                year_int = int(year)
                if year_int < 2020 or year_int > 2024:
                    print("❌ Year must be between 2020 and 2024")
                    continue
                
                print(f"\n🧪 Testing {region} {year_int}")
                print(f"   Output: {processor.config.NETCDF_DIR}")
                print(f"   Rasters: {processor.config.RASTER_DIR}/{region.lower().replace(' ', '_')}/{year_int}/")
                
                confirm = input("\nContinue? (y/n): ")
                if confirm.lower() == 'y':
                    netcdf_files = processor.export_annual_netcdf([year_int], region)
                    if netcdf_files:
                        print(f"✅ Created: {netcdf_files[0]}")
                        
                        # Test opening the file
                        try:
                            with xr.open_dataset(netcdf_files[0]) as ds:
                                print(f"📊 NetCDF info:")
                                print(f"   Dimensions: {dict(ds.dims)}")
                                print(f"   Variables: {list(ds.data_vars)}")
                                print(f"   Time points: {len(ds.time)} (should be 12)")
                                if 'precipitation' in ds:
                                    print(f"   Precipitation shape: {ds.precipitation.shape}")
                                    print(f"   Band names: {ds.attrs.get('band_names', 'Not found')}")
                        except Exception as e:
                            print(f"⚠️ Error reading NetCDF: {e}")
                
            except ValueError:
                print("❌ Invalid year")
        
        elif choice == '5':
            print("\n📁 Current raster organization:")
            print("="*40)
            
            if os.path.exists(processor.config.RASTER_DIR):
                region_dirs = [d for d in os.listdir(processor.config.RASTER_DIR) 
                             if os.path.isdir(os.path.join(processor.config.RASTER_DIR, d))]
                
                if region_dirs:
                    for region_dir in sorted(region_dirs):
                        region_path = os.path.join(processor.config.RASTER_DIR, region_dir)
                        print(f"\n🌍 {region_dir.replace('_', ' ').title()}:")
                        print(f"   Path: {region_path}")
                        
                        year_dirs = [d for d in os.listdir(region_path) 
                                   if os.path.isdir(os.path.join(region_path, d)) and d.isdigit()]
                        
                        if year_dirs:
                            for year_dir in sorted(year_dirs):
                                year_path = os.path.join(region_path, year_dir)
                                tif_files = [f for f in os.listdir(year_path) if f.endswith('.tif')]
                                if tif_files:
                                    print(f"   • {year_dir}: {len(tif_files)} GeoTIFF files")
                                else:
                                    print(f"   • {year_dir}: No GeoTIFF files")
                        else:
                            print(f"   No year directories found")
                else:
                    print("No region directories found in raster folder")
            else:
                print("Raster directory does not exist yet")
        
        elif choice == '6':
            print("\n👋 Exiting. Goodbye!")
            break
        
        else:
            print("❌ Invalid choice")

# ============================================================================
# 10. RUN THE PIPELINE
# ============================================================================

if __name__ == "__main__":
    print("🚀 GPM V7 NetCDF Pipeline - Organized Rasters")
    print("=" * 50)
    print("Features:")
    print("  • Raster files organized in year-specific folders")
    print("  • Proper NetCDF files with 12 bands (months)")
    print("  • CSV statistics with comprehensive analysis")
    print("  • Automatic README files for raster organization")
    print("  • All files saved locally")
    print()
    
    # Check for required packages
    try:
        import ee
        import pandas as pd
        import numpy as np
        from tqdm import tqdm
        import requests
        import rasterio
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        print("Install with: pip install earthengine-api pandas numpy tqdm xarray requests rasterio")
        exit(1)
    
    print("Starting pipeline...")
    main()