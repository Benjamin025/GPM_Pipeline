#!/usr/bin/env python3
"""
Extract GPM precipitation data from GeoTIFF files based on Area of Interest (AOI)
Generates a CSV file with monthly precipitation values from 1998-2025
"""

import os
import glob
from pathlib import Path
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping, shape
import geopandas as gpd
from datetime import datetime
import json


def load_aoi(aoi_path):
    """
    Load AOI from file (supports GeoJSON, Shapefile, or other vector formats)
    
    Parameters:
    -----------
    aoi_path : str
        Path to the AOI file
        
    Returns:
    --------
    geometry : shapely geometry
        The AOI geometry
    """
    if aoi_path.endswith('.geojson') or aoi_path.endswith('.json'):
        gdf = gpd.read_file(aoi_path)
    else:
        gdf = gpd.read_file(aoi_path)
    
    # If multiple features, union them into one
    if len(gdf) > 1:
        geometry = gdf.unary_union
    else:
        geometry = gdf.geometry.iloc[0]
    
    return geometry


def create_aoi_from_bounds(min_lon, min_lat, max_lon, max_lat):
    """
    Create AOI from bounding box coordinates
    
    Parameters:
    -----------
    min_lon, min_lat, max_lon, max_lat : float
        Bounding box coordinates in decimal degrees
        
    Returns:
    --------
    geometry : shapely geometry
        The AOI geometry as a box
    """
    from shapely.geometry import box
    return box(min_lon, min_lat, max_lon, max_lat)


def extract_precipitation(geotiff_path, aoi_geometry):
    """
    Extract mean precipitation value from GeoTIFF within AOI
    
    Parameters:
    -----------
    geotiff_path : str
        Path to the GeoTIFF file
    aoi_geometry : shapely geometry
        Area of interest geometry
        
    Returns:
    --------
    float : mean precipitation value within AOI
    """
    try:
        with rasterio.open(geotiff_path) as src:
            # Reproject AOI to match the raster CRS if needed
            if aoi_geometry.geom_type == 'Polygon':
                geom = [mapping(aoi_geometry)]
            else:
                geom = [mapping(aoi_geometry)]
            
            # Mask the raster with the AOI
            out_image, out_transform = mask(src, geom, crop=True, nodata=np.nan)
            
            # Calculate statistics
            # Remove nodata values
            valid_data = out_image[~np.isnan(out_image)]
            
            if len(valid_data) > 0:
                mean_precip = np.mean(valid_data)
                return mean_precip
            else:
                return np.nan
                
    except Exception as e:
        print(f"Error processing {geotiff_path}: {str(e)}")
        return np.nan


def process_gpm_data(base_dir, aoi, output_csv, start_year=1998, end_year=2025):
    """
    Process all GPM GeoTIFF files and extract precipitation data
    
    Parameters:
    -----------
    base_dir : str
        Base directory containing year folders with GeoTIFF files
    aoi : shapely geometry or str
        AOI geometry or path to AOI file
    output_csv : str
        Path to output CSV file
    start_year : int
        Starting year (default: 1998)
    end_year : int
        Ending year (default: 2025)
    """
    
    # Load AOI if it's a file path
    if isinstance(aoi, str):
        if os.path.exists(aoi):
            aoi_geometry = load_aoi(aoi)
        else:
            raise ValueError(f"AOI file not found: {aoi}")
    else:
        aoi_geometry = aoi
    
    # Store results
    results = []
    
    # Loop through years
    for year in range(start_year, end_year + 1):
        year_dir = os.path.join(base_dir, str(year))
        
        if not os.path.exists(year_dir):
            print(f"Warning: Directory not found for year {year}: {year_dir}")
            continue
        
        # Loop through months (1-12)
        for month in range(1, 13):
            # Construct filename pattern
            month_str = str(month).zfill(2)
            pattern = f"GPM_{year}_{month_str}_monthly_from_daily.tif"
            geotiff_path = os.path.join(year_dir, pattern)
            
            if os.path.exists(geotiff_path):
                print(f"Processing: {year}-{month_str}")
                
                # Extract precipitation
                precip = extract_precipitation(geotiff_path, aoi_geometry)
                
                # Store result
                results.append({
                    'year': year,
                    'month': month,
                    'date': f"{year}-{month_str}-01",
                    'precipitation_mm': precip
                })
            else:
                print(f"Warning: File not found: {geotiff_path}")
                results.append({
                    'year': year,
                    'month': month,
                    'date': f"{year}-{month_str}-01",
                    'precipitation_mm': np.nan
                })
    
    # Create DataFrame
    df = pd.DataFrame(results)
    
    # Add additional columns
    df['date'] = pd.to_datetime(df['date'])
    
    # Save to CSV
    df.to_csv(output_csv, index=False)
    print(f"\nData saved to: {output_csv}")
    print(f"Total records: {len(df)}")
    print(f"Records with valid data: {df['precipitation_mm'].notna().sum()}")
    
    return df


def main():
    """
    Main function - example usage
    """
    
    # Configuration
    base_dir = "/home/benjamin/Documents/Benjamin/GPM/GPM_gee/GPM_NASA_HalfHourly_1999_2026/geotiffs/2025" #"/home/benjamin/Documents/Benjamin/GPM/GPM_gee/GPM_NASA_Daily/geotiffs"
    
    # Option 1: Use AOI from file (uncomment and modify path)
    aoi = "/home/benjamin/Documents/Benjamin/GPM/GPM_gee/data/Dalalekutuk.geojson"
    
    # Option 2: Create AOI from bounding box coordinates (example for Nairobi area)
    # Modify these coordinates to match your area of interest
    #aoi = create_aoi_from_bounds(
        #min_lon=36.6,   # Western boundary
        #min_lat=-1.5,   # Southern boundary
        #max_lon=37.1,   # Eastern boundary
        #max_lat=-1.1    # Northern boundary
    #)
    
    # Output CSV path
    output_csv = "/home/benjamin/Documents/Benjamin/GPM/GPM_gee/data/Dalalekutuk_pcp_adm3_30.csv"
    
    # Process the data
    df = process_gpm_data(
        base_dir=base_dir,
        aoi=aoi,
        output_csv=output_csv,
        start_year=1998,
        end_year=2025
    )
    
    # Display summary statistics
    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    print(df['precipitation_mm'].describe())
    print("\nFirst few records:")
    print(df.head(10))
    print("\nLast few records:")
    print(df.tail(10))


if __name__ == "__main__":
    main()