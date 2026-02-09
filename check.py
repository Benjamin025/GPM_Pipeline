"""
GPM IMERG Processing: Daily vs Monthly Data
Explanation of correct unit handling
"""

import numpy as np

# =============================================================================
# UNDERSTANDING GPM IMERG PRODUCTS
# =============================================================================

"""
GPM IMERG has different temporal products:

1. HALF-HOURLY (3IMERGHH): mm/hr
2. DAILY (3IMERGD): mm/hr (averaged over 24 hours)
3. MONTHLY (3IMERGM): mm/month (ALREADY AGGREGATED!)

Your code is downloading: 3B-MO.MS.MRG.3IMERG... 
The "MO" means MONTHLY product
"""

# =============================================================================
# YOUR CURRENT CODE (INCORRECT FOR MONTHLY DATA)
# =============================================================================

def your_current_processing(precip_data_from_hdf5, year, month):
    """
    This is what your code currently does
    """
    import calendar
    
    # Calculate hours in month
    days_in_month = calendar.monthrange(year, month)[1]
    hours_in_month = days_in_month * 24
    
    # Remove fill values
    fill_value = -9999.9
    precip_masked = np.where(precip_data_from_hdf5 == fill_value, np.nan, precip_data_from_hdf5)
    
    # Convert to mm/month by multiplying by hours
    precip_monthly = precip_masked * hours_in_month
    
    return precip_monthly

# Example with your current code:
# If HDF5 contains: 100 mm/month
# Your output: 100 * 720 = 72,000 mm/month ❌ WRONG!

# =============================================================================
# CORRECT PROCESSING FOR MONTHLY DATA
# =============================================================================

def correct_processing_for_monthly(precip_data_from_hdf5):
    """
    Correct processing for 3IMERGM (monthly) data
    """
    # Remove fill values
    fill_value = -9999.9
    precip_monthly = np.where(precip_data_from_hdf5 == fill_value, np.nan, precip_data_from_hdf5)
    
    # NO MULTIPLICATION NEEDED - data is already in mm/month!
    
    return precip_monthly

# Example with correct code:
# If HDF5 contains: 100 mm/month
# Your output: 100 mm/month ✅ CORRECT!

# =============================================================================
# IF YOU WERE PROCESSING DAILY DATA (3IMERGD)
# =============================================================================

def correct_processing_for_daily(precip_data_from_hdf5_daily):
    """
    This is what you'd do if processing DAILY data to monthly
    """
    # Daily data is in mm/hr (instantaneous rate)
    # To get daily mm, multiply by 24
    # To get monthly mm, sum all days in the month
    
    # Remove fill values
    fill_value = -9999.9
    precip_masked = np.where(precip_data_from_hdf5_daily == fill_value, np.nan, precip_data_from_hdf5_daily)
    
    # Convert mm/hr to mm/day
    precip_daily_mm = precip_masked * 24
    
    # If you have multiple days, sum them:
    # precip_monthly = np.sum(precip_daily_mm_for_all_days, axis=0)
    
    return precip_daily_mm

# =============================================================================
# HOW TO VERIFY YOUR DATA
# =============================================================================

def verify_your_data():
    """
    Steps to verify if your data is correct
    """
    print("VERIFICATION STEPS:")
    print("="*70)
    
    print("\n1. Check HDF5 metadata:")
    print("   Open one of your HDF5 files and check the units attribute")
    print("   Look for: 'mm/month' or 'mm/hr'")
    
    print("\n2. Check reasonable values:")
    print("   Expected monthly precipitation ranges:")
    print("   - Arid regions: 0-50 mm/month")
    print("   - Moderate: 50-200 mm/month")
    print("   - Tropical: 200-500 mm/month")
    print("   - Extreme wet: 500-1000+ mm/month")
    print("   - If you see values like 50,000 mm/month → WRONG!")
    
    print("\n3. Compare with GEE:")
    print("   GEE uses daily data summed to monthly")
    print("   If your monthly files are correct, they should match GEE")
    print("   If off by ~720x, you're multiplying when you shouldn't")
    
    print("\n4. Quick sanity check:")
    print("   Nairobi average annual rainfall: ~900 mm/year")
    print("   Average monthly: ~75 mm/month")
    print("   Wet months (April): ~200 mm/month")
    print("   If you see 50,000 mm/month → multiplied by 720 incorrectly!")

# =============================================================================
# CORRECTED VERSION OF YOUR FUNCTION
# =============================================================================

def process_single_file_CORRECTED(hdf5_path, create_preview=True):
    """
    Corrected version of your process_single_file function
    """
    import h5py
    from pathlib import Path
    import re
    import calendar
    
    hdf5_path = Path(hdf5_path)
    print(f"\n🔄 Processing: {hdf5_path.name}")
    
    # Extract year and month
    match = re.search(r'(\d{4})(\d{2})', hdf5_path.name)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
    else:
        year = int(hdf5_path.parent.name)
        month = 1
    
    try:
        with h5py.File(hdf5_path, 'r') as h5f:
            grid = h5f['Grid']
            
            # Extract data
            precip = grid['precipitation'][:]  # Shape: (1, 3600, 1800)
            lat = grid['lat'][:]
            lon = grid['lon'][:]
            
            print(f"  Data shape: {precip.shape}")
            
            # Remove time dimension
            precip_2d = precip[0, :, :]
            
            # Handle fill values
            fill_value = -9999.9
            precip_masked = np.where(precip_2d == fill_value, np.nan, precip_2d)
            
            # ===========================================================
            # KEY CHANGE: NO MULTIPLICATION FOR MONTHLY DATA!
            # ===========================================================
            # The 3IMERGM product is ALREADY in mm/month
            # We do NOT multiply by hours_in_month
            
            precip_monthly = precip_masked  # Already in mm/month!
            
            # ===========================================================
            
            # Check if values are reasonable
            mean_precip = np.nanmean(precip_monthly)
            max_precip = np.nanmax(precip_monthly)
            
            print(f"  Mean precipitation: {mean_precip:.2f} mm/month")
            print(f"  Max precipitation: {max_precip:.2f} mm/month")
            
            # Sanity check
            if max_precip > 5000:
                print("  ⚠️ WARNING: Unusually high values detected!")
                print("  ⚠️ This might indicate incorrect unit conversion")
            
            # Continue with transpose and export...
            data_for_export = precip_monthly.T
            
            if lat[0] > lat[-1]:
                data_for_export = np.flipud(data_for_export)
                lat = np.flip(lat)
            
            return data_for_export, lat, lon, year, month
            
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None

# =============================================================================
# DIAGNOSTIC SCRIPT TO CHECK YOUR EXISTING FILES
# =============================================================================

def diagnose_existing_geotiff(geotiff_path):
    """
    Check if your existing GeoTIFF has reasonable values
    """
    import rasterio
    
    print(f"\n🔍 DIAGNOSING: {geotiff_path}")
    print("="*70)
    
    with rasterio.open(geotiff_path) as src:
        data = src.read(1)
        
        # Mask nodata
        nodata = src.nodata
        if nodata is not None:
            data_valid = data[data != nodata]
        else:
            data_valid = data[~np.isnan(data)]
        
        stats = {
            'min': np.min(data_valid),
            'max': np.max(data_valid),
            'mean': np.mean(data_valid),
            'median': np.median(data_valid),
            'p95': np.percentile(data_valid, 95)
        }
        
        print("\nStatistics (mm/month):")
        print(f"  Min:    {stats['min']:.2f}")
        print(f"  Max:    {stats['max']:.2f}")
        print(f"  Mean:   {stats['mean']:.2f}")
        print(f"  Median: {stats['median']:.2f}")
        print(f"  95th %: {stats['p95']:.2f}")
        
        print("\n🔍 ASSESSMENT:")
        if stats['max'] > 5000:
            print("  ❌ VALUES TOO HIGH - likely multiplied by 720 incorrectly")
            print("  Expected range: 0-1000 mm/month (rarely >2000)")
            print(f"  Your max: {stats['max']:.0f} mm/month")
            print(f"  Suggested correction: Divide by ~720")
        elif stats['max'] > 2000:
            print("  ⚠️ Some high values - check if region has extreme rainfall")
            print("  Could be correct for tropical regions, but verify")
        elif stats['mean'] < 1:
            print("  ❌ VALUES TOO LOW - check if units are wrong")
        else:
            print("  ✅ Values appear reasonable for monthly precipitation")
            print(f"  Mean: {stats['mean']:.1f} mm/month is plausible")
        
        return stats

# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    print("="*70)
    print("GPM PROCESSING VERIFICATION")
    print("="*70)
    
    verify_your_data()
    
    print("\n" + "="*70)
    print("TO CHECK YOUR EXISTING FILES:")
    print("="*70)
    print("\n1. Run this diagnostic on one of your GeoTIFFs:")
    print("   diagnose_existing_geotiff('/path/to/your/GPM_2024_01_monthly.tif')")
    print("\n2. If max values > 5000 mm/month:")
    print("   → Your files are incorrectly multiplied by ~720")
    print("   → Need to reprocess without the multiplication")
    print("\n3. If values look reasonable (mean ~50-200 mm/month):")
    print("   → Your processing might actually be correct")
    print("   → But verify with GEE comparison to be sure")