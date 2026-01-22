
- `YYYYMM` → Year and month (e.g., 200901)
- `MM` → Month number (padded, e.g., 01)
- `V07B` → Current sub-version

**Example**:  
`3B-MO.MS.MRG.3IMERG.20090101-S000000-E235959.01.V07B.HDF5`

## Dataset Structure & Variables (Bands)

All scientific data is stored in the `/Grid` group of the HDF5 file.  
Fields are 2D arrays: 1800 (lat) × 3600 (lon).

| Variable Name                      | Units                     | Description                                                                                     | Primary Use / Relevance                                      |
|------------------------------------|---------------------------|-------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| `precipitation`                    | mm/hr                     | Final gauge-adjusted monthly mean precipitation rate (core product), merged from multi-satellite estimates and calibrated with monthly gauge data (e.g., from GPCC). This is the bias-corrected, research-grade field formerly referred to as "precipitationCal" in some contexts. Values represent the average rate over the month.                          | Core input for hydrological models (e.g., runoff, infiltration); drought monitoring; ecosystem productivity assessment (e.g., correlating with vegetation indices like NDVI); soil erosion risk evaluation. Multiply by hours in the month for total accumulation if needed.           |
| `gaugeRelativeWeighting`           | % (0–100)                 | The relative contribution of gauge data versus satellite-only estimates in the final precipitation field. Higher values indicate stronger gauge influence, typically in regions with dense rain gauge networks (e.g., continents like North America or Europe).                                         | Assess regional data reliability i.e. Quality assurance: Identify regions where data is more reliable (high weighting) vs. satellite-dominant (low, e.g., oceans or remote areas). Useful for masking uncertain data in soil/ecosystem models.                             |
| `precipitationQualityIndex`        | Equivalent gauges / 2.5° box | Effective gauge density / quality indicator i.e. A quality metric representing the effective gauge support density for the precipitation estimate. Higher values signify better calibration and lower uncertainty.                                                   | Uncertainty filtering i.e. Uncertainty analysis: Filter or weight data in applications; critical for ecosystem health studies where precise precip inputs affect soil moisture simulations or vegetation stress predictions                                        |
| `probabilityLiquidPrecipitation`   | % (0–100)                 | Probability of liquid (rain) vs. frozen precipitation (ERA5-based) (snow/ice). Derived from ancillary surface temperature, humidity, and pressure data (ERA5 reanalysis in V07).                            | Phase discrimination for soil infiltration & snowmelt: Essential for land processes like soil absorption (liquid infiltrates better than frozen); ecosystem impacts in seasonal climates (e.g., snowmelt timing for plant growth).      |
| `randomError`                      | mm/hr                     | Estimated random uncertainty in precipitation field, based on error propagation from input sources.                                            | Error quantification: Propagate uncertainties in downstream models (e.g., hydrological forecasting for soil health); assess reliability for ecosystem vulnerability mapping.          |

**Quick notes**:
- `precipitation` is the primary variable most users work with
- Monthly total accumulation = `precipitation` × (days in month × 24)
- Missing value: typically -9999.9 (check file attributes)

## Automated Download Workflow

### Prerequisites

1. NASA Earthdata account → [https://urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov)
2. Authorize application: "NASA GESDISC DATA ARCHIVE"
3. Create `~/.netrc` file: