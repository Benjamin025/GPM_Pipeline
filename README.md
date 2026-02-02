# 🌧️ GPM IMERG Monthly Rainfall Data Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![GPM IMERG](https://img.shields.io/badge/Data-GPM%20IMERG%20V07B-green.svg)](https://gpm.nasa.gov/data/imerg)

> **Automated workflow for downloading and processing GPM IMERG monthly rainfall data from NASA's Global Precipitation Measurement mission (1998-2025)**

This repository provides a complete pipeline for obtaining, processing, and analyzing monthly aggregated rainfall data from NASA's GPM (Global Precipitation Measurement) IMERG dataset. The workflow supports two aggregation approaches:

1. **Daily-to-Monthly**: Aggregating daily observations (1998-2025) to monthly totals
2. **30-Minute-to-Monthly**: Deriving monthly aggregates from high-temporal resolution 30-minute observations

---

## 📊 About GPM IMERG

The **Integrated Multi-satellitE Retrievals for GPM (IMERG)** algorithm combines data from multiple satellites and calibrates with ground-based rain gauge networks to produce high-quality, global precipitation estimates. The monthly product (3B-MO) provides research-grade, bias-corrected precipitation data ideal for:

- Hydrological modeling and water resource management
- Agricultural drought monitoring
- Ecosystem productivity assessment
- Climate variability analysis
- Soil erosion and land degradation studies

**Coverage**: Global (90°S - 90°N)  
**Spatial Resolution**: 0.1° × 0.1° (~11 km at equator)  
**Temporal Coverage**: June 2000 - Present (near real-time updates)  
**Version**: V07B (current sub-version)

---

## 📁 Dataset Overview

### File Naming Convention

GPM IMERG monthly files follow this standardized naming pattern:

```
3B-MO.MS.MRG.3IMERG.YYYYMMDD-SHHMMSS-EHHMMSS.MM.V07B.HDF5
```

**Components**:
- `3B-MO` → Monthly product
- `MS.MRG` → Merged satellite-gauge data
- `3IMERG` → IMERG algorithm version 3
- `YYYYMMDD` → Start date (e.g., 20090101)
- `SHHMMSS` → Start time (always 000000 for monthly)
- `EHHMMSS` → End time (always 235959 for monthly)
- `MM` → Month number (padded, e.g., 01)
- `V07B` → Current sub-version

**Example**:  
`3B-MO.MS.MRG.3IMERG.20090101-S000000-E235959.01.V07B.HDF5`

---

## 🗂️ Dataset Structure & Variables (Bands)

All scientific data is stored in the `/Grid` group of the HDF5 file.  
Fields are 2D arrays: **1800 (lat) × 3600 (lon)**

| Variable Name                      | Units                     | Description                                                                                     | Primary Use / Relevance                                      |
|------------------------------------|---------------------------|-------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| `precipitation`                    | mm/hr                     | Final gauge-adjusted monthly mean precipitation rate (core product), merged from multi-satellite estimates and calibrated with monthly gauge data (e.g., from GPCC). This is the bias-corrected, research-grade field formerly referred to as "precipitationCal" in some contexts. Values represent the average rate over the month.                          | Core input for hydrological models (e.g., runoff, infiltration); drought monitoring; ecosystem productivity assessment (e.g., correlating with vegetation indices like NDVI); soil erosion risk evaluation. Multiply by hours in the month for total accumulation if needed.           |
| `gaugeRelativeWeighting`           | % (0–100)                 | The relative contribution of gauge data versus satellite-only estimates in the final precipitation field. Higher values indicate stronger gauge influence, typically in regions with dense rain gauge networks (e.g., continents like North America or Europe).                                         | Assess regional data reliability i.e. Quality assurance: Identify regions where data is more reliable (high weighting) vs. satellite-dominant (low, e.g., oceans or remote areas). Useful for masking uncertain data in soil/ecosystem models.                             |
| `precipitationQualityIndex`        | Equivalent gauges / 2.5° box | Effective gauge density / quality indicator i.e. A quality metric representing the effective gauge support density for the precipitation estimate. Higher values signify better calibration and lower uncertainty.                                                   | Uncertainty filtering i.e. Uncertainty analysis: Filter or weight data in applications; critical for ecosystem health studies where precise precip inputs affect soil moisture simulations or vegetation stress predictions                                        |
| `probabilityLiquidPrecipitation`   | % (0–100)                 | Probability of liquid (rain) vs. frozen precipitation (ERA5-based) (snow/ice). Derived from ancillary surface temperature, humidity, and pressure data (ERA5 reanalysis in V07).                            | Phase discrimination for soil infiltration & snowmelt: Essential for land processes like soil absorption (liquid infiltrates better than frozen); ecosystem impacts in seasonal climates (e.g., snowmelt timing for plant growth).      |
| `randomError`                      | mm/hr                     | Estimated random uncertainty in precipitation field, based on error propagation from input sources.                                            | Error quantification: Propagate uncertainties in downstream models (e.g., hydrological forecasting for soil health); assess reliability for ecosystem vulnerability mapping.          |

### 📌 Quick Notes

- **`precipitation`** is the primary variable most users work with
- **Monthly total accumulation** = `precipitation` × (days in month × 24)
- **Missing value**: typically -9999.9 (check file attributes)
- **Data quality**: Highest over land with dense gauge networks, lower over oceans

---

## 🚀 Quick Start

### Prerequisites

Before running the pipeline, ensure you have:

1. **NASA Earthdata Account**  
   → Register at [https://urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov)

2. **Authorize GES DISC Application**  
   → Log in to Earthdata and approve "NASA GESDISC DATA ARCHIVE"
---

# Data source
data_source:
  base_url: "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGM.07/"
  version: "V07B"

## 📚 Resources & Documentation

### Official GPM Resources
- **GPM Mission**: [https://gpm.nasa.gov/](https://gpm.nasa.gov/)
- **IMERG Technical Documentation**: [https://gpm.nasa.gov/data/imerg](https://gpm.nasa.gov/data/imerg)
- **Algorithm Theoretical Basis Document (ATBD)**: [GPM IMERG ATBD](https://gpm.nasa.gov/sites/default/files/2020-05/IMERG_ATBD_V06.pdf)
- **GES DISC Data Portal**: [https://disc.gsfc.nasa.gov/](https://disc.gsfc.nasa.gov/)

### Tutorials & Guides
- [Working with GPM HDF5 Files](https://disc.gsfc.nasa.gov/information/howto)
- [IMERG Data Access Guide](https://gpm.nasa.gov/data/directory)
- [Python HDF5 Tutorial](https://docs.h5py.org/en/stable/)

### Citation
If you use GPM IMERG data in your research, please cite:

```
Huffman, G.J., E.F. Stocker, D.T. Bolvin, E.J. Nelkin, Jackson Tan (2023), 
GPM IMERG Final Precipitation L3 1 month 0.1 degree x 0.1 degree V07, 
Greenbelt, MD, Goddard Earth Sciences Data and Information Services Center (GES DISC), 
Accessed: [Data Access Date], doi:10.5067/GPM/IMERG/3B-MONTH/07
```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

Please ensure:
- Code follows PEP 8 style guidelines
- All tests pass
- Documentation is updated for new features

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🐛 Issues & Support

Encountering problems? Please check:

1. **Common Issues**: See [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)
2. **GitHub Issues**: [Report a bug](https://github.com/Benjamin025/GPM_Pipeline/issues)
3. **Discussions**: [Ask questions](https://github.com/Benjamin025/GPM_Pipeline/discussions)

---

## 🙏 Acknowledgments

- **NASA Global Precipitation Measurement (GPM) Mission** for providing open-access precipitation data
- **GES DISC** for data hosting and distribution infrastructure
- **IMERG Science Team** for algorithm development and validation

---

## 📧 Contact

**Benjamin Musembi**  
GitHub: [@Benjamin025](https://github.com/Benjamin025)

---

<p align="center">
  <i>⭐ If you find this repository useful, please consider giving it a star!</i>
</p>

<p align="center">
  <sub>Last Updated: February 2026</sub>
</p>
