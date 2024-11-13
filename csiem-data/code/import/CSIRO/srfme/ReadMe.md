# Commonwealth Scientific and Industrial Research Organisation (CSIRO) Import

## Strategic Research Fund for the Marine Environment (SRFME) Two-Rock Moorings Data
> [!NOTE]
> This data is imported by the function importCSIROSRFME.py.

### Variable Conversion
| Variable ID | Variable Name | Conversion | Variable in ROMS |
| -------- | -------- | -------- | -------- |
| var00007 | Temperature | 1 | Temperature (0C) |
| var00006 | Salinity | 1 | Salinity (psu) |
| var00008 | Depth | 1 | Pressure (m) |
| var00211 | UCUR (eastward velocity) | 1 | Longshore-Current (m/s) |
| var00214 | VCUR (northward velocity) | 1 | Cross-Shore Current (m/s) |
| var00284 | Current Velocity | 1 | Current Speed (m/s) |

### Raw Data
    /GIS_DATA/csiem-data-hub/data-lake/CSIRO/SRFME/MOORING/Two Rocks Moorings Data 2004-2005

### Processed Data
    /GIS_DATA/csiem-data-hub/data-warehouse/csv/csiro/srfme/mooring

> [!IMPORTANT]
> Sensor depth for Mooring A and C (except Current) is calculated by subtracting the sensor depth from seabed from the raw pressure data. For Mooring B, the sensor depth is extracted directly from the sensor depth from water surface as no pressure data is available. For Current, the sensor depth is provided in the raw data files.
> 
> | ![Mooring A](Mooring_A.png) | ![Mooring B](Mooring_B.png) | ![Mooring C](Mooring_C.png) |
> |:---:|:---:|:---:|