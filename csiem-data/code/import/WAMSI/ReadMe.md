# Western Australian Marine Science Institution (WAMSI) Import

## WWMSP5 ROMS Data
This is data is imported by the function importWAMSIWWMSP5ROMS.py.

### Variable Conversion
| Variable ID | Variable Name | Conversion | Variable in ROMS |
| -------- | -------- | -------- | -------- |
| var00007 | Temperature | 1 | temp |
| var00006 | Salinity | 1 | salt |

### Raw Data
The raw data is stored in the data lake in the directory /GIS_DATA/csiem-data-hub/data-lake/WAMSI/WWMSP5/ROMS/.

### Data transformation
WQ data is imported directly from the data lake.