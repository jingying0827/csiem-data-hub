import pandas as pd
import os

# specify constants
AGENCY_NAME = "Department of Environmental Protection"
AGENCY_CODE = "EPA"
PROGRAM = "Cockburn Sound Monitoring Program"
PROJECT = "Southern Metropolitan Coastal Waters Study (SMCWS)" 
STATION_STATUS = "Completed"
TIME_ZONE = "GMT +8"
VERT_DATUM = "mAHD"
DEPLOYMENT = "Profile"
DEPLOYMENT_POSITION = "m from bottom"
VERT_REF = "Water Surface"
SITE_MEAN_DEPTH = ""
BAD_VALUE = 'NaN'
EMAIL = ""
SAMPLING_RATE = ""
DATE = "yyyy-mm-dd HH:MM:SS"
DEPTH = "Decimal"
QC = "NaN"

dir_lst = ["../../../../data-warehouse/csv/epa/smcws"]

mapping_keys_df = pd.read_csv("mapping_keys.csv")
site_coordinates_df = pd.read_csv("../../../data-mapping/By Agency/EPA_SMCWS.csv")

for dir_name in dir_lst:
    for root, dirs, files in os.walk(dir_name):
        for file in files:
            if file.endswith('.csv'):
                if "Data" in file:
                    NATIONAL_STATION_ID = file.split("_")[0]
                    if "CS" in NATIONAL_STATION_ID:
                        SITE_DESCRIPTION = f'Cockburn Sound Site {NATIONAL_STATION_ID[2]} {file.split("_")[1]}'
                    elif "WS" in NATIONAL_STATION_ID:
                        SITE_DESCRIPTION = f'Warnbro Sound Site {NATIONAL_STATION_ID[2]} {file.split("_")[1]}'
                    elif "SD" in NATIONAL_STATION_ID:
                        SITE_DESCRIPTION = f'Sepia Depression Site {NATIONAL_STATION_ID[2]} {file.split("_")[1]}'
                    elif "MS" in NATIONAL_STATION_ID:
                        SITE_DESCRIPTION = f"Mid-Shelf (up to about 10km offshore of Cape Peron and Warnbro Sound) Site {NATIONAL_STATION_ID[2]} {file.split('_')[1]}"
                    SITE_DESCRIPTION = SITE_DESCRIPTION.replace(" B"," (bottom)").replace(" M"," (mid-depth)").replace(" T"," (surface)")
                    print(file)
                    print(NATIONAL_STATION_ID)

                    LAT = site_coordinates_df.loc[site_coordinates_df["SiteCode"] == NATIONAL_STATION_ID,"Latitude"].iloc[0]
                    LONG = site_coordinates_df.loc[site_coordinates_df["SiteCode"] == NATIONAL_STATION_ID,"Longitude"].iloc[0]
                
                    TAG = AGENCY_CODE + "-" + PROJECT.split("(")[1].strip(")") + "-" + NATIONAL_STATION_ID[:2]

                    header_dict = {
                        "Agency Name": AGENCY_NAME,
                        "Agency Code": AGENCY_CODE,
                        "Program": PROGRAM,
                        "Project": PROJECT,
                        "Tag": TAG,
                        "Data File Name": file,
                        "Location": root.rsplit("../",1)[-1],
                        "Station Status": STATION_STATUS,
                        "Lat": LAT,
                        "Long": LONG,
                        "Time Zone": TIME_ZONE,
                        "Vertical Datum": VERT_DATUM,
                        "National Station ID": NATIONAL_STATION_ID,
                        "Site Description": SITE_DESCRIPTION,
                        "Deployment": DEPLOYMENT,
                        "Deployment Position": DEPLOYMENT_POSITION,
                        "Vertical Reference": VERT_REF,
                        "Site Mean Depth": SITE_MEAN_DEPTH,
                        "Bad or Unavailable Data Value": BAD_VALUE,
                        "Contact Email": EMAIL,
                        "Variable ID": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[-3], "Key"].iloc[0],
                        "Data Category": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[-3], "Category"].iloc[0],
                        "Sampling Rate (min)": SAMPLING_RATE,
                        "Date": DATE,
                        "Depth": DEPTH,
                        "Variable": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[-3], "Key Value"].iloc[0],
                        "QC": QC
                    }
                    
                    output_filename = file.replace("Data","Header")

                    print(output_filename)
                    file_path = os.path.join(root, output_filename)

                    header_df = pd.DataFrame({"Header": header_dict.keys(), "Value": header_dict.values()})
                    # print(header_df)
                    header_df.to_csv(file_path, index=False)
