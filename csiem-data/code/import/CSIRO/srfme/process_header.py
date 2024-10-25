import pandas as pd
import os

# specify constants
PROJECT = "SRFME Two-Rock Moorings"
PROGRAM = "Strategic Research Fund for the Marine Environment (SRFME)"
AGENCY_CODE = "CSIRO"
AGENCY_NAME = "Commonwealth Scientific and Industrial Research Organisation"
STATION_STATUS = "Completed"
TIME_ZONE = "GMT +8"
VERT_DATUM = "mAHD"
DEPLOYMENT = "Fixed"
VERT_REF = "Water Surface"
BAD_VALUE = 'NaN'
EMAIL = ""
SAMPLING_RATE = "60 min"
DATE = "yyyy-mm-dd HH:MM:SS"
DEPTH = "Decimal"
QC = "NaN"

dir_lst = ['../../../../../data-warehouse/csv/csiro/srfme/mooring']

mapping_keys_df = pd.read_csv("mapping_keys.csv")

for dir_name in dir_lst:
    for root, dirs, files in os.walk(dir_name):
        for file in files:
            if file.endswith('.csv'):
                if "Data" in file:
                    NATIONAL_STATION_ID = f'Mooring_{file.split("_")[0]}'
                    SITE_DESCRIPTION = NATIONAL_STATION_ID.replace("_"," ")

                    if "CurrentVelocity" in file or "UCUR" in file or "VCUR" in file:
                        DEPLOYMENT_POSITION = f'{file.split("_")[2]} from surface (at {file.split("_")[4].replace("dp",".")})'
                        var_idx = 5
                    else:
                        DEPLOYMENT_POSITION = f'{file.split("_")[2]} from bottom'
                        var_idx = 3
                    
                    if NATIONAL_STATION_ID == "Mooring_A":
                        SITE_MEAN_DEPTH = "20m"
                    elif NATIONAL_STATION_ID == "Mooring_B":
                        SITE_MEAN_DEPTH = "40m"
                    elif NATIONAL_STATION_ID == "Mooring_C":
                        SITE_MEAN_DEPTH = "100m"
        
                    point_coordinates = {
                        "A_": (-31.5367, 115.5583),
                        "B_": (-31.6183, 115.365),
                        "C_": (-31.6817, 115.2217)
                    }
                    
                    for key, coords in point_coordinates.items():
                        if key in file:
                            LAT, LONG = coords
                            break
                        
                    TAG = AGENCY_CODE + "-" + PROGRAM.split("(")[1].strip(")") + "-" +  "Mooring"

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
                        "Variable ID": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[var_idx], "Key"].iloc[0],
                        "Data Category": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[var_idx], "Category"].iloc[0],
                        "Sampling Rate (min)": SAMPLING_RATE,
                        "Date": DATE,
                        "Depth": DEPTH,
                        "Variable": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[var_idx], "Key Value"].iloc[0],
                        "QC": QC
                    }
                    
                    output_filename = file.replace("Data","Header")

                    print(output_filename)
                    file_path = os.path.join(root, output_filename)

                    header_df = pd.DataFrame({"Header": header_dict.keys(), "Value": header_dict.values()})
                    # print(header_df)
                    header_df.to_csv(file_path, index=False)
