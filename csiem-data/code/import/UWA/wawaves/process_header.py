import pandas as pd
import os

# specify constants
AGENCY_NAME = "The University of Western Australia"
AGENCY_CODE = "UWA"
PROGRAM = "Oceans Institute WA Waves"
PROJECT = "Wave buoy data"
TAG = "UWA-OI-WAWAVES"
STATION_STATUS = "Historic"
LAT = -31.8465995
LON = 115.642707
TIME_ZONE = "GMT +8"
VERT_DATUM = "mAHD"
SITE_DESCRIPTION = "Hillarys buoy"
DEPLOYMENT = "Floating"
DEPLOYMENT_POSITION = "m from surface"
VERT_REF = "Water Surface"
SITE_MEAN_DEPTH = ""
BAD_VALUE = 'NaN'
EMAIL = ""
SAMPLING_RATE = "60 min"
DATE = "yyyy-mm-dd HH:MM:SS"
DEPTH = "Decimal"
QC = "NaN"

dir_lst = ["../../../../../data-warehouse/csv/uwa/oi/wawaves"]

mapping_keys_df = pd.read_csv("mapping_keys.csv")

for dir_name in dir_lst:
    for root, dirs, files in os.walk(dir_name):
        for file in files:
            if file.endswith('.csv'):
                if "Data" in file:
                    NATIONAL_STATION_ID = f'uwa_hillarys_{file.split("_")[0]}'

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
                        "Lon": LON,
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
                        "Variable ID": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[1], "Key"].iloc[0],
                        "Data Category": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[1], "Category"].iloc[0],
                        "Sampling Rate (min)": SAMPLING_RATE,
                        "Date": DATE,
                        "Depth": DEPTH,
                        "Variable": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[1], "Key Value"].iloc[0],
                        "QC": QC
                    }
                    
                    output_filename = file.replace("Data","Header")

                    print(output_filename)
                    file_path = os.path.join(root, output_filename)

                    header_df = pd.DataFrame({"Header": header_dict.keys(), "Value": header_dict.values()})
                    # print(header_df)
                    header_df.to_csv(file_path, index=False)