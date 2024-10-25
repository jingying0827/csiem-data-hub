import pandas as pd
import os

# specify constants
AGENCY_NAME = "Integrated Marine Observing System"
AGENCY_CODE = "IMOS"
PROGRAM = "Ships of Opportunity (SOOP)"
PROJECT = "Perth waters gridded SOOP data"
STATION_STATUS = "Active"
TIME_ZONE = "GMT +8"
VERT_DATUM = "mAHD"
DEPLOYMENT = "Floating"
DEPLOYMENT_POSITION = "m from surface"
VERT_REF = "Water Surface"
SITE_MEAN_DEPTH = ""
BAD_VALUE = 'NaN'
EMAIL = ""
SAMPLING_RATE = ""
DATE = "yyyy-mm-dd HH:MM:SS"
DEPTH = "Decimal"
QC = "Z (value passes all tests)"

dir_lst = ["../../../../../data-warehouse/csv/imos/soop/perth"]

mapping_keys_df = pd.read_csv("mapping_keys.csv")
site_coordinates_df = pd.read_csv("../../../../data-mapping/By Agency/IMOS_SOOP.csv")

for dir in dir_lst:
    for file in os.listdir(dir):
        if file.endswith('_Data.csv'):
            print(f"Datafile: {file}")

            NATIONAL_STATION_ID = "_".join(file.split("_")[2:4])
            SITE_DESCRIPTION = NATIONAL_STATION_ID
                    
            LAT = round(site_coordinates_df.loc[site_coordinates_df["grid_id"] == NATIONAL_STATION_ID, "lat"].iloc[0], 4)
            LONG = round(site_coordinates_df.loc[site_coordinates_df["grid_id"] == NATIONAL_STATION_ID, "lon"].iloc[0], 4)
                    
            TAG = AGENCY_CODE + "-" + PROGRAM.split("(")[1].strip(")") + "-PERTH"

            header_dict = {
                "Agency Name": AGENCY_NAME,
                "Agency Code": AGENCY_CODE,
                "Program": PROGRAM,
                "Project": PROJECT,
                "Tag": TAG,
                "Data File Name": file,
                "Location": dir.rsplit("../",1)[-1],
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
                "Variable ID": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[4], "Key"].iloc[0],
                "Data Category": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[4], "Category"].iloc[0],
                "Sampling Rate (min)": SAMPLING_RATE,
                "Date": DATE,
                "Depth": DEPTH,
                "Variable": mapping_keys_df.loc[mapping_keys_df["Key Value"].str.replace(" ","") == file.split("_")[4], "Key Value"].iloc[0],
                "QC": QC
            }
            
            output_filename = file.replace("Data","Header")

            print(output_filename)
            file_path = os.path.join(dir, output_filename)

            header_df = pd.DataFrame({"Header": header_dict.keys(), "Value": header_dict.values()})
            # print(header_df)
            header_df.to_csv(file_path, index=False)
