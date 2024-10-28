import pandas as pd
import os

# specify constants
PROJECT = "Cockburn Sound Model Virtual Sensor"
STATION_STATUS = "Active"
TIME_ZONE = "GMT +8"
VERT_DATUM = "mAHD"
DEPLOYMENT_POSITION = "m from surface"
VERT_REF = "Water Surface"
SITE_MEAN_DEPTH = ""
BAD_VALUE = 'NaN'
EMAIL = ""
SAMPLING_RATE = ""
DATE = "yyyy-mm-dd HH:MM:SS"
DEPTH = "Decimal"
QC = "NaN"

dir_lst = ["../../../../data-warehouse/csv/ESA",
           "../../../../data-warehouse/csv/MOI",
           "../../../../data-warehouse/csv/NASA"
           ]

mapping_keys_df = pd.read_csv("mapping_keys.csv")
site_coordinates_df = pd.read_csv("../../../data-mapping/By Agency/ESA_MOI_NASA.csv")

for dir_name in dir_lst:
    for root, dirs, files in os.walk(dir_name):
        for file in files:
            if file.endswith('.csv'):
                if "Data" in file:
                    print(file)
                    if "sentinel" in root and "point" in file.lower():
                        NATIONAL_STATION_ID = file.split("_Chlorophyll-a")[0].replace("_CHL","")
                    elif "globcolor" in root and "point" in file.lower():
                        NATIONAL_STATION_ID = "CMEMS_GlobColor_" + "_".join(file.split("_")[2:4])
                    elif "nemo" in root and "point" in file.lower():
                        NATIONAL_STATION_ID = file.split("_profile")[0]
                        NATIONAL_STATION_ID = "_".join(NATIONAL_STATION_ID.split("_")[:-1])
                    elif "pisces" in root and "point" in file.lower():
                        NATIONAL_STATION_ID = "CMEMS_PISCES_" + "_".join(file.replace("_profile_Data.csv","").split("_")[2:4])
                    elif "seapodym" in root and "point" in file.lower():
                        NATIONAL_STATION_ID = "CMEMS_SEAPODYM_" + "_".join(file.replace("_profile_Data.csv","").split("_")[3:5])
                    elif "nasa" not in root and "polygon" in file.lower():
                        NATIONAL_STATION_ID = "CMEMS_" + "_".join(file.replace("_profile_Data.csv","").split("_")[-3:-1])
                    elif "ghrsst" in root or "modis" in root:
                        NATIONAL_STATION_ID = "_".join(file.split("_")[:3])
                    print(NATIONAL_STATION_ID)
                    
                    if "point" in file.lower():
                        point_coordinates = {
                            "_1_": (-31.769, 115.649),
                            "_2_": (-31.852, 115.479),
                            "_3_": (-31.939, 115.685),
                            "_4_": (-31.896, 115.685),
                            "_5_": (-32.027, 115.367),
                            "_6_": (-32.1, 115.686),
                            "_7_": (-32.193, 115.351),
                            "_8_": (-32.188, 115.521),
                            "_9_": (-32.194, 115.731),
                            "_10_": (-32.355, 115.441),
                            "_11_": (-32.36, 115.604),
                            "_12_": (-32.444, 115.686),
                            "_13_": (-32.52, 115.601),
                            "_14_": (-32.1927, 115.7265),
                            "_15_": (-32.1811, 115.7697),
                            "_16_": (-32, 115.4),
                            "_17_": (-31.983, 115.228),
                            "_18_": (-32.251, 115.728),
                            "_19_": (-32.198, 115.72),
                            "_20_": (-32.152, 115.72),
                            "_21_": (-32.198, 115.766),
                            "_22_": (-32.18, 115.743),
                            "_23_": (-32.145, 115.75),
                            "_24_": (-31.8517, 115.6465),
                            "_25_": (-32.0684, 115.701),
                            "_26_": (-32.2604, 115.6239),
                            "_27_": (-32.2636, 115.5013),
                            "_28_": (-32.5431, 115.5996),
                            "_29_": (-32.1384, 115.7156),
                            "_30_": (-32.1384, 115.6559),
                            "_31_": (-32.1384, 115.5371),
                            "_32_": (-32.1384, 115.4464)
                        }
                        
                        for key, coords in point_coordinates.items():
                            if key in file:
                                LAT, LONG = coords
                                break
                    elif "polygon" in file.lower():
                        LAT = site_coordinates_df.loc[site_coordinates_df["site_id"] == NATIONAL_STATION_ID,"lat"].iloc[0]
                        LONG = site_coordinates_df.loc[site_coordinates_df["site_id"] == NATIONAL_STATION_ID,"lon"].iloc[0]

                    SITE_DESCRIPTION = site_coordinates_df.loc[site_coordinates_df["site_id"] == NATIONAL_STATION_ID,"site_name"].iloc[0]

                    NATIONAL_STATION_ID = f'{"-".join(root.split("/")[6:])}-{SITE_DESCRIPTION.replace(" ","_")}'  
                    
                    AGENCY_CODE = dir_name.split("/")[-1]
                    print(root)
                    PROGRAM = root.split("/")[7]

                    if PROGRAM == "globcolor":
                        PROGRAM = f'Global Ocean Colour ({PROGRAM.upper()})'
                    elif PROGRAM == "ghrsst":
                        PROGRAM = f'Group for High Resolution Sea Surface Temperature ({PROGRAM.upper()})'
                    elif PROGRAM == "nemo":
                        PROGRAM = f'Nucleus for European Modelling of the Ocean ({PROGRAM.upper()})'
                    elif PROGRAM == "pisces":
                        PROGRAM = f'Pelagic Interactions Scheme for Carbon and Ecosystem Studies ({PROGRAM.upper()})'
                    elif PROGRAM == "seapodym":
                        PROGRAM = f'Spatial Ecosystem and Population Dynamics Model ({PROGRAM.upper()})'
                    elif PROGRAM == "modis":
                        PROGRAM = f'Moderate Resolution Imaging Spectroradiometer ({PROGRAM.upper()})'
                    elif PROGRAM == "sentinel":
                        PROGRAM = f'Ocean and Land Colour Instrument (OLCI)'

                    if AGENCY_CODE == "ESA":
                        AGENCY_NAME = "European Space Agency"
                    elif AGENCY_CODE == "NASA":
                        AGENCY_NAME = "National Aeronautics and Space Administration"
                    elif AGENCY_CODE == "MOI":
                        AGENCY_NAME = "Mercator Ocean International"
                
                    TAG = "-".join(root.split("/")[6:]).replace("globcolor","GC").replace("optics","OPT").replace("plankton","PLK").replace("reflectance","REF").replace("transp","TRA").replace("sentinel","SEN").upper()

                    if "MODEL_SALINITY" in TAG:
                        TAG = TAG.replace("MODEL_SALINITY","SAL")
                    elif "MODEL_BIO" in TAG:
                        TAG = TAG.replace("MODEL_BIO","BIO")
                    elif "MODEL_CAR" in TAG:
                        TAG = TAG.replace("MODEL_CAR","CAR")
                    elif "MODEL_CO2" in TAG:
                        TAG = TAG.replace("MODEL_CO2","CO2")
                    elif "MODEL_NUT" in TAG:
                        TAG = TAG.replace("MODEL_NUT","NUT")
                    elif "MODEL_OPTICS" in TAG:
                        TAG = TAG.replace("MODEL_OPTICS","OPT")
                    elif "MODEL_PFT" in TAG:
                        TAG = TAG.replace("MODEL_PFT","PFT")
                    elif "MODEL_PP_ZO" in TAG:
                        TAG = TAG.replace("MODEL_PP_ZO","PPZO")

                    df = pd.read_csv(os.path.join(root, file), header=0)

                    if (df['Depth'] == 0).all():
                        DEPLOYMENT = "Floating"
                    else:
                        DEPLOYMENT = "Profile"
                    
                    var_idx = 4
                    if "sentinel" in root or "seapodym" in root:
                        var_idx = 5
                    elif "NASA" in root:
                        var_idx = 3
                    print(file.split("_")[var_idx])

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
