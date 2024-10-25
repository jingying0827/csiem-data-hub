import pandas as pd
import numpy as np
import os
import math

dir_lst = ["../../../../../data-lake/UWA/CWR/cwrctd"]

mapping_keys_df = pd.read_csv("mapping_keys.csv")
site_coordinates_lst = []

def process_data(dir):
    all_data = {}  # Dictionary to store data for each sitecode and variable
    for file in os.listdir(dir):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(dir, file), header=0)
            df['Date'] = pd.to_datetime(df['DATE'], format='%Y%m%d %H:%M:%S')
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df = df.dropna(how='all')
            df['SITECODE'] = df['SITECODE'].str.upper().str.replace("?", "").str.replace("/","").str.replace(".","dp")
            sitecode_lst = df['SITECODE'].unique().tolist()
            df['LOCATION'] = df['LOCATION'].str.lower()
            # Replace 'EPA COCKBURN S' with 'EPA COCKBURN' in the 'LOCATION' column
            df['LOCATION'] = df['LOCATION'].replace('epa cockburn s', 'epa cockburn', regex=False)
            site_coordinates_lst.append(df[['SITECODE','LOCATION','LAT (EPSG7844)','LON (EPSG7844)']].drop_duplicates())
            df = df[['Date','SITECODE','TEMPERATURE (C)','SALINITY (pss)','DENSITY (kgm-3)','CONDUCTIVITY (sm)','VELOCITY (ms-1)','DEPTH (m)','LOCATION']]
            
            var_lst = df.columns.to_list()
            variables = [var for var in var_lst if var not in ['Date', 'SITECODE', 'DEPTH (m)', 'LOCATION']]
            for sitecode in sitecode_lst:
                df_sitecode = df[df['SITECODE'] == sitecode]
                location = df_sitecode['LOCATION'].iloc[0]
                for variable in variables:
                    df_filtered = pd.DataFrame()

                    df_filtered['Date'] = df_sitecode['Date']
                    df_filtered['Data'] = df_sitecode[variable]
                    df_filtered['Variable'] = variable
                    df_filtered['Depth'] = df_sitecode['DEPTH (m)']
                    df_filtered['QC'] = 'N'

                    df_filtered.replace("", np.nan, inplace=True)

                    conv_factor = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Conv'].iloc[0]
                    if conv_factor != 1:
                        df_filtered['Data'] = pd.to_numeric(df_filtered['Data'], errors='coerce')
                        df_filtered['Data'] *= conv_factor

                    df_filtered = df_filtered.dropna(subset=['Data'])
                    # Drop rows where 'Data' is NaN using pd.isna()
                    df_filtered = df_filtered[~pd.isna(df_filtered['Data'])]

                    # Convert 'Data' to numeric, replacing non-numeric values with NaN
                    df_filtered['Data'] = pd.to_numeric(df_filtered['Data'], errors='coerce')
                    
                    # Remove rows where 'Data' is NaN
                    df_filtered = df_filtered.dropna(subset=['Data'])
                    
                    # Additional check: remove rows with infinite values
                    df_filtered = df_filtered[~np.isinf(df_filtered['Data'])]
                    
                    if variable.lower() == 'salinity (pss)':
                        df_filtered = df_filtered[df_filtered['Data'] <= 100000000000000]

                    df_filtered = df_filtered.loc[:, ["Variable", "Date", "Depth", "Data", "QC"]]
                    file_name = file.split('_EPSG7844')[0].split('CTD_')[1]
                    key = (file_name, location, sitecode, variable)
                    if key not in all_data:
                        all_data[key] = df_filtered
                    else:
                        all_data[key] = pd.concat([all_data[key], df_filtered], ignore_index=True)

    # Now process and save the concatenated data
    for (file_name, location, sitecode, variable), df_filtered in all_data.items():
        name_conv = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Key Value'].iloc[0]

        output_dir = f"../../../../../data-warehouse/csv/uwa/cwr/cwrctd"
        os.makedirs(output_dir, exist_ok=True)

        output_filename = f'{file_name}_{sitecode}_{name_conv.replace(" ","")}_profile_Data.csv'.replace("/","")
        print(output_dir)
        print(output_filename)
        df_filtered = df_filtered.sort_values(by=['Date', 'Depth'])
        df_filtered = df_filtered.drop_duplicates(keep='first')
        print(df_filtered)
        
        # Check if all 'Data' values are NaN
        if not df_filtered.empty and not df_filtered['Data'].isna().all():
            df_filtered.to_csv(os.path.join(output_dir, output_filename), index=False)
        else:
            print(f"Skipping {output_filename} as all 'Data' values are NaN")

    site_coordinates_df = pd.concat(site_coordinates_lst, ignore_index=True)
    site_coordinates_output_dir = "../../../../data-mapping/By Agency"
    os.makedirs(site_coordinates_output_dir, exist_ok=True)
    site_coordinates_df.to_csv(os.path.join(site_coordinates_output_dir, "UWA_CWR.csv"), index=False)

for dir in dir_lst:
    process_data(dir)