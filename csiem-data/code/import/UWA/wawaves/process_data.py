import pandas as pd
import numpy as np
import os
import math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

dir_lst = ["../../../../../data-lake/UWA/OI/wawaves"]

mapping_keys_df = pd.read_csv("mapping_keys.csv")

def convert_timestamp_to_perth_time(timestamp):
    # Convert Unix timestamp to datetime object
    utc_time = pd.to_datetime(timestamp, unit='s', utc=True)
    
    # Convert to Perth time
    perth_tz = ZoneInfo("Australia/Perth")
    perth_time = utc_time.dt.tz_convert(perth_tz)
    
    # Format the Perth time
    formatted_perth = perth_time.dt.strftime("%d/%m/%Y %H:%M")
    
    return formatted_perth

def process_data(dir):
    for file in os.listdir(dir):
        if file.endswith(".csv") and "buoy-67-" in file:
            print(file)
            df = pd.read_csv(os.path.join(dir, file), header=0)
            df['Date'] = convert_timestamp_to_perth_time(df['Time (UNIX/UTC)'])
            
            df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y %H:%M', dayfirst=True)
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            # Drop columns if all rows are -9999
            df = df.loc[:, (df != -9999).any()]
            # Drop the specified columns
            df = df.drop(columns=['Time (UNIX/UTC)', 'Timestamp (UTC)'], errors='ignore')
            
            # Move 'Date' column to the first position
            cols = df.columns.tolist()
            cols.insert(0, cols.pop(cols.index('Date')))
            df = df[cols]
            # print(df)

            var_lst = df.columns.to_list()
            variables = [var for var in var_lst if var.strip() not in ['Date','Site','BuoyID','Latitude (deg)','Longitude (deg)','buoy_id','QF_waves','QF_sst','QF_bott_temp']]

            for variable in variables:
                df_filtered = pd.DataFrame()  # Initialize an empty DataFrame

                df_filtered['Date'] = df['Date']
                df_filtered['Data'] = df[variable]
                df_filtered['Variable'] = variable
                df_filtered['Depth'] = 0
                df_filtered['QC'] = 'N'

                df_filtered = df_filtered.sort_values(by='Date')

                # Replace empty cells with NaN
                df_filtered.replace("", np.nan, inplace=True)

                # Convert value of different units
                print(variable)
                conv_factor = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Conv'].iloc[0]
                if conv_factor != 1:
                    df_filtered['Data'] = pd.to_numeric(df_filtered['Data'], errors='coerce')  # Convert non-numeric values to NaN
                    df_filtered['Data'] *= conv_factor

                # Drop rows where Data is NaN
                df_filtered = df_filtered.dropna(subset=['Data'])

                df_filtered = df_filtered.loc[:, ["Variable", "Date", "Depth", "Data", "QC"]]

                print(df_filtered)

                name_conv = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Key Value'].iloc[0]

                output_dir = "../../../../../data-warehouse/csv/uwa/oi/wawaves"
                os.makedirs(output_dir, exist_ok=True)
                output_filename = f'{"".join(file.split("-")[0:2])}_{name_conv.replace(" ","")}_Data.csv'.replace("/","")
                print(output_dir)
                print(output_filename)

                if not df_filtered.empty:
                    df_filtered.to_csv(os.path.join(output_dir, output_filename), index=False)

for dir in dir_lst:
    process_data(dir)