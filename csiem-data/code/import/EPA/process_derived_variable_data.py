import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

data_folder = "../../../../data-warehouse/csv/epa/smcws"

# Dictionary to store data for each sitecode and measurement_code
data_dict = {}

for file in os.listdir(data_folder):
    if file.endswith("Data.csv") and any(param in file for param in ["TotalDissolvedNitrogen", "NitrateNitrogen", "Ammonium", "TotalDissolvedPhosphorus", "FilterableReactivePhosphate"]):
        df = pd.read_csv(os.path.join(data_folder, file))
        sitecode = file.split("_")[0]
        measurement_code = file.split("_")[1]
        variable = file.split("_")[2]
        
        # Initialize nested dictionary if not exists
        if (sitecode, measurement_code) not in data_dict:
            data_dict[(sitecode, measurement_code)] = {}
            
        # Store dataframe in dictionary
        data_dict[(sitecode, measurement_code)][variable] = df

# print(data_dict)

# Function to fill missing values using historical median for same month-day
def fill_with_historical_median(series, dates, month_days):
    filled_series = series.copy()
    for idx in filled_series[filled_series.isna()].index:
        current_md = month_days[idx]
        current_date = dates[idx]
        
        # Try exact month-day first
        historical_vals = series[month_days == current_md]
        
        if len(historical_vals.dropna()) > 0:
            filled_series[idx] = historical_vals.median()
        else:
            # If no exact month-day match, find closest dates within ±31 days
            current_doy = current_date.dayofyear
            date_diffs = abs(dates.dt.dayofyear - current_doy)
            nearby_vals = series[date_diffs <= 31]
            if len(nearby_vals.dropna()) > 0:
                filled_series[idx] = nearby_vals.median()
    
    return filled_series

# Calculate derived variables for each sitecode and measurement_code
for (sitecode, measurement_code), variables in data_dict.items():
    print(sitecode, measurement_code)
    if all(var in variables for var in ["TotalDissolvedNitrogen", "NitrateNitrogen", "Ammonium"]):
        # Create dataframes directly with final column names
        tdn = variables["TotalDissolvedNitrogen"][["Date", "Data"]].rename(columns={"Data": "TDN_Data"})
        no3 = variables["NitrateNitrogen"][["Date", "Data"]].rename(columns={"Data": "NO3_Data"})
        nh4 = variables["Ammonium"][["Date", "Data"]].rename(columns={"Data": "NH4_Data"})
        
        # Merge and sort in one chain
        don_df = (tdn.merge(no3, on='Date', how='outer')
                    .merge(nh4, on='Date', how='outer')
                    .sort_values('Date'))
        
        # Convert Date to datetime for proper processing
        don_df['Date'] = pd.to_datetime(don_df['Date'])
        
        # Add month-day column for grouping
        don_df['month_day'] = don_df['Date'].dt.strftime('%m-%d')
        
        # Apply historical median filling for each variable
        for col in ['TDN_Data', 'NO3_Data', 'NH4_Data']:
            don_df[col] = fill_with_historical_median(
                don_df[col], 
                don_df['Date'],
                don_df['month_day']
            )
            # Use linear interpolation for any remaining NaN values
            don_df[col] = don_df[col].interpolate(method='linear')
            don_df[col] = don_df[col].ffill().bfill()
            
        # Calculate DON
        don_df['DON'] = don_df['TDN_Data'] - don_df['NO3_Data'] - don_df['NH4_Data']

        don_df_export = don_df[["Date", "DON"]]
        don_df_export.columns = ["Date", "Data"]
        don_df_export['Date'] = pd.to_datetime(don_df_export['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        don_df_export['Depth'] = variables["TotalDissolvedNitrogen"]["Depth"].iloc[0]
        don_df_export['QC'] = 'N'
        don_df_export['Variable'] = 'Derived DON'
        don_df_export = don_df_export[["Variable", "Date", "Depth", "Data", "QC"]]
        print(don_df_export)
        
        don_df_export.to_csv(os.path.join(data_folder, f"{sitecode}_{measurement_code}_DissolvedOrganicNitrogen_profile_Data.csv"), index=False)
        
    if all(var in variables for var in ["TotalDissolvedPhosphorus", "FilterableReactivePhosphate"]):
        # Create dataframes directly with final column names
        tdp = variables["TotalDissolvedPhosphorus"][["Date", "Data"]].rename(columns={"Data": "TDP_Data"})
        frp = variables["FilterableReactivePhosphate"][["Date", "Data"]].rename(columns={"Data": "FRP_Data"})
        
        # Merge and sort in one chain
        dop_df = (tdp.merge(frp, on='Date', how='outer')
                    .sort_values('Date'))
        
        # Convert Date to datetime for proper processing
        dop_df['Date'] = pd.to_datetime(dop_df['Date'])
        
        # Add month-day column for grouping
        dop_df['month_day'] = dop_df['Date'].dt.strftime('%m-%d')
        
        # Apply historical median filling for each variable
        for col in ['TDP_Data', 'FRP_Data']:
            dop_df[col] = fill_with_historical_median(
                dop_df[col], 
                dop_df['Date'],
                dop_df['month_day']
            )
            dop_df[col] = dop_df[col].interpolate(method='linear')
            dop_df[col] = dop_df[col].ffill().bfill()
        
        # Calculate DOP
        dop_df['DOP'] = dop_df['TDP_Data'] - dop_df['FRP_Data']

        dop_df_export = dop_df[["Date", "DOP"]]
        dop_df_export.columns = ["Date", "Data"]
        dop_df_export['Date'] = pd.to_datetime(dop_df_export['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        dop_df_export['Depth'] = variables["TotalDissolvedPhosphorus"]["Depth"].iloc[0]
        dop_df_export['QC'] = 'N'
        dop_df_export['Variable'] = 'Derived DOP'
        dop_df_export = dop_df_export[["Variable", "Date", "Depth", "Data", "QC"]]
        print(dop_df_export)
        
        dop_df_export.to_csv(os.path.join(data_folder, f"{sitecode}_{measurement_code}_DissolvedOrganicPhosphorus_profile_Data.csv"), index=False)