import pandas as pd
import numpy as np
import os

dir_lst = ["../../../../../data-lake/IMOS/SOOP/ALL"]

mapping_keys_df = pd.read_csv("mapping_keys.csv")

def process_data(dir):
    for file in os.listdir(dir):
        if file.endswith("hourly.xlsx"):
            print(file)
            # Read all sheets in the spreadsheet
            excel_file = pd.ExcelFile(os.path.join(dir, file))
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                grid_id = sheet_name
                # Process the data for each sheet
                print(f"Processing sheet: {grid_id}")
                df['Date'] = pd.to_datetime(df['TIME'])
                df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                df = df.drop('TIME', axis=1)  # Remove the TIME column
                # Move Date column to first position
                cols = df.columns.tolist()
                cols = ['Date'] + [col for col in cols if col != 'Date']
                df = df[cols]
                print(f"df:\n{df}")
                
                variable = file.split("_")[2]

                df_filtered = df[['Date', 'Avg']].rename(columns={'Avg': 'Data'})
                df_filtered['Variable'] = variable
                df_filtered['Depth'] = 0
                df_filtered['QC'] = 'Z'
                df_filtered = df_filtered.sort_values(by='Date')

                # Replace empty cells with NaN
                df_filtered.replace("", np.nan, inplace=True)

                df_filtered = df_filtered.loc[:, ["Variable", "Date", "Depth", "Data", "QC"]]

                # Convert value of different units
                conv_factor = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Conv'].iloc[0]
                if conv_factor != 1:
                    df_filtered['Data'] = pd.to_numeric(df_filtered['Data'], errors='coerce')  # Convert non-numeric values to NaN
                    df_filtered['Data'] *= conv_factor
                
                print(f"df_filtered:\n{df_filtered}")

                name_conv = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Key Value'].iloc[0]
                output_filename = f'SOOP_perth_{grid_id.split("_")[0]}{file.split("_")[3]}_{grid_id.split("_")[1]}_{name_conv.replace(" ","")}_Data.csv'

                print(output_filename)

                # Write the filtered DataFrame to a CSV file in the specified directory only if it's not empty
                output_dir = "../../../../../data-warehouse/csv/imos/soop"
                os.makedirs(output_dir, exist_ok=True)
                
                if not df_filtered.empty:
                    df_filtered.to_csv(os.path.join(output_dir, output_filename), index=False)

for dir in dir_lst:
    process_data(dir)