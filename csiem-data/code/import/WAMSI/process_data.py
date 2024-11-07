import pandas as pd
import os
import numpy as np

dir_lst = [
    "../../../../data-lake/WAMSI/WWMSP5/ROMS/Perth_ROMS_0.5km_2023",
    "../../../../data-lake/WAMSI/WWMSP5/ROMS/WA_ROMS_2km_2000-2022"
]

mapping_keys_df = pd.read_csv("mapping_keys.csv")

def process_data(dir, output_dir):
    for file in os.listdir(dir):
        if file.endswith(".csv"):
            print(file)
            df = pd.read_csv(os.path.join(dir, file), header=0)
            df['Date'] = pd.to_datetime(df['ocean_time'], format='%Y-%m-%d %H:%M:%S')

            for variable in ['temp','salt']:
                df_filtered = df[['Date', variable,'s_rho']]
                df_filtered = df_filtered.rename(columns={variable: 'Data', 's_rho': 'Depth'})
                df_filtered['Variable'] = variable
                df_filtered['QC'] = 'N'

                df_filtered.replace("", np.nan, inplace=True)

                conv_factor = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Conv'].iloc[0]
                if conv_factor != 1:
                    df_filtered['Data'] = pd.to_numeric(df_filtered['Data'], errors='coerce')
                    df_filtered['Data'] *= conv_factor
                name_conv = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Key Value'].iloc[0]
                
                df_filtered = df_filtered.loc[:, ["Variable", "Date", "Depth", "Data", "QC"]]

                if "Perth_ROMS_0.5km_2023" in dir:
                    output_filename = f'Perth_ROMS_{file.split("_",4)[-1].replace(".csv","").replace("_","")}_500m_{name_conv.replace(" ","")}_profile_Data.csv'.replace("moooring","mooring")
                elif "WA_ROMS_2km_2000-2022" in dir:
                    output_filename = f'WA_ROMS_{file.split("_",5)[-1].replace(".csv","").replace("_","")}_2km_{name_conv.replace(" ","")}_profile_Data.csv'.replace("moooring","mooring")

                print(output_dir,output_filename)    
                print(df_filtered.head())

                df_filtered.to_csv(os.path.join(output_dir, output_filename), index=False)

for dir in dir_lst:
    output_dir = f'../../../../data-warehouse/csv/wamsi/wwmsp5/roms/{dir.split("/")[-1].lower()}'
    os.makedirs(output_dir, exist_ok=True)
    process_data(dir, output_dir)
