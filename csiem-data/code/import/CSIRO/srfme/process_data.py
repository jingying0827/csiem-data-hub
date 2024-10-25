import pandas as pd
import numpy as np
import os

dir_lst = ["../../../../../data-lake/CSIRO/SRFME/MOORING/Two Rocks Moorings Data 2004-2005"]

mapping_keys_df = pd.read_csv("mapping_keys.csv")
pressure_df = pd.read_csv("mooring_instrument_pressure_m.csv")

def process_data(dir):
    for file in os.listdir(dir):
        if file.endswith(".csv"):
            if "Current" not in file:
                print(file)
                if "Pressure" in file:
                    row_skip = 4
                else:
                    row_skip = 2
                df = pd.read_csv(os.path.join(dir, file),skiprows=row_skip, header=0, usecols=[1,2])
                if "Pressure" in file:
                    df = df.rename(columns={'sampled':'Pressure (m)'})
                else:
                    df = df.rename(columns={'Date/Time':'date/time'})
                df['Date'] = pd.to_datetime(df['date/time'], format='%d/%m/%Y %H:%M', dayfirst=True)
                df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                df = df.dropna(how='all')
                # print(df)
            
            elif "Currents_A" in file:
                print(file)
                df = pd.read_csv(os.path.join(dir, file), header=None)

                # Extract substring before "-" from first row and copy to subsequent columns
                for col in df.columns:
                    first_row_value = str(df.iloc[0, col])
                    if '-' in first_row_value:
                        substring = first_row_value.split('-')[0]
                        # Propagate to subsequent columns
                        for next_col in range(col + 1, len(df.columns)):
                            if pd.notna(df.iloc[2, next_col]) and df.iloc[2, next_col].replace('.', '', 1).isdigit():
                                df.iloc[0, next_col] = substring
                            else:
                                break  # Stop if encounter a non-numeric value in the third row

                # Add first row value to fifth row if first row has a value
                for col in df.columns:
                    if pd.notna(df.iloc[0, col]):
                        new_value = str(df.iloc[0, col]).replace('-Current (m/s)', '').replace('Current (m/s)','').replace('-Shore','').replace('Cross','Cross-Shore').replace('Longshore','U_').replace('Cross-Shore','V_').strip() + str(df.iloc[4, col]).replace('m', '').replace('nan','avg').strip()
                        df.iloc[4, col] = new_value + 'm' if 'avg' not in new_value else new_value

                df.columns = df.iloc[4]
                df = df.drop([0,1,2,3,4])

                df['Date'] = pd.to_datetime(df['date time/depth'], format='%d/%m/%Y %H:%M', dayfirst=True)
                df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                df = df.dropna(how='all')
                
                # Drop first column, "date time/depth" column, and columns with all rows nan
                df = df.iloc[:, 1:]  # Drop first column
                df = df.drop(columns=['date time/depth'])  # Drop "date time/depth" column
                df = df.dropna(axis=1, how='all')  # Drop columns with all rows nan
                
                # Move 'Date' column to the first position
                cols = df.columns.tolist()
                cols = ['Date'] + [col for col in cols if col != 'Date']
                df = df[cols]
                
                # print(df)

            elif "Currents_B" in file:
                print(file)
                df = pd.read_csv(os.path.join(dir, file), header=None)

                # Select only the second, third, and fourth columns
                df = df.iloc[:, 1:4]

                # Set column headers
                df.columns = [df.iloc[1, 0], 
                            f"{df.iloc[0, 1]} {df.iloc[2, 1]}", 
                            f"{df.iloc[0, 2]} {df.iloc[2, 2]}"]

                # Remove the first three rows as they've been used for headers
                # and start the data from the fourth row
                df = df.iloc[3:]

                # Reset the index
                df = df.reset_index(drop=True)

                df['Date'] = pd.to_datetime(df['Date Time'], format='%d/%m/%Y %H:%M', dayfirst=True)
                df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                df = df.dropna(how='all')

                df = df.drop(columns=['Date Time'])  # Drop "date time/depth" column
                df = df.dropna(axis=1, how='all')  # Drop columns with all rows nan
                
                # Move 'Date' column to the first position
                df.columns = df.columns.str.replace(' ', '').str.replace('Longshore', 'U_').str.replace('Crosshore', 'V_')
                cols = df.columns.tolist()
                cols = ['Date'] + [col for col in cols if col != 'Date']
                df = df[cols]
                
                # print(df)
            
            elif "Currents_C" in file:
                print(file)
                df = pd.read_csv(os.path.join(dir, file), header=None)

                # Extract full string from first row and copy to subsequent columns
                for col in df.columns:
                    first_row_value = df.iloc[0, col]
                    if pd.notna(first_row_value):
                        # Propagate to subsequent columns
                        for next_col in range(col + 1, len(df.columns)):
                            third_row_value = str(df.iloc[2, next_col])
                            if pd.notna(third_row_value) and third_row_value.replace('.', '', 1).isdigit():
                                df.iloc[0, next_col] = first_row_value
                            else:
                                break  # Stop if encounter a non-numeric-looking value in the third row

                # Add first row value to fourth row if first row has a value
                for col in df.columns:
                    if pd.notna(df.iloc[0, col]):
                        df.iloc[3, col] = str(df.iloc[0, col]).replace('Cross-shore-Current (m/s)', 'V_').replace('Long-shore-Current (m/s)', 'U_').replace('Current Speed(m/s)', 'Tot_').strip() + str(df.iloc[3, col]).replace('m', '').strip() + 'm'

                df.columns = df.iloc[3]
                df = df.drop([0,1,2,3])

                df['Date'] = pd.to_datetime(df['date time/depth(m)'], format='%d/%m/%Y %H:%M', dayfirst=True)
                df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                df = df.dropna(how='all')
                
                # Drop first column, "date time/depth" column, and columns with all rows nan
                df = df.iloc[:, 1:]  # Drop first column
                df = df.drop(columns=['date time/depth(m)'])  # Drop "date time/depth" column
                df = df.dropna(axis=1, how='all')  # Drop columns with all rows nan
                
                # Move 'Date' column to the first position
                cols = df.columns.tolist()
                cols = ['Date'] + [col for col in cols if col != 'Date']
                df = df[cols]
                
                # print(df)

            var_lst = df.columns.to_list()
            variables = [var for var in var_lst if var not in ['date/time','Date']]

            if "1036" in file or "1484" in file:
                sensor_depth_from_seabed = 0
                sensor_depth = 20
            elif "1414" in file:
                sensor_depth = 28
            elif "3713" in file:
                sensor_depth_from_seabed = 0
            elif "010095" in file:
                sensor_depth_from_seabed = 0
            elif "3027" in file:
                if "50m" in file:
                    sensor_depth_from_seabed = 50
                    sensor_depth = 50
                else:
                    sensor_depth_from_seabed = 0
                    sensor_depth = 100
            elif "3712" in file:
                sensor_depth_from_seabed = 0
            elif "3125" in file:
                sensor_depth_from_seabed = 0
                sensor_depth = 20
            elif "4534" in file:
                sensor_depth_from_seabed = 10
                sensor_depth = 10
            elif "3050" in file:
                if "50m" in file:
                    sensor_depth_from_seabed = 50
                    sensor_depth = 50
                else:
                    sensor_depth_from_seabed = 0
                    sensor_depth = 100
            elif "4536" in file:
                sensor_depth_from_seabed = 90
                sensor_depth = 10
            elif "2972" in file:
                sensor_depth = 25
            
            if "_A_" in file:
                mooring = 'A'
            elif "_B_" in file:
                mooring = 'B'
            elif "_C_" in file:
                mooring = 'C'

            for variable in variables:
                if "Currents_" in file:
                    DEPTH = variable.split(' ')[-1].split('_')[1].replace('m', '').replace('.','dp')
                    if 'dp0' in DEPTH:
                        DEPTH = DEPTH.replace('dp0','')

                df_filtered = pd.DataFrame()  # Initialize an empty DataFrame

                df_filtered['Date'] = df['Date']
                df_filtered['Data'] = df[variable]
                df_filtered['Variable'] = variable

                if "Currents_" in file:
                    if "U_" in variable:
                        variable = "Longshore-Current (m/s)"
                    elif "V_" in variable:
                        variable = "Cross-Shore Current (m/s)"
                    elif "Tot_" in variable:
                        variable = "Current Speed (m/s)"

                if mooring == 'B':
                    df_filtered['Depth'] = sensor_depth
                else:
                    # Merge pressure data based on Mooring and Date
                    pressure_data = pressure_df[pressure_df['Mooring'] == mooring]
                    df_filtered = pd.merge(df_filtered, pressure_data[['Date', 'Pressure (m)']], 
                                           on='Date', how='left')
                    df_filtered['Depth'] = df_filtered['Pressure (m)'] - sensor_depth_from_seabed
                    # If Pressure (m) is 0, use sensor_depth instead
                    df_filtered.loc[df_filtered['Pressure (m)'].isnull(), 'Depth'] = sensor_depth
                    df_filtered = df_filtered.drop(['Pressure (m)'], axis=1)

                df_filtered["QC"] = 'N'

                df_filtered = df_filtered.sort_values(by='Date')

                # Replace empty cells with NaN
                df_filtered.replace("", np.nan, inplace=True)

                # Convert value of different units

                conv_factor = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Conv'].iloc[0]
                if conv_factor != 1:
                    df_filtered['Data'] = pd.to_numeric(df_filtered['Data'], errors='coerce')  # Convert non-numeric values to NaN
                    df_filtered['Data'] *= conv_factor
                
                # Handle duplicate rows with same date but different data
                df_filtered['Data'] = pd.to_numeric(df_filtered['Data'], errors='coerce')

                # Drop rows where Data is NaN
                df_filtered = df_filtered.dropna(subset=['Data'])

                df_filtered = df_filtered.groupby(['Variable', 'Date', 'Depth', 'QC']).agg({
                    'Data': 'mean'
                }).reset_index()

                df_filtered = df_filtered.loc[:, ["Variable", "Date", "Depth", "Data", "QC"]]

                name_conv = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Key Value'].iloc[0]
                file_info = file.split("_")

                output_dir = '../../../../../data-warehouse/csv/csiro/srfme/mooring'
                os.makedirs(output_dir, exist_ok=True)

                if "Currents_A" in file:
                    if "AQD1036_AQD1484" in file:
                        df_filtered_1036 = df_filtered[(df_filtered['Date'] < '2005-01-21') | (df_filtered['Date'] >= '2005-04-18')]
                        df_filtered_1484 =df_filtered[(df_filtered['Date'] < '2005-04-17') & (df_filtered['Date'] >= '2005-02-05')]
                        output_filename_1036 = f'{file_info[1]}_AQD1036_{int(sensor_depth)}m_at_{DEPTH}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        output_filename_1484 = f'{file_info[1]}_AQD1484_{int(sensor_depth)}m_at_{DEPTH}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        print(output_filename_1036)
                        print(df_filtered_1036)
                        print(output_filename_1484)
                        print(df_filtered_1484)
                        if not df_filtered_1036.empty:
                            df_filtered_1036.to_csv(os.path.join(output_dir, output_filename_1036), index=False)
                        if not df_filtered_1484.empty:
                            df_filtered_1484.to_csv(os.path.join(output_dir, output_filename_1484), index=False)
                
                elif "Currents_B" in file:
                    output_filename = f'{file_info[1]}_AQD1414_{int(sensor_depth)}m_at_{DEPTH}m_{name_conv.replace(" ","")}_profile_Data.csv'
                    print(output_filename)
                    print(df_filtered)
                    if not df_filtered.empty:
                        df_filtered.to_csv(os.path.join(output_dir, output_filename), index=False)
                
                elif "Currents_C" in file:
                    df_filtered_3713 = df_filtered[(df_filtered['Date'] < '2004-10-06')]
                    df_filtered_3712 = df_filtered[(df_filtered['Date'] >= '2005-01-12')]
                    output_filename_3713 = f'{file_info[1]}_AQD3713_{int(sensor_depth)}m_at_{DEPTH}m_{name_conv.replace(" ","")}_profile_Data.csv'
                    output_filename_3712 = f'{file_info[1]}_AQD3712_{int(sensor_depth)}m_at_{DEPTH}m_{name_conv.replace(" ","")}_profile_Data.csv'
                    print(output_filename_3713)
                    # print(df_filtered_3713)
                    print(output_filename_3712)
                    # print(df_filtered_3712)
                    if not df_filtered_3713.empty:
                        df_filtered_3713.to_csv(os.path.join(output_dir, output_filename_3713), index=False)
                    if not df_filtered_3712.empty:
                        df_filtered_3712.to_csv(os.path.join(output_dir, output_filename_3712), index=False)
                
                elif "Current" not in file:
                    if "3027_3050" in file:
                        df_filtered_3027 = df_filtered.loc[pd.to_datetime(df_filtered['Date']) < pd.to_datetime('2005-06-20')]
                        df_filtered_3050 = df_filtered.loc[pd.to_datetime(df_filtered['Date']) >= pd.to_datetime('2005-06-20')]
                        output_filename_3027 = f'{file_info[1]}_SBE{file_info[2]}_{int(sensor_depth)}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        output_filename_3050 = f'{file_info[1]}_SBE{file_info[3]}_{int(sensor_depth)}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        print(output_filename_3027)
                        print(output_filename_3050)
                        print(df_filtered_3027)
                        print(df_filtered_3050)
                        if not df_filtered_3027.empty:
                            df_filtered_3027.to_csv(os.path.join(output_dir, output_filename_3027), index=False)
                        if not df_filtered_3050.empty:
                            df_filtered_3050.to_csv(os.path.join(output_dir, output_filename_3050), index=False)
                    elif "SBE3050-3027" in file:
                        df_filtered_3027 = df_filtered[((df_filtered['Date'] < '2005-04-19') & (df_filtered['Date'] >= '2005-01-12')) | ((df_filtered['Date'] < '2004-10-08') & (df_filtered['Date'] >= '2004-07-01'))]
                        df_filtered_3050 = df_filtered[((df_filtered['Date'] < '2005-01-07') & (df_filtered['Date'] >= '2004-10-08')) | ((df_filtered['Date'] < '2005-07-17') & (df_filtered['Date'] >= '2005-06-20'))]
                        output_filename_3027 = f'{file_info[1]}_SBE3027_{int(sensor_depth)}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        output_filename_3050 = f'{file_info[1]}_SBE3050_{int(sensor_depth)}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        print(output_filename_3027)
                        print(output_filename_3050)
                        print(df_filtered_3027)
                        print(df_filtered_3050)
                        if not df_filtered_3027.empty:
                            df_filtered_3027.to_csv(os.path.join(output_dir, output_filename_3027), index=False)
                        if not df_filtered_3050.empty:
                            df_filtered_3050.to_csv(os.path.join(output_dir, output_filename_3050), index=False)
                    elif "AQD1036_RBR010095" in file:
                        df_filtered_1036 = df_filtered.loc[pd.to_datetime(df_filtered['Date']) < pd.to_datetime('2004-10-09')]
                        df_filtered_010095 = df_filtered.loc[pd.to_datetime(df_filtered['Date']) >= pd.to_datetime('2004-10-13')]
                        output_filename_1036 = f'{file_info[1]}_AQD1036_{int(sensor_depth)}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        output_filename_010095 = f'{file_info[1]}_RBR010095_{int(sensor_depth)}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        print(output_filename_1036)
                        print(output_filename_010095)
                        print(df_filtered_1036)
                        print(df_filtered_010095)
                        if not df_filtered_1036.empty:
                            df_filtered_1036.to_csv(os.path.join(output_dir, output_filename_1036), index=False)
                        if not df_filtered_010095.empty:
                            df_filtered_010095.to_csv(os.path.join(output_dir, output_filename_010095), index=False)
                    elif "SBE3027_RDI3712" in file:
                        df_filtered_3027 = df_filtered.loc[pd.to_datetime(df_filtered['Date']) < pd.to_datetime('2005-04-19')]
                        df_filtered_3712 = df_filtered.loc[pd.to_datetime(df_filtered['Date']) >= pd.to_datetime('2005-06-20')]
                        output_filename_3027 = f'{file_info[1]}_SBE3027_{int(sensor_depth)}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        output_filename_3712 = f'{file_info[1]}_RDI3712_{int(sensor_depth)}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        print(output_filename_3027)
                        print(output_filename_3712)
                        print(df_filtered_3027)
                        print(df_filtered_3712)
                        if not df_filtered_3027.empty:
                            df_filtered_3027.to_csv(os.path.join(output_dir, output_filename_3027), index=False)
                        if not df_filtered_3712.empty:
                            df_filtered_3712.to_csv(os.path.join(output_dir, output_filename_3712), index=False)
                    else:
                        output_filename = f'{file_info[1]}_{file_info[2]}_{int(sensor_depth)}m_{name_conv.replace(" ","")}_profile_Data.csv'
                        print(output_filename)
                        print(df_filtered)
                        if not df_filtered.empty:
                            df_filtered.to_csv(os.path.join(output_dir, output_filename), index=False)

for dir in dir_lst:
    process_data(dir)