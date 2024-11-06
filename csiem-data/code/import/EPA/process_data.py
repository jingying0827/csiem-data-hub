import pandas as pd
import os
import numpy as np

# Define the path to the data folder
data_folder = '../../../../data-lake/EPA/SMCWS'
output_folder = '../../../../data-warehouse/csv/epa/smcws'
os.makedirs(output_folder, exist_ok=True)
mapping_keys_df = pd.read_csv('mapping_keys.csv')
site_info_df = pd.read_csv('../../../data-mapping/By Agency/EPA_SMCWS.csv')
df_lst = []

def process_data(data_folder):
    # Define constants
    EXCLUDED_SHEETS = {'Table Content', 'Table 1', 'Metadata'}
    
    for file in os.listdir(data_folder):
        if not (file.endswith('.xlsx') and not file.startswith('~$')):
            continue
            
        excel_file = pd.ExcelFile(os.path.join(data_folder, file))
        print(f"Processing file: {file}")
        
        # Filter sheets
        sheet_names = [sheet for sheet in excel_file.sheet_names if sheet not in EXCLUDED_SHEETS]
        
        # Process each sheet
        for sheet_name in sheet_names:
            print(f"Processing sheet: {sheet_name}")
            if file == "SMCWS_Jan1991_Feb1992.xlsx":
                df = pd.read_excel(
                    excel_file,
                    skiprows=2,
                    sheet_name=sheet_name
                )
                # Skip rows containing 'mean' or 'se'
                df = df[~df.apply(lambda x: x.astype(str).str.contains('mean|se|CS5|CS7|CS8', case=False)).any(axis=1)]
            elif file == "SMCWS_Dec1993_Mar1994.xlsx":
                df = pd.read_excel(
                    excel_file, 
                    skiprows=1,
                    sheet_name=sheet_name
                ).iloc[1:]

            # Drop columns where all values are NaN and rows where all values are NaN
            df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
            df['DATE'] = pd.to_datetime(df['DATE']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_lst.append(df)

    # Combine all sheets
    df = pd.concat(df_lst, ignore_index=True)
    # print(df)
    
    # Get unique values once
    sample_lst = [s for s in df['SAMPLE'].unique() if 'WS B' not in s]
    # print(sample_lst)
    variables = [col for col in df.columns if col not in {"SAMPLE", "DATE", "PO4"}]

    # Dictionary to store DataFrames by sitecode, measurement_code and name_conv
    output_dfs = {}

    for sample in sample_lst:
        print(sample)
        parts = sample.split(" ")
        sitecode = parts[0]
        measurement_code = parts[1] if len(parts) > 1 else "T"
        print(sitecode, measurement_code)
        
        # Filter once for each sample
        df_sample = df[df['SAMPLE'] == sample]
        sample_dates = df_sample['DATE']

        if "M" in measurement_code:
            DEPTH = site_info_df.loc[site_info_df['SiteCode'] == sitecode, 'Depth'].iloc[0]/2
        elif "T" in measurement_code:
            DEPTH = site_info_df.loc[site_info_df['SiteCode'] == sitecode, 'Depth'].iloc[0]
        else:
            DEPTH = 0
        
        for variable in variables:
            # Create filtered DataFrame
            df_filtered = pd.DataFrame({
                'Date': sample_dates,
                'Data': df_sample[variable],
                'Variable': variable,
                'Depth': DEPTH,
                'QC': 'N'
            })
            
            df_filtered.replace("", np.nan, inplace=True)
            
            # Apply conversion if needed
            conv_factor = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Conv'].iloc[0]
            if conv_factor != 1:
                df_filtered['Data'] = pd.to_numeric(df_filtered['Data'], errors='coerce') * conv_factor
            
            df_filtered = df_filtered.dropna(subset=['Data'])
            
            # Reorder columns
            df_filtered = df_filtered[["Variable", "Date", "Depth", "Data", "QC"]]

            name_conv = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Key Value'].iloc[0]
            
            if not df_filtered.empty:
                output_key = f'{sitecode}_{measurement_code}_{name_conv.replace(" ","")}'
                
                if output_key in output_dfs:
                    output_dfs[output_key] = pd.concat([output_dfs[output_key], df_filtered])
                else:
                    output_dfs[output_key] = df_filtered

    # Process and save each combined DataFrame
    for output_key, combined_df in output_dfs.items():
        # Remove duplicates (ignoring Variable column) and sort by date
        combined_df = combined_df.drop_duplicates(subset=['Date', 'Depth', 'Data', 'QC']).sort_values('Date')

        # Replace "<1" values with 0
        combined_df.loc[combined_df['Data'] == '<1', 'Data'] = 0

        # Group by date and keep first value of Variable along with mean of Data for duplicate dates
        combined_df = combined_df.groupby(['Date', 'Depth', 'QC'], as_index=False).agg({
            'Variable': 'first',
            'Data': 'mean'
        })

        # Reorder columns
        combined_df = combined_df[["Variable", "Date", "Depth", "Data", "QC"]]
        
        output_filename = f'{output_key}_profile_Data.csv'
        print(output_filename)
        combined_df.to_csv(os.path.join(output_folder, output_filename), index=False)
        print(combined_df)
    
process_data(data_folder)