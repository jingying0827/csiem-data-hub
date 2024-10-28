import pandas as pd
import numpy as np
import os

dir_lst = ["../../../../data-lake/ESA/GLOBCOLOR/Optics/Point",
           "../../../../data-lake/ESA/GLOBCOLOR/Optics/Polygon",
           "../../../../data-lake/ESA/GLOBCOLOR/Plankton/Point",
           "../../../../data-lake/ESA/GLOBCOLOR/Plankton/Polygon",
           "../../../../data-lake/ESA/GLOBCOLOR/PP/Point",
           "../../../../data-lake/ESA/GLOBCOLOR/PP/Polygon",
           "../../../../data-lake/ESA/GLOBCOLOR/Reflectance/Point",
           "../../../../data-lake/ESA/GLOBCOLOR/Reflectance/Polygon",
           "../../../../data-lake/ESA/GLOBCOLOR/Transp/Point",
           "../../../../data-lake/ESA/GLOBCOLOR/Transp/Polygon",
           "../../../../data-lake/ESA/SENTINEL/Chla/Points",
           "../../../../data-lake/ESA/SENTINEL/Chla/Polygon_offshore",
           "../../../../data-lake/MOI/NEMO/Model_salinity/Points",
           "../../../../data-lake/MOI/NEMO/Model_salinity/Polygon",
           "../../../../data-lake/MOI/PISCES/Model_bio/Points",
           "../../../../data-lake/MOI/PISCES/Model_bio/Polygon",
           "../../../../data-lake/MOI/PISCES/Model_car/Points",
           "../../../../data-lake/MOI/PISCES/Model_car/Polygon",
           "../../../../data-lake/MOI/PISCES/Model_co2/Points",
           "../../../../data-lake/MOI/PISCES/Model_co2/Polygon",
           "../../../../data-lake/MOI/PISCES/Model_Nut/Points",
           "../../../../data-lake/MOI/PISCES/Model_Nut/Polygon",
           "../../../../data-lake/MOI/PISCES/Model_optics/Points",
           "../../../../data-lake/MOI/PISCES/Model_optics/Polygon",
           "../../../../data-lake/MOI/PISCES/Model_pft/Points",
           "../../../../data-lake/MOI/PISCES/Model_pft/Polygon",
           "../../../../data-lake/MOI/SEAPODYM/Model_PP_ZO/Points",
           "../../../../data-lake/MOI/SEAPODYM/Model_PP_ZO/Polygon",
           "../../../../data-lake/NASA/GHRSST/Points",
           "../../../../data-lake/NASA/GHRSST/Polygon_offshore",
           "../../../../data-lake/NASA/MODIS/PAR/Points",
           "../../../../data-lake/NASA/MODIS/PAR/Polygon",
           "../../../../data-lake/NASA/MODIS/PIC/Points",
           "../../../../data-lake/NASA/MODIS/PIC/Polygon",
           "../../../../data-lake/NASA/MODIS/POC/Points",
           "../../../../data-lake/NASA/MODIS/POC/Polygon"
           ]

mapping_keys_df = pd.read_csv("mapping_keys.csv")

def process_data(dir):
    for file in os.listdir(dir):
        if file.endswith(".csv") and "Store" not in file:
            # print(file)
            # Read data file
            df = pd.read_csv(os.path.join(dir, file), header=0, encoding='utf-8')
            df['Date'] = pd.to_datetime(df['time'])
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            # print(df)

            # Extract site id from file
            if "SENTINEL/Chla/Points" in dir:
                site = file.split(".")[0].replace("_CHL","")
            elif "GLOBCOLOR" in dir and "Point" in dir:
                site = "CMEMS_GlobColor_" + "_".join(file.replace(".csv","").split("_")[2:4])
            elif "NEMO" in dir and "Points" in dir:
                site = file.split(".")[0]
            elif "PISCES" in dir and "Points" in dir:
                site = "CMEMS_PISCES_" + "_".join(file.replace(".csv","").split("_")[-2:])
            elif "SEAPODYM" in dir and "Points" in dir:
                site = "CMEMS_SEAPODYM_" + "_".join(file.replace(".csv","").split("_")[-2:])
            elif "GHRSST" in dir and "Points" in dir:
                site = "GHRSST_point_" + file.replace(".csv","").split("_p")[1]
            elif "GHRSST" in dir and "Polygon" in dir:
                site = "GHRSST_polygon_" + file.replace(".csv","").split("_p")[1]
            elif "MODIS" in dir and "Points" in dir:
                site = "MODIS_point_" + file.replace(".csv","").split("_p")[1]
            elif "MODIS" in dir and "Polygon" in dir:
                site = "MODIS_polygon_" + file.replace(".csv","").split("_p")[1]
            elif "NASA" not in dir and "Polygon" in dir:
                site = "CMEMS_" + "_".join(file.replace(".csv","").split("_")[-2:])
            print(site)

            variables_name = list(df)
            variables = [var for var in variables_name if var not in ['time','latitude','longitude','Date','depth']]
            # print(variables)

        for variable in variables:
            df_filtered = pd.DataFrame()  # Initialize an empty DataFrame

            df_filtered['Date'] = df['Date']

            df_filtered['Data'] = df[variable]

            df_filtered['Variable'] = variable

            if 'depth' in df.columns:
                df_filtered['Depth'] = df['depth']
            else:
                df_filtered['Depth'] = 0

            df_filtered["QC"] = 'N'

            df_filtered = df_filtered.sort_values(by='Date')

            # Replace empty cells with NaN
            df_filtered.replace("", np.nan, inplace=True)

            df_filtered = df_filtered.loc[:, ["Variable", "Date", "Depth", "Data", "QC"]]

            # Convert value of different units
            conv_factor = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Conv'].iloc[0]
            if conv_factor != 1:
                df_filtered['Data'] = pd.to_numeric(df_filtered['Data'], errors='coerce')  # Convert non-numeric values to NaN
                df_filtered['Data'] *= conv_factor

            # Drop rows where Data is NaN
            df_filtered = df_filtered.dropna(subset=['Data'])
            # Handle duplicate rows with same date but different data
            df_filtered['Data'] = pd.to_numeric(df_filtered['Data'], errors='coerce')
            df_filtered = df_filtered.groupby(['Variable', 'Date', 'Depth', 'QC']).agg({
                'Data': 'mean'
            }).reset_index()

            name_conv = mapping_keys_df.loc[mapping_keys_df['Params.Name'] == variable, 'Key Value'].iloc[0]
            output_filename = f'{file.split(".")[0]}_{name_conv.replace(" ","")}_profile_Data.csv'
            if "combined" in output_filename:
                if "Point" in dir:
                    output_filename = f'{site}_{name_conv.replace(" ","")}_profile_Data.csv'
                elif "Polygon" in dir:
                    output_filename = f'{site}_{name_conv.replace(" ","")}_profile_Data.csv'
            print(output_filename)
            print(df_filtered)
            # Write the filtered DataFrame to a CSV file in the specified directory only if it's not empty
            output_dir = dir.replace("data-lake","data-warehouse/csv")
            output_dir = "/".join(output_dir.split("/")[:-1]).lower()
            os.makedirs(output_dir, exist_ok=True)
            if not df_filtered.empty:
                df_filtered.to_csv(os.path.join(output_dir, output_filename), index=False)

for dir in dir_lst:
    process_data(dir)
