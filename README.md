# Python Data Import Pipeline

## Overview
This directory contains the Python pipeline for importing and processing data from various sources into the CSIEM data warehouse.

## Usage

1. Navigate to the actions directory from the csiem-data folder:
    ```
    cd code/actions_py
    ```
2. Execute the script:
    ```
    python3 execute_import_py_pipeline.py
    ```

> [!NOTE]
> Please set the variables to True in execute_import_py_pipeline.py to run the import pipeline for the desired datasets, and False to skip.
>
> xImport functions are located in code/import_py.
