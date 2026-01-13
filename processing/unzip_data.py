"""
This code is a translation/re-written python script of the original R code of the following publication:
Schwarzmueller, F. & Kastner, T (2022), Agricultural trade and its impact on cropland use
and the global loss of species' habitats. Sustainability Science, doi: 10.1007/s11625-022-01138-7

Please cite ;-)
(c) Florian Schwarzmueller, December 2021
Re-written in Python, October 2025 by Louis De Neve
"""

import os
import zipfile

from pathlib import Path

def unzip_data(path="./input_data"):
    """
    Unzip FAOSTAT data files
    This function should be executed to unzip the data from FAOSTAT if not already done
    """
    
    # Look for zip files in the path directory
    zip_files = list(Path(path).glob("*.zip"))
    
    if not zip_files:
        print("No zip files to extract")
        return
    
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            if os.path.exists(f"{path}/{zip_ref.namelist()[0]}"):
                continue
            else:
                print(f"Extracting {zip_file}...")
                zip_ref.extractall(path)

    # Delete unnecessary files
    # Delete unnecessary files: *Flags.csv, AreaCodes.csv, Elements.csv, ItemCodes.csv
    unnecessary_patterns = ["*Flags.csv", "*AreaCodes.csv", "*Elements.csv", "*ItemCodes.csv", "*NOFLAG.csv"]
    for pattern in unnecessary_patterns:
        for file in Path(path).glob(pattern):
            if str(file)[11:14] == "SUA" and pattern == "*ItemCodes.csv":
                continue
            try:
                file.unlink()
            except Exception as e:
                print(f"Error deleting {file}: {e}")
