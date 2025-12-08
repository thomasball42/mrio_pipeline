#!/usr/bin/env python3
"""
This code is a translation/re-written python script of the original R code of the following publication:
Schwarzmueller, F. & Kastner, T (2022), Agricultural trade and its impact on cropland use
and the global loss of species' habitats. Sustainability Science, doi: 10.1007/s11625-022-01138-7

Please cite ;-)
(c) Florian Schwarzmueller, December 2021
Re-written in Python, October 2025 by Louis De Neve
"""


import os
from pathlib import Path
import time

from processing.unzip_data import unzip_data
from processing.calculate_trade_matrix import calculate_trade_matrix
from processing.animal_products_to_feed import animal_products_to_feed
from processing.calculate_area import calculate_area

from provenance._consumption_provenance import main as consumption_provenance_main
from provenance._get_impacts_bd import get_impacts as get_impacts_main
from provenance._process_dat import main as process_dat_main
# from provenance.global_commodity_impacts import main as global_commodity_impacts_main

# CONFIG
# Select years for which to calculate the results 
YEARS = list(range(1986, 2022))
YEARS=[2018]

# Select a conversion method
# inputs: ("dry_matter", "Energy", "Protein", "Fiber_TD", "Zinc", "Iron", "Calcium",
#          "Folate_Tot", "Riboflavin", "Choline_Tot", "Potassium", "Vit_E", "Vit_B12",
#          "Vit_K", "Vit_A")
CONVERSION_OPTION = "dry_matter"

# elect whether to prefer reported import or export data
# inputs: ("import", "export")
PREFER_IMPORT = "import"

# select working directory
WORKING_DIR = '.'

# 0 = all, 1 = unzip, 2 = trade matrix, 3 = animal products to feed, 4 = area calculation, 5 = country impacts
PIPELINE_COMPONENTS:list = [5]
# PIPELINE_COMPONENTS:list = [5]

from pandas import read_excel, read_csv
cdat = read_excel("input_data/nocsDataExport_20251021-164754.xlsx")
COUNTRIES = [_.upper() for _ in cdat["ISO3"].unique().tolist() if isinstance(_, str)]
# COUNTRIES = ["USA", "IND", "BRA", "JPN", "UGA", "GBR"]
COUNTRIES = ["GBR"]

##########################################################

def main(years=list(range(1986, 2022)),
         conversion_option="dry_matter",
         prefer_import="import",
         pipeline_components=[0],
         working_dir=".",
         countries = ["GBR"],
         results_dir="./results"):
    
    os.system('cls' if os.name == 'nt' else 'clear')
    os.chdir(working_dir)

    component_dict = {
        0: "Full pipeline",
        1: "Unzipping data",
        2: "Trade matrix calculation",
        3: "Animal products to feed calculation",
        4: "Area calculation",
        5: "Country-level provenance calculations",}

    print(f"""\nStarting MRIO calculations with options:
    Working directory: {working_dir}
    Using {conversion_option} as the conversion option
    Preferring {prefer_import} data
    Running pipeline component {[p for p in pipeline_components]}: {[component_dict[p] for p in pipeline_components]} 

    Years to process: {years}
    """)

    # Create directories
    results_dir = Path("./results")
    results_dir.mkdir(exist_ok=True)


    if (0 in pipeline_components) or (1 in pipeline_components):
        print("Unzipping data...")
        try:
            unzip_data("./input_data") # Uncomment this line to enable unzipping
            print("Data already unzipped or unzipping completed successfully.")
        except Exception as e:
            print(f"Error during data unzipping: {e}")
            # Continue anyway as data might already be unzipped

    if pipeline_components == [1]:
        return
    
    for year in years:

        year_dir = Path(f"./results/{year}")
        year_dir.mkdir(exist_ok=True)
        mrio_dir = Path(f"./results/{year}/.mrio")
        mrio_dir.mkdir(exist_ok=True)

        print(f"\nProcessing year: {year}")
        
        hist = "Historic" if year < 2010 else ""

        if (0 in pipeline_components) or (2 in pipeline_components):
            calculate_trade_matrix(
                conversion_opt=conversion_option,
                prefer_import=prefer_import,
                year=year,
                historic=hist)
            
        if (0 in pipeline_components) or (3 in pipeline_components):
            animal_products_to_feed(
                prefer_import=prefer_import,
                conversion_opt=conversion_option,
                year=year,
                historic=hist)
            
        if (0 in pipeline_components) or (4 in pipeline_components):
            if 4 in pipeline_components:
                print("    MRIO area calculation is deprecated")
            else:
                print("   MRIO complete") 
            
            # calculate_area(
            #     prefer_import=PREFER_IMPORT,
            #     conversion_opt=CONVERSION_OPTION,
            #     year=year)
            

        if (0 in pipeline_components) or (5 in pipeline_components):
            print("    Processing country-level provenance and impacts...")
            missing_items = []
            if hist == "Historic":
                sua = read_csv(f"./input_data/FoodBalanceSheetsHistoric_E_All_Data_(Normalized).csv", encoding="latin-1", low_memory=False)
            else:
                sua = read_csv(f"./input_data/SUA_Crops_Livestock_E_All_Data_(Normalized).csv", encoding="latin-1", low_memory=False)

            for country in countries:
                print(f"    Processing country: {country}")
                t0 = time.perf_counter()
                cons, feed = consumption_provenance_main(year, country, sua, hist)
                if len(cons) == 0:
                    continue
                bf = get_impacts_main(feed, year, country, "feed_impacts_wErr.csv")  
                bh = get_impacts_main(cons, year, country, "human_consumed_impacts_wErr.csv") 
                mi = process_dat_main(year, country, bh, bf)
                missing_items.extend(mi)
                t1 = time.perf_counter()
                print(f"         Completed in {t1 - t0:.2f} seconds")

            
            # Save missing items to a file
            missing_items_file = Path(f"./results/{year}/missing_items.txt")
            with open(missing_items_file, "w") as f:
                f.write("Missing items and their codes:\n")
                for item, code in set(missing_items):
                    f.write(f" - {item}: {code}\n")



        print(f"Year {year} processing completed successfully\n")


if __name__ == "__main__":
    main(   
        years=YEARS,
        conversion_option=CONVERSION_OPTION,
        prefer_import=PREFER_IMPORT,
        pipeline_components=PIPELINE_COMPONENTS,
        working_dir=WORKING_DIR,
        countries=COUNTRIES
    )