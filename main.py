#!/usr/bin/env python3
"""
This code is a translation/re-written python script of the original R code of the following publication:
Schwarzmueller, F. & Kastner, T (2022), Agricultural trade and its impact on cropland use
and the global loss of species' habitats. Sustainability Science, doi: 10.1007/s11625-022-01138-7

Please cite ;-)
(c) Florian Schwarzmueller, December 2021
Re-written in Python, October 2025 by Louis De Neve

Added multiprocessing (country-level parallelism) October/December 2025
"""

import os
from pathlib import Path
import time

import multiprocessing

from processing.unzip_data import unzip_data
from processing.calculate_trade_matrix import calculate_trade_matrix
from processing.animal_products_to_feed import animal_products_to_feed
from processing.calculate_area import calculate_area

from provenance._consumption_provenance import main as consumption_provenance_main
from provenance._get_impacts_bd import get_impacts as get_impacts_main
from provenance._process_dat import main as process_dat_main
# from provenance.global_commodity_impacts import main as global_commodity_impacts_main

from pandas import read_excel, read_csv

# CONFIG
RESULTS_DIR = "./results_2"
YEARS = list(range(2010, 2022))

# Select a conversion method
CONVERSION_OPTION = "dry_matter"

# Prefer import or export data
PREFER_IMPORT = "import"

# select working directory
WORKING_DIR = '.'

N_PROCESSES = 16

# Pipeline components to run
# 0 = all, 1 = unzip, 2 = trade matrix, 3 = animal products to feed, 4 = area calculation, 5 = country impacts
PIPELINE_COMPONENTS: list = [0]

cdat = read_excel("input_data/nocsDataExport_20251021-164754.xlsx")
COUNTRIES = [_.upper() for _ in cdat["ISO3"].unique().tolist() if isinstance(_, str)]
# COUNTRIES = ["USA", "IND", "BRA", "JPN", "UGA", "GBR"]
COUNTRIES = ["BRA", "USA"]

# globals for workers
_SUA = None
_HIST = None


def _init_worker(sua_path: str, hist: str):
    """
    Loads the SUA CSV once per worker process to speed things up
    """
    global _SUA, _HIST
    _HIST = hist
    try:
        _SUA = read_csv(sua_path, encoding="latin-1", low_memory=False)
    except Exception as e:
        raise RuntimeError(f"Worker failed to load SUA from {sua_path}: {e}")


def _process_country(country: str, year: int):
    """
    Uses the globally-initialized _SUA and _HIST values.
    """
    global _SUA, _HIST
    missing_items_local = []
    try:
        print(f"    [PID {os.getpid()}] Processing country: {country}")
        t0 = time.perf_counter()
        # consumption_provenance_main returns (cons, feed) per original script
        cons, feed = consumption_provenance_main(year, country, _SUA, _HIST, results_dir=Path(RESULTS_DIR))
        if len(cons) == 0:
            print(f"    [PID {os.getpid()}] No consumption data for {country} in {year}")
            return []  # nothing to do for this country
        bf = get_impacts_main(feed, year, country, "feed_impacts_wErr.csv", results_dir=Path(RESULTS_DIR))
        bh = get_impacts_main(cons, year, country, "human_consumed_impacts_wErr.csv", results_dir=Path(RESULTS_DIR))
        mi = process_dat_main(year, country, bh, bf, results_dir=Path(RESULTS_DIR))
        missing_items_local.extend(mi)
        t1 = time.perf_counter()
        print(f"    [PID {os.getpid()}] Completed {country} in {t1 - t0:.2f} seconds")
        return missing_items_local
    except Exception as e:
        # Print error and continue; return empty list so caller can continue aggregating
        print(f"    [PID {os.getpid()}] Error processing {country} for {year}: {e}")
        return []


def main(years=list(range(1986, 2022)),
         conversion_option="dry_matter",
         prefer_import="import",
         pipeline_components=[0],
         working_dir=".",
         countries=None,
         results_dir="./results",
         n_processes=None):

    if countries is None:
        countries = COUNTRIES

    os.system('cls' if os.name == 'nt' else 'clear')
    os.chdir(working_dir)

    component_dict = {
        0: "Full pipeline",
        1: "Unzipping data",
        2: "Trade matrix calculation",
        3: "Animal products to feed calculation",
        4: "Area calculation",
        5: "Country-level provenance calculations",
    }

    print(f"""\nStarting MRIO calculations with options:
    Working directory: {working_dir}
    Using {conversion_option} as the conversion option
    Preferring {prefer_import} data
    Running pipeline component {[p for p in pipeline_components]}: {[component_dict[p] for p in pipeline_components]}

    Years to process: {years}
    """)

    # check results dir
    results_dir = Path(results_dir)
    results_dir.mkdir(exist_ok=True)

    if (0 in pipeline_components) or (1 in pipeline_components):
        print("Unzipping data...")
        try:
            unzip_data("./input_data") 
            print("Data already unzipped or unzipping completed successfully.")
        except Exception as e:
            print(f"Error during data unzipping: {e}")


    if pipeline_components == [1]:
        return

    if n_processes is None:
        try:
            n_processes = max(1, multiprocessing.cpu_count() - 1)
        except Exception:
            n_processes = 1

    for year in years:

        # year_dir = Path(f"./results/{year}")
        year_dir = results_dir / str(year)
        year_dir.mkdir(exist_ok=True)
        # mrio_dir = Path(f"./results/{year}/.mrio")
        mrio_dir = results_dir / str(year) / ".mrio"
        mrio_dir.mkdir(exist_ok=True)

        print(f"\nProcessing year: {year}")

        hist = "Historic" if year < 2010 else ""

        if (0 in pipeline_components) or (2 in pipeline_components):
            calculate_trade_matrix(
                conversion_opt=conversion_option,
                prefer_import=prefer_import,
                year=year,
                historic=hist,
                results_dir=results_dir)

        if (0 in pipeline_components) or (3 in pipeline_components):
            animal_products_to_feed(
                prefer_import=prefer_import,
                conversion_opt=conversion_option,
                year=year,
                historic=hist,
                results_dir=results_dir)

        if (0 in pipeline_components) or (4 in pipeline_components):
            if 4 in pipeline_components:
                print("    MRIO area calculation is deprecated")
            else:
                print("    MRIO complete")

            # calculate_area(
            #     prefer_import=PREFER_IMPORT,
            #     conversion_opt=CONVERSION_OPTION,
            #     year=year)

        if (0 in pipeline_components) or (5 in pipeline_components):
            print("    Processing country-level provenance and impacts...")
            missing_items = []

            if hist == "Historic":
                sua_path = "./input_data/FoodBalanceSheetsHistoric_E_All_Data_(Normalized).csv"
            else:
                sua_path = "./input_data/SUA_Crops_Livestock_E_All_Data_(Normalized).csv"

            if len(countries) <= 1 or n_processes == 1:
                try:
                    if hist == "Historic":
                        sua = read_csv(sua_path, encoding="latin-1", low_memory=False)
                    else:
                        sua = read_csv(sua_path, encoding="latin-1", low_memory=False)
                except Exception as e:
                    print(f"Failed to load SUA in main process: {e}")
                    sua = None

                for country in countries:
                    try:
                        print(f"    Processing country: {country}")
                        t0 = time.perf_counter()
                        cons, feed = consumption_provenance_main(year, country, sua, hist, results_dir=results_dir)
                        if len(cons) == 0:
                            continue
                        bf = get_impacts_main(feed, year, country, "feed_impacts_wErr.csv", results_dir=results_dir)
                        bh = get_impacts_main(cons, year, country, "human_consumed_impacts_wErr.csv", results_dir=results_dir)
                        mi = process_dat_main(year, country, bh, bf, results_dir=results_dir)
                        missing_items.extend(mi)
                        t1 = time.perf_counter()
                        print(f"         Completed in {t1 - t0:.2f} seconds")
                    except Exception as e:
                        print(f"Error processing {country}: {e}")

            else:
                # Use a Pool of worker processes. Initialize each worker to load the SUA file once.
                processes = min(n_processes, len(countries))
                print(f"    Spawning {processes} worker processes for {len(countries)} countries")
                pool = multiprocessing.Pool(processes=processes, initializer=_init_worker, initargs=(sua_path, hist))
                try:
                    args_iterable = [(c, year) for c in countries]

                    results = pool.starmap(_process_country, args_iterable)

                    for res in results:
                        if res:
                            missing_items.extend(res)
                finally:
                    pool.close()
                    pool.join()

            # missing_items_file = Path(f"./results/{year}/missing_items.txt")
            missing_items_file = results_dir / str(year) / "missing_items.txt"

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
        results_dir=RESULTS_DIR,
        countries=COUNTRIES,
        n_processes=N_PROCESSES, 
    )