"""
This code is a translation/re-written python script of the original R code of the following publication:
Schwarzmueller, F. & Kastner, T (2022), Agricultural trade and its impact on cropland use
and the global loss of species' habitats. Sustainability Science, doi: 10.1007/s11625-022-01138-7

Please cite ;-)
(c) Florian Schwarzmueller, December 2021
Re-written in Python, October 2025 by Louis De Neve
"""

from pathlib import Path
import numpy as np
import pandas as pd
from tqdm import tqdm
from numba import jit
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
np.seterr(divide="ignore")

def eliminate_dates(reporting_dates:pd.DataFrame, function_dataframe:pd.DataFrame) -> pd.DataFrame:
    # removes countries that were not correctly reported
    
    reporting_dates_start = reporting_dates[["Country_Code", "Start_Year"]].dropna()
    reporting_dates_end = reporting_dates[["Country_Code", "End_Year"]].dropna()

    for country, year in reporting_dates_start.values:
        function_dataframe.loc[(function_dataframe["Reporter_Country_Code"] == country) & (function_dataframe["Year"] < year), "Value"] = 0
    for country, year in reporting_dates_end.values:
        function_dataframe.loc[(function_dataframe["Reporter_Country_Code"] == country) & (function_dataframe["Year"] > year), "Value"] = 0

    return function_dataframe

@jit(nopython=True)
def calculate_mrio_matrices(Z, p):
    """JIT-compiled version of matrix calculations"""
    summation_vector = np.ones(len(p))
    x = p + Z @ summation_vector
    
    one_over_x = np.where(x != 0, 1.0/x, 0.0)
    A = Z @ np.diag(one_over_x)
    
    I = np.eye(len(p))
    R = np.linalg.pinv(I - A) @ np.diag(p) # note pseudo-inverse rather than inverse (inverse creates some extra)
    
    ac = x - Z.sum(axis=0)
    c = ac * one_over_x
    R_bar = np.diag(c) @ R
    
    return R_bar


def calculate_naive_matrix(Z, p):
    summation_vector = np.ones(len(p))
    x = p + Z @ summation_vector
    e = summation_vector @ Z
    one_over_x = np.where(x != 0, 1.0/x, 0.0)
    g = (x-e) * one_over_x
    G = np.diag(g)
    attributable_prod_and_import = Z + np.diag(p)
    R_error = G @ attributable_prod_and_import
    return R_error

def mrio_model(item_code, year, p_data, prod_data):
    """
    Perform matrix operations for MRIO calculation
    Equivalent to matrix.operation function in R
    
    Args:
        item_code: Primary item code to process
        year: Year to process
        primary_data: DataFrame with trade data in primary equivalents
        production_all: DataFrame with production data
    
    Returns:
        DataFrame with Consumer_Country_Code, Producer_Country_Code, Value, Item_Code, Year
    """

    data_subset = p_data[(p_data["Year"] == year) & (p_data["primary_item"] == item_code)]
    production_data_subset = prod_data[(prod_data["Year"] == year) & (prod_data["Item_Code"] == item_code)]

    production_data_subset.loc[production_data_subset["Value"].isna(), "Value"] = 0

    producers = production_data_subset["Area_Code"].unique()
    importers = data_subset["Consumer_Country_Code"].unique()
    exporters = data_subset["Producer_Country_Code"].unique()

    traders = np.union1d(importers, exporters)
    countries = np.union1d(producers, traders)

    country_dict = {code: idx for idx, code in enumerate(countries)}

    Z = np.zeros((len(countries), len(countries)))

    for _, row in data_subset.iterrows():
        i = country_dict[row["Consumer_Country_Code"]]
        j = country_dict[row["Producer_Country_Code"]]
        Z[i, j] = row["Value_Sum"] # denoted Z in Kastner 2011

    Z[np.isnan(Z)] = 0

    p = np.zeros((len(countries),)) 
    for _, row in production_data_subset.iterrows():
        i = country_dict[row["Area_Code"]]
        p[i] = row["Value"] # denoted p in Kastner 2011

    R_bar = calculate_mrio_matrices(Z, p)
    R_error = calculate_naive_matrix(Z, p)
    R_rel_error = np.divide(np.abs(R_bar - R_error), np.where(R_bar != 0, R_bar, 1))

    R_bar = np.round(R_bar, 2)
    # R_rel_error = np.round(R_rel_error, 5)
    nonzero_mask = R_bar != 0
    i_indices, j_indices = np.where(nonzero_mask)

    if len(i_indices) == 0:
        return []
    
    return [{
        "Consumer_Country_Code": countries[i],
        "Producer_Country_Code": countries[j], 
        "Value_Sum": R_bar[i, j],
        "primary_item": item_code,
        "Year": year,
        "Value_Error": R_rel_error[i, j],
    } for i, j in zip(i_indices, j_indices)]


def calculate_conversion_factors(conversion_opt, content_factors, item_map):
        """Calculate conversion factors from processed to primary items"""

        # Calculate conversion factors
        if conversion_opt not in content_factors.columns:
                raise ValueError(f"Primary Conversion option ({conversion_opt}) not available")
                
        joined = item_map.merge(
            content_factors[["Item_Code", conversion_opt]], 
            left_on="FAO_code", 
            right_on="Item_Code", 
            how="left")

        joined = joined.merge(
            content_factors[["Item_Code", conversion_opt]], 
            left_on="primary_item", 
            right_on="Item_Code", 
            how="left",
            suffixes=("_processed", "_primary"))

        joined["Conversion_factor"] = joined[f"{conversion_opt}_processed"] / joined[f"{conversion_opt}_primary"]
        joined = joined.sort_values("FAO_code")
        joined.loc[~np.isfinite(joined["Conversion_factor"]), "Conversion_factor"] = 0
        result = joined.dropna(subset=["Conversion_factor"])
        conversion_factors = result[["FAO_code", "FAO_name", "primary_item", "FAO_name_primary", "Conversion_factor"]]
        return conversion_factors


def calculate_trade_matrix(
        conversion_opt="dry_matter",
        prefer_import="import", 
        year=2013,
        historic="Historic",
        results_dir=Path("./results")):
    """Calculate Trade Matrix module for MRIO pipeline"""

    output_filename = results_dir / str(year) / ".mrio" / f"TradeMatrix_{prefer_import}_{conversion_opt}.csv"

    print("    Loading trade data...")

    # File paths
    item_map_filename = "input_data/primary_item_map_feed.csv" 
    trade_filename = "input_data/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv" # FAOSTAT import
    reporting_filename = "input_data/Reporting_Dates.xls"
    content_filename = "input_data/content_factors_per_100g.xlsx"
    production_filename = "input_data/Production_Crops_Livestock_E_All_Data_(Normalized).csv"
    sugar_processing_filename = f"input_data/FoodBalanceSheets{historic}_E_All_Data_(Normalized).csv"


    # Load Files
    item_map = pd.read_csv(item_map_filename, encoding="Latin-1")
    raw_trade_data = pd.read_csv(trade_filename, encoding="Latin-1")
    reporting_date = pd.read_excel(reporting_filename)
    content_factors = pd.read_excel(content_filename, skiprows=1)
    sugar_processing = pd.read_csv(sugar_processing_filename, encoding="Latin-1")                                
    production = pd.read_csv(production_filename, low_memory=False)


    # Rename columns
    item_map.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
    raw_trade_data.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
    reporting_date.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
    content_factors.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
    sugar_processing.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
    production.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)


    # Select year
    raw_trade_data = raw_trade_data[raw_trade_data["Year"] == year]
    sugar_processing = sugar_processing[sugar_processing["Year"] == year]
    production = production[production["Year"] == year]


    # Tweaks for slightly different files
    production_all = production[["Area_Code", "Area", "Item_Code", "Item", "Element_Code", "Element", "Year_Code", "Year", "Unit", "Value"]]

    item_map[item_map["FAO_code"]==156] = [156, "Sugar cane", 2545, "Sugar agregate"]
    item_map[item_map["FAO_code"]==157] = [157, "Sugar beet", 2545, "Sugar agregate"]
    # production_crops.drop(columns=["Note"], inplace=True)
    # production_offals = sugar_processing[(sugar_processing["Element_Code"] == 5511) & (sugar_processing["Item_Code"] == 2736)]
    # production_offals['Element_Code'] = 5510
    # production_offals['Value'] = production_offals['Value']*1000
    
    print("    Preprocessing trade data...")

    # Combine and filter
    # production_all = pd.concat([production_all, production_offals], ignore_index=True)
    production_all = production_all[(production_all["Area_Code"]<300) & (production_all["Element_Code"]==5510)]


    # harmonise import and export data

    data_import = raw_trade_data[raw_trade_data["Element_Code"] == 5610][["Reporter_Country_Code", "Partner_Country_Code", "Element_Code", "Item_Code", "Year", "Value"]]
    data_export = raw_trade_data[raw_trade_data["Element_Code"] == 5910][["Partner_Country_Code", "Reporter_Country_Code", "Element_Code", "Item_Code", "Year", "Value"]]


    data_import = eliminate_dates(reporting_date, data_import)
    data_export = eliminate_dates(reporting_date, data_export)

    data_import.loc[data_import["Reporter_Country_Code"] == data_import["Partner_Country_Code"], "Value"] = 0
    data_export.loc[data_export["Reporter_Country_Code"] == data_export["Partner_Country_Code"], "Value"] = 0
    data_import = data_import[data_import["Value"] != 0]
    data_export = data_export[data_export["Value"] != 0]

    data_import.rename(columns={"Reporter_Country_Code": "Consumer_Country_Code", "Partner_Country_Code": "Producer_Country_Code"}, inplace=True)
    data_export.rename(columns={"Reporter_Country_Code": "Producer_Country_Code", "Partner_Country_Code": "Consumer_Country_Code"}, inplace=True)

    if prefer_import == "import":
        trade_data = pd.concat([data_import, data_export], ignore_index=True)
        trade_data = trade_data.drop_duplicates(subset=["Consumer_Country_Code", "Producer_Country_Code", "Year", "Item_Code"],
                                                keep="first")
    elif prefer_import == "export":
        trade_data = pd.concat([data_export, data_import], ignore_index=True)
        trade_data = trade_data.drop_duplicates(subset=["Consumer_Country_Code", "Producer_Country_Code", "Year", "Item_Code"],
                                                keep="first")
    else:
        raise ValueError("prefer_import must be either 'import' or 'export'")

    trade_data = trade_data.sort_values(["Consumer_Country_Code", "Producer_Country_Code"])

    conversion_factors = calculate_conversion_factors(conversion_opt, content_factors, item_map)


    trade_data = trade_data.merge(
        conversion_factors,
        left_on="Item_Code",
        right_on="FAO_code",
        how="left")
    trade_data.drop(columns=["FAO_code"], inplace=True)

    trade_data["primary_Value"] = trade_data["Value"] * trade_data["Conversion_factor"]

    primary_data = trade_data.groupby(["Consumer_Country_Code", "Producer_Country_Code", "Year", "primary_item"])["primary_Value"].sum().reset_index()
    primary_data.columns = ["Consumer_Country_Code", "Producer_Country_Code", "Year", "primary_item", "Value_Sum"]
            
    primary_data = primary_data[
        (primary_data["primary_item"] != 0) & 
        (primary_data["primary_item"].notna()) &
        (primary_data["Value_Sum"].notna())]


    # Calculate sugar production and sugar production shares
    sugar_crop_codes = [156, 157]

    # Filter production data for sugar crops and merge with conversion factors
    sugar_production = production_all[production_all["Item_Code"].isin(sugar_crop_codes)].merge(
        conversion_factors, 
        left_on="Item_Code", 
        right_on="FAO_code", 
        how="left"
    )
    sugar_production["Value_new"] = sugar_production["Value"] * sugar_production["Conversion_factor"]

    # Calculate shares for later use
    sugar_shares = (sugar_production
        .groupby(["Area_Code", "Year"],)
        .apply(lambda x: x.assign(share=x["Value"] / x["Value"].sum()))
        .reset_index(drop=True)
        [["Area_Code", "Year", "Item_Code", "share"]]
        .query("share > 0"))

    # Add production data - group by key columns and aggregate
    sugar_production = (sugar_production
        .groupby(["Area_Code", "Area", "primary_item", "FAO_name_primary", 
                "Element_Code", "Element", "Year_Code", "Year", "Unit"])
        .agg({"Value_new": "sum"})
        .reset_index()
        .query("Value_new > 0")
        .rename(columns={"primary_item": "Item_Code", 
                        "FAO_name_primary": "Item",
                        "Value_new": "Value"})
        .assign(Flag=" "))

    # Add sugar production to main production data
    production_all = pd.concat([production_all, sugar_production], ignore_index=True)

    unique_combinations = primary_data[["Year", "primary_item"]].drop_duplicates()

    


    mrio_output = []
    for index, (yr, ic) in enumerate(tqdm(unique_combinations.values, desc="    Processing MRIO models", leave=True, position=0)):
        m = mrio_model(ic, yr, primary_data, production_all)
        mrio_output += m
        
    cols = list(primary_data.columns)
    cols.append("Value_Error")
    transformed_data = pd.DataFrame(mrio_output, columns=cols)

    missing_data = production_all[
        (production_all["Element_Code"] == 5510) &
        (production_all["Year"] == year) &
        (production_all["Item_Code"].notna()) &
        (production_all["Value"].notna()) &
        (production_all["Item_Code"].isin(item_map["primary_item"])) &
        (~production_all["Item_Code"].isin(primary_data["primary_item"]))]

    add_data = missing_data[["Area_Code", "Value", "Item_Code", "Year"]].copy()
    add_data = add_data.rename(columns={"Area_Code": "Producer_Country_Code"})
    add_data["Consumer_Country_Code"] = add_data["Producer_Country_Code"]
    add_data["Value_Error"] = 0
    print(len(transformed_data[transformed_data.Producer_Country_Code == transformed_data.Consumer_Country_Code]), "diagonal elements in MRIO trade matrix")
    print(len(transformed_data[(transformed_data.Producer_Country_Code == transformed_data.Consumer_Country_Code)&(transformed_data.Value_Error==0)]), "zero diagonal elements in MRIO trade matrix")

    transformed_data = transformed_data.rename(columns={"primary_item": "Item_Code", "Value_Sum": "Value"})
    transformed_data = pd.concat([transformed_data, add_data], ignore_index=True)

    ###################################

    sugar_trade_data = transformed_data[transformed_data["Item_Code"] == 2545]

    sugar_processing = sugar_processing[
        (sugar_processing["Item_Code"].isin([2536, 2537]))&
        (sugar_processing["Element_Code"] == 5131)&
        (sugar_processing["Area_Code"] < 300)&
        (sugar_processing["Value"] > 0)]
    sugar_processing['Value'] = sugar_processing['Value']*1000

    sugar_processing["Item_Code"] = sugar_processing["Item_Code"].replace({2536: 156, 2537: 157})

    sugar_processing = sugar_processing.merge(
        conversion_factors, 
        left_on="Item_Code", 
        right_on="FAO_code", 
        how="left"
    )
    
    sugar_processing["Value_new"] = sugar_processing["Value"] * sugar_processing["Conversion_factor"]


    sugar_processing = (sugar_processing
        .groupby(["Area_Code", "Year"])
        .apply(lambda x: x.assign(processing_share=x["Value_new"] / x["Value_new"].sum()))
        .reset_index(drop=True)[["Area_Code", "Year", "Item_Code", "processing_share"]])

    # Join sugar shares with processing data
    sugar_crop_share = sugar_shares.merge(sugar_processing, 
        on=["Area_Code", "Year", "Item_Code"], 
        how="left")

    # Check if a crop does not appear in processing data but does in production
    sugar_crop_share = (sugar_crop_share
        .groupby(["Area_Code", "Year"])
        .apply(lambda x: x.assign(control=x["processing_share"].sum(skipna=True)))
        .reset_index(drop=True))

    # If that is the case, set the contribution to processing to zero
    mask1 = (sugar_crop_share["processing_share"].isna() & 
            (sugar_crop_share["control"] == 1))
    sugar_crop_share.loc[mask1, "processing_share"] = 0

    # For all cases where there is no processing data, set the shares to production shares
    mask2 = sugar_crop_share["processing_share"].isna()
    sugar_crop_share.loc[mask2, "processing_share"] = sugar_crop_share.loc[mask2, "share"]

    sugar_crop_share = (sugar_crop_share
        .rename(columns={"Item_Code": "Sugar_Crop_Code"})
        .assign(Item_Code = 2545))

    sugar_crop_share = sugar_crop_share.merge(
        conversion_factors,
        left_on="Sugar_Crop_Code",
        right_on="FAO_code",
        how="left")

    sugar_data = sugar_trade_data.merge(
        sugar_crop_share,
        left_on=["Year", "Item_Code", "Producer_Country_Code"],
        right_on=["Year", "Item_Code", "Area_Code"],
        how="left")

    sugar_data["Value_new"] = sugar_data["Value"] * sugar_data["processing_share"] / sugar_data["Conversion_factor"]
    sugar_data = sugar_data[["Consumer_Country_Code", "Producer_Country_Code", "Year", "Sugar_Crop_Code", "Value_new", "Value_Error"]]
    sugar_data = sugar_data.rename(columns={"Value_new": "Value", "Sugar_Crop_Code": "Item_Code"})

    sugar_production_2 = production_all[
        production_all["Item_Code"].isin(sugar_crop_codes)
        ].rename(columns={"Value": "national_production"})
    
    sugar_data = (sugar_data
        .groupby(["Producer_Country_Code", "Year", "Item_Code"])
        .apply(lambda x: x.assign(sugar_crop_total=x["Value"].sum(skipna=True)))
        .reset_index(drop=True))

    sugar_data = sugar_data.merge(
        sugar_production_2,
        left_on=["Year", "Item_Code", "Producer_Country_Code"],
        right_on=["Year", "Item_Code", "Area_Code"],
        how="left")

    sugar_data["Value_new"] = sugar_data["Value"] + sugar_data["national_production"] - sugar_data["sugar_crop_total"]

    mask_diagonal = (sugar_data["Producer_Country_Code"] == sugar_data["Consumer_Country_Code"])
    sugar_data.loc[mask_diagonal, "Value"] = sugar_data.loc[mask_diagonal, "Value_new"]

    sugar_data = sugar_data[["Consumer_Country_Code", "Producer_Country_Code", "Value", "Item_Code", "Year", "Value_Error"]]
    
    output_data = pd.concat([transformed_data[transformed_data["Item_Code"] != 2545], sugar_data], ignore_index=True)

    print("    Saving MRIO results...")
    output_data["Error"] = output_data["Value_Error"] * output_data["Value"]
    # transformed_data["Value"] = transformed_data["Value"].round(2)
    output_data = output_data[["Consumer_Country_Code", "Producer_Country_Code", "Item_Code", "Year", "Value", "Error"]]
    output_data.to_csv(output_filename, index=False)

