"""
Created on Fri Dec 12 2025

@author: Louis De Neve - vectorised and integrated into MRIO pipeline Dec 2025
"""

import pandas as pd
import os
from pathlib import Path
    


def add_cols(indf, area_codes, item_codes):
    ac = area_codes[["ISO3", "FAOSTAT"]].rename(columns={"ISO3":"Country_ISO", "FAOSTAT":"Producer_Country_Code"})
    indf = indf.merge(ac, on="Producer_Country_Code", how="left")

    ic = item_codes[[" Item Code", " Item"]].rename(columns={" Item Code":"Item_Code", " Item":"Item"})
    indf = indf.merge(ic, on="Item_Code", how="left")

    if "Animal_Product_Code" in indf.columns:
        ic = ic.rename(columns={"Item_Code":"Animal_Product_Code", "Item":"Animal_Product"})
        indf = indf.merge(ic, on="Animal_Product_Code", how="left")

    return indf


def main(year, country_of_interest, sua, historic="", results_dir=Path("./results")):

    
    datPath = "./input_data"
    trade_feed = results_dir / str(year) / ".mrio" / "TradeMatrixFeed_import_dry_matter.csv"
    trade_nofeed = results_dir / str(year) / ".mrio" / "TradeMatrix_import_dry_matter.csv"


    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        area_codes = pd.read_excel(f"{datPath}/nocsDataExport_20251021-164754.xlsx", engine="openpyxl")  
    item_codes = pd.read_csv(f"{datPath}/SUA_Crops_Livestock_E_ItemCodes.csv", encoding = "latin-1", low_memory=False)
    country_code = area_codes[area_codes["ISO3"] == country_of_interest]["FAOSTAT"].values[0]

    prov_mat_no_feed = pd.read_csv(trade_nofeed)
    prov_mat_feed = pd.read_csv(trade_feed)
    animal_codes = prov_mat_feed["Animal_Product_Code"].dropna().unique().tolist()
    # print(country_code)

    alpha = prov_mat_no_feed[prov_mat_no_feed["Item_Code"].isin(animal_codes)].copy()
    beta = prov_mat_feed[~prov_mat_feed["Animal_Product_Code"].isna()].copy()
    gamma = prov_mat_feed[prov_mat_feed["Animal_Product_Code"].isna()].copy()

    gamma = gamma.drop(columns=["Animal_Product_Code"])

    alpha["Animal_Product"] = "Primary"
    gamma["Animal_Product"] = ""


    human_consumed = pd.concat([alpha, gamma], ignore_index=True)
    human_consumed = human_consumed[human_consumed.Consumer_Country_Code == country_code]


    totals = alpha.groupby(["Producer_Country_Code", "Item_Code"])["Value"].sum().reset_index()
    totals["Total"] = totals["Value"]
    totals = totals.drop(columns=["Value"])
    alpha = alpha.merge(totals, on=["Producer_Country_Code", "Item_Code"], how="left")
    alpha["Proportion"] = alpha["Value"] / alpha["Total"]
    alpha = alpha.drop(columns=["Total"])


    animals_consumed_in_country = alpha[alpha.Consumer_Country_Code == country_code].copy()
    
    # alpha2=add_cols(animals_consumed_in_country, area_codes, item_codes)
    # print(alpha2[alpha2.Animal_Product=="Primary"].groupby(["Item"])["Value"].sum())
    
    animals_consumed_in_country["match_code"] = animals_consumed_in_country["Producer_Country_Code"].astype(str) + "_" + animals_consumed_in_country["Item_Code"].astype(str)
    beta["match_code"] = beta["Consumer_Country_Code"].astype(str) + "_" + beta["Animal_Product_Code"].astype(str)
    feed = beta[beta["match_code"].isin(animals_consumed_in_country["match_code"])]

    feed = feed.merge(animals_consumed_in_country[["match_code", "Proportion"]], on="match_code", how="left")
    feed["Value"] = feed["Value"] * feed["Proportion"]
    feed["Error"] = feed["Error"] * feed["Proportion"]
    feed = feed.drop(columns=["match_code", "Proportion"])
    feed = feed[feed.Value > 0.015]
    human_consumed = human_consumed[human_consumed.Value > 0.015]

    feed = add_cols(feed, area_codes, item_codes)
    human_consumed = add_cols(human_consumed, area_codes, item_codes)
    # print(human_consumed[human_consumed.Animal_Product=="Primary"].groupby(["Item"])["Value"].sum().sum())
    
    feed = feed.rename(columns={"Value": "provenance", "Error": "provenance_err"})
    human_consumed = human_consumed.rename(columns={"Value": "provenance", "Error": "provenance_err"})
    human_consumed["Animal_Product_Code"] = ""

    country_savefile_path = results_dir / str(year) / country_of_interest
    human_consumed.to_csv(f"{country_savefile_path}/human_consumed.csv")
    feed.to_csv(f"{country_savefile_path}/feed.csv")

    return human_consumed, feed


if __name__ == "__main__":

    YEARS = [2019]
    COUNTRIES = ["GBR"]
    os.chdir("../")
    
    for year in YEARS:
        for country in COUNTRIES:
            print(f"Processing {country} for year {year}...")
            hc, feed = main(year, country, "")
