# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 16:41:04 2023

@author: tom
"""
from pathlib import Path
import pandas as pd
import numpy as np
import os

try:
    import provenance.data_utils as data_utils
except ModuleNotFoundError:
    import data_utils

import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


def get_impacts(wdf, year, coi, filename, results_dir=Path("./results")):
    # setup
    country_savefile_path = results_dir / str(year) / coi
    datPath = "./input_data"

    
    # trim input data to significant values
    wdf = wdf[np.logical_not(wdf.Item.isna())]
    # wdf = wdf[wdf.Value >= 0.015]

    # load additional data and merge into wdf
    commodity_crosswalk = pd.read_csv(f"{datPath}/commodity_crosswalk.csv", index_col = 0)
    wwf = data_utils.get_wwf_pbd(datPath)
    Sm_wwf_items = pd.read_csv(f"{datPath}/schwarzmueller_wwf.csv",index_col = 0)
    wdf = (wdf
        .merge(Sm_wwf_items[["Item_Code_FAO", "WWF_cat"]], left_on="Item_Code", right_on="Item_Code_FAO", how="left")
        .drop(columns=["Item_Code_FAO"]))

    # load yield data
    fao_prod = pd.read_csv(f"{datPath}/Production_Crops_Livestock_E_All_Data_(Normalized).csv", encoding = "latin-1", low_memory=False)
    fao_prod = fao_prod[fao_prod.Year == year]
    yield_dat = fao_prod[fao_prod["Element Code"] == 5412]
    yield_dat = yield_dat.rename(columns={"Area Code":"Area_Code", "Item Code":"Item_Code"})
    yield_dat = yield_dat[["Area_Code", "Item_Code", "Value"]]
    
    
    # Fall back to global yields
    global_yields = yield_dat[yield_dat["Area_Code"] == 5000].copy()
    global_yields = global_yields.rename(columns={"Value":"Global_Yield"})
    wdf = wdf.merge(global_yields[["Global_Yield", "Item_Code"]], how="left", on="Item_Code")
    yield_dat = yield_dat.rename(columns={"Value":"Yield"})
    wdf = wdf.merge(yield_dat, how="left", left_on=["Producer_Country_Code", "Item_Code"], right_on=["Area_Code", "Item_Code"])
    wdf = wdf.drop(columns=["Area_Code"])
    wdf.loc[wdf.Yield.isna(), "Yield"] = wdf.loc[wdf.Yield.isna(), "Global_Yield"]
    wdf = wdf.drop(columns=["Global_Yield"])


    # convert from kg/ha to kg/m2
    wdf.Yield = wdf.Yield/10000 


    # load other impacts and fall back on global values
    wwf_arable_land = wwf[["Country_ISO", "Product", "Arable_avg", "SWWU_avg", "GHG_avg", "Pasture_avg"]].copy()
    wwf_global_values = (wwf[wwf["Country_ISO"]=="all-r"][["Product", "Arable_avg", "SWWU_avg", "GHG_avg", "Pasture_avg"]]
        .rename(columns={"Arable_avg":"Global_Arable_avg", "SWWU_avg":"Global_SWWU_avg", "GHG_avg":"Global_GHG_avg", "Pasture_avg":"Global_Pasture_avg"}))
    wdf = (wdf
        .merge(wwf_arable_land, how="left", left_on=["Country_ISO", "WWF_cat"], right_on=["Country_ISO", "Product"])
        .drop(columns=["Product"])
        .merge(wwf_global_values, how="left", left_on=["WWF_cat"], right_on=["Product"])
        .drop(columns=["Product"]))
    wdf.loc[wdf.Pasture_avg.isna(), "Pasture_avg"] = wdf.loc[wdf.Pasture_avg.isna(), "Global_Pasture_avg"]
    wdf.loc[wdf.Arable_avg.isna(), "Arable_avg"] = wdf.loc[wdf.Arable_avg.isna(), "Global_Arable_avg"]
    wdf.loc[wdf.SWWU_avg.isna(), "SWWU_avg"] = wdf.loc[wdf.SWWU_avg.isna(), "Global_SWWU_avg"]
    wdf.loc[wdf.GHG_avg.isna(), "GHG_avg"] = wdf.loc[wdf.GHG_avg.isna(), "Global_GHG_avg"]
    wdf = wdf.drop(columns=["Global_Arable_avg", "Global_SWWU_avg", "Global_GHG_avg", "Global_Pasture_avg"])


    # Pasture calcs (only runs if not feed calc)
    if filename[:4] != "feed":
        rums = {"Meat of cattle boneless; fresh or chilled" : "bvmeat",
            "Meat of cattle with the bone; fresh or chilled" : "bvmeat",
            'Meat of sheep; fresh or chilled' : "sgmeat",
            'Meat of goat; fresh or chilled' : "sgmeat",
            'Raw milk of cattle' : "bvmilk",
            }  
        rums_df = pd.DataFrame.from_dict(rums, orient='index', columns=['livestock'])
        tb_pasture_vals = pd.read_csv(f"{datPath}/tb_pasture_factors_2.csv", index_col = 0)[["livestock", "fp_m2_kg", "Country_ISO"]]
        global_median_tb = {v: tb_pasture_vals[tb_pasture_vals["livestock"]==v]["fp_m2_kg"].median() for v in set(rums.values())}
        global_median_tb_df = pd.DataFrame.from_dict(global_median_tb, orient='index', columns=['global_median_fp_m2_kg'])

        wdf = wdf.merge(rums_df, how="left", left_on="Item", right_index=True)
        wdf = wdf.merge(tb_pasture_vals, how="left", left_on=["Country_ISO", "livestock"], right_on=["Country_ISO", "livestock"])
        wdf = wdf.merge(global_median_tb_df, how="left", left_on=["livestock"], right_index=True)
        wdf["Pasture_avg"] = wdf[["Pasture_avg","fp_m2_kg", "global_median_fp_m2_kg"]].min(axis=1)
        wdf = wdf.drop(columns=["fp_m2_kg", "global_median_fp_m2_kg", "livestock"])

    # set non-applicable values to zero
    wdf.loc[wdf.Animal_Product == "Primary", "Arable_avg"] = 0
    wdf.loc[wdf.Animal_Product != "Primary", "Pasture_avg"] = 0


    # land use calculations with arable_avg as redundant fallback
    wdf["WWF_derived_yield"] = 1/(wdf.Arable_avg) # convert from ha/kg to kg/m2
    wdf.loc[wdf.Yield.isna(), "Yield"] = wdf.loc[wdf.Yield.isna(), "WWF_derived_yield"]
    wdf = wdf.drop(columns=["WWF_derived_yield", "Arable_avg"])
    wdf["FAO_land_calc_m2"] = (wdf.provenance * 1000) / wdf.Yield


    # calculate impacts
    impact_list = ["SWWU_avg", "GHG_avg", "Pasture_avg"]
    for impact in impact_list:
        wdf[impact + "_calc"] = wdf[impact] * (wdf.provenance * 1000) # impact per kg
    wdf = wdf.drop(columns=impact_list)


    # error propogation
    wdf["err"] = (wdf.provenance_err / wdf.provenance)
    wdf["FAO_land_calc_m2_err"] = wdf["FAO_land_calc_m2"] * wdf["err"]
    wdf["SWWU_avg_calc_err"] = wdf["SWWU_avg_calc"] * wdf["err"]
    wdf["GHG_avg_calc_err"] = wdf["GHG_avg_calc"] * wdf["err"]
    wdf["Pasture_avg_calc_err"] = wdf["Pasture_avg_calc"] * wdf["err"]
    wdf = wdf.drop(columns=["err"])

    # biodiversity opportunity cost
    # bd_path = f"{datPath}/LIFE_results_SPAM_2020.csv"

    # bd_opp_cost = bd_opp_cost[bd_opp_cost.band_name=="all"]
    spam_years = os.listdir(os.path.join(datPath, "mapspam_outputs", "outputs"))
    spam_years = [int(yr) for yr in spam_years]
    next_year = min([yr for yr in spam_years if yr >= year], default=max(spam_years))
    bd_path = os.path.join(datPath, "mapspam_outputs", "outputs", str(next_year), f"processed_results_{next_year}.csv")

    bd_opp_cost = pd.read_csv(bd_path)
    bd_opp_cost.deltaE_mean *= -bd_opp_cost.sp_count
    bd_opp_cost.deltaE_mean_sem *= bd_opp_cost.sp_count
    commodity_crosswalk.SPAM_name_abr = commodity_crosswalk.SPAM_name_abr.str.upper()

    pasture = f"{datPath}/country_opp_cost_v6.csv"
    pasture = pd.read_csv(pasture, index_col = 0)
    

    # calculate fallback 2
    oc_past = pasture.past
    oc_past = oc_past[oc_past > 0]
    oc_past = np.exp(np.log(oc_past).mean())
    oc_past_err = pasture.past_err
    oc_past_err = oc_past_err[oc_past_err >0]
    oc_past_err = np.exp(np.log(oc_past_err).mean())

    oc_crop = bd_opp_cost[(bd_opp_cost.deltaE_mean > 0)].copy()
    oc_crop_pixels = oc_crop.pixel_count.sum()
    oc_crop["weighted_deltaE"] = oc_crop.deltaE_mean * oc_crop.pixel_count
    oc_crop = np.exp(np.log(oc_crop.weighted_deltaE).mean())/oc_crop_pixels

    oc_crop_err = bd_opp_cost[(bd_opp_cost.deltaE_mean_sem > 0)].copy()
    oc_crop_err_pixels = oc_crop_err.pixel_count.sum()
    oc_crop_err["weighted_deltaE_sem"] = oc_crop_err.deltaE_mean_sem * oc_crop_err.pixel_count
    oc_crop_err = np.exp(np.log(oc_crop_err.weighted_deltaE_sem).mean())/oc_crop_err_pixels
    


    # reshape bd_opp_cost for merging
    bd_opp_cost = bd_opp_cost[["ISO3", "item_name", "deltaE_mean", "deltaE_mean_sem"]]
    bd_opp_cost = bd_opp_cost.rename(columns={"ISO3":"Country_ISO", "item_name":"spam_name", "deltaE_mean":"opp_cost_val", "deltaE_mean_sem": "opp_cost_err"})

    # calculate global averages for fallback 1
    global_bd_opp_cost = pd.DataFrame()
    for v in bd_opp_cost.spam_name.dropna().unique():
        subset = bd_opp_cost[(bd_opp_cost.spam_name == v)&(bd_opp_cost.opp_cost_val>0)]["opp_cost_val"].dropna().values
        mean = np.exp(np.log(subset).mean())
        subset2 = bd_opp_cost[(bd_opp_cost.spam_name == v)&(bd_opp_cost.opp_cost_val>0)]["opp_cost_err"].dropna().values
        err = np.exp(np.log(subset2).mean())
        global_bd_opp_cost.loc[v, "opp_cost_val_fallback"] = mean
        global_bd_opp_cost.loc[v, "opp_cost_err_fallback"] = err

    animals = list(commodity_crosswalk.animal_bd_name.dropna().unique())
    animals_err = [f"{a}_err" for a in animals]

    pasture_opp_costs = pasture[animals]
    pasture_opp_costs = pasture_opp_costs.stack()
    pasture_opp_costs = pasture_opp_costs.reset_index()
    pasture_opp_costs = pasture_opp_costs.rename(columns={"level_0":"Country_ISO", "level_1":"spam_name", 0:"opp_cost_val"})

    pasture_opp_costs_err = pasture[animals_err]
    pasture_opp_costs_err = pasture_opp_costs_err.stack()
    pasture_opp_costs_err = pasture_opp_costs_err.reset_index()
    pasture_opp_costs_err = pasture_opp_costs_err.rename(columns={"level_0":"Country_ISO", "level_1":"spam_name", 0:"opp_cost_err"})
    pasture_opp_costs_err.spam_name = pasture_opp_costs_err.spam_name.replace({"_err":""}, regex=True)
    pasture_opp_costs = pasture_opp_costs.merge(pasture_opp_costs_err, on=["Country_ISO", "spam_name"])

    # get spam_name to merge with life data
    animal_df = wdf[wdf.Animal_Product == "Primary"].copy()
    crop_df = wdf[wdf.Animal_Product != "Primary"].copy()
    animal_df = animal_df.merge(commodity_crosswalk[["Item_Code", "animal_bd_name"]], on="Item_Code", how="left")
    animal_df = animal_df.rename(columns={"animal_bd_name":"spam_name"})
    crop_df = crop_df.merge(commodity_crosswalk[["Item_Code", "SPAM_name_abr"]], on="Item_Code", how="left")
    crop_df = crop_df.rename(columns={"SPAM_name_abr":"spam_name"})

    

    # merge in life data
    crop_df = crop_df.merge(bd_opp_cost, how="left", on=["Country_ISO", "spam_name"])
    animal_df = animal_df.merge(pasture_opp_costs, how="left", on=["Country_ISO", "spam_name"])
    wdf = pd.concat([crop_df, animal_df], axis=0)

    # fallback 1 (global item averages)
    wdf = wdf.merge(global_bd_opp_cost, how="left", left_on=["spam_name"], right_index=True)
    wdf.loc[wdf.opp_cost_val.isna(), "opp_cost_val"] = wdf.loc[wdf.opp_cost_val.isna(), "opp_cost_val_fallback"]
    wdf.loc[wdf.opp_cost_err.isna(), "opp_cost_err"] = wdf.loc[wdf.opp_cost_err.isna(), "opp_cost_err_fallback"]



    # fallback 2 (global type averages)
    wdf.loc[(wdf.opp_cost_val.isna())&(wdf.Animal_Product!="Primary"), "opp_cost_val"] = oc_crop
    wdf.loc[(wdf.opp_cost_val.isna())&(wdf.Animal_Product=="Primary"), "opp_cost_val"] = oc_past
    wdf.loc[(wdf.opp_cost_err.isna())&(wdf.Animal_Product!="Primary"), "opp_cost_err"] = oc_crop_err
    wdf.loc[(wdf.opp_cost_err.isna())&(wdf.Animal_Product=="Primary"), "opp_cost_err"] = oc_past_err
    wdf = wdf.drop(columns=["opp_cost_val_fallback", "opp_cost_err_fallback"])

    # convert opp cost from km2 to m2
    wdf["bd_opp_cost_m2"] = np.abs(wdf["opp_cost_val"] / 1000000)


    wdf.loc[wdf.Animal_Product=="Primary", "bd_val"] = wdf.loc[wdf.Animal_Product=="Primary", "Pasture_avg_calc"]
    wdf.loc[wdf.Animal_Product=="Primary", "bd_err"] = wdf.loc[wdf.Animal_Product=="Primary", "Pasture_avg_calc_err"]
    wdf.loc[wdf.Animal_Product!="Primary", "bd_val"] = wdf.loc[wdf.Animal_Product!="Primary", "FAO_land_calc_m2"]
    wdf.loc[wdf.Animal_Product!="Primary", "bd_err"] = wdf.loc[wdf.Animal_Product!="Primary", "FAO_land_calc_m2_err"]

    wdf["bd_opp_cost_calc"] = wdf["bd_val"] * wdf["bd_opp_cost_m2"]
    wdf["err"] = np.sqrt((wdf.opp_cost_err/wdf.opp_cost_val)**2 + (wdf.bd_err/wdf.bd_val)**2)
    wdf["bd_opp_cost_calc_err"] = wdf["bd_opp_cost_calc"] * wdf["err"]

    wdf.drop(columns=["bd_val", "bd_err", "err"], inplace=True)
    
    wdf.to_csv(f"{country_savefile_path}/{filename}")
    return wdf

if __name__ == "__main__":
    YEARS = [2019]
    COUNTRIES = ["GBR"]
    import os
    os.chdir("../")
    for year in YEARS:
        for country in COUNTRIES:
            print(f"Processing {country} for year {year}...")
            hc = pd.read_csv(f"results/{year}/{country}/human_consumed_no_sua.csv")
            get_impacts(hc, year, country, "human_consumed_impacts_wErr.csv")

