# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 16:41:04 2023

@author: tom
"""

import pandas as pd
import numpy as np

try:
    import provenance.data_utils as data_utils
except ModuleNotFoundError:
    import data_utils


def get_impacts(wdf, year, coi, filename):
    # setup
    country_savefile_path = f"./results/{year}/{coi}"
    datPath = "./input_data"

    
    # trim input data to significant values
    wdf = wdf[np.logical_not(wdf.Item.isna())]
    # wdf = wdf[wdf.Value >= 0.015]

    # load additional data and merge into wdf
    crop_database = pd.read_csv(f"{datPath}/crop_db.csv", index_col = 0)
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
    bd_path = f"{datPath}/country_opp_cost_v6.csv"
    bd_opp_cost = pd.read_csv(bd_path, index_col = 0)


    # calculate fallback 2
    oc_past = bd_opp_cost.past
    oc_past = oc_past[oc_past > 0]
    oc_past = np.exp(np.log(oc_past).mean())
    oc_past_err = bd_opp_cost.past_err
    oc_past_err = oc_past_err[oc_past_err >0]
    oc_past_err = np.exp(np.log(oc_past_err).mean())
    oc_crop = bd_opp_cost.crop
    oc_crop = oc_crop[oc_crop > 0]
    oc_crop = np.exp(np.log(oc_crop).mean())
    oc_crop_err = bd_opp_cost.crop_err
    oc_crop_err = oc_crop_err[oc_crop_err >0]
    oc_crop_err = np.exp(np.log(oc_crop_err).mean())


    # reshape bd_opp_cost for merging
    bd_opp_cost = bd_opp_cost.stack()
    bd_opp_cost = bd_opp_cost.reset_index()
    bd_opp_cost = bd_opp_cost.rename(columns={"level_0":"Country_ISO", "level_1":"gz_name", 0:"opp_cost_val"})


    # calculate global averages for fallback 1
    global_bd_opp_cost = pd.DataFrame() 
    global_bd_opp_cost_err = pd.DataFrame() 
    for v in bd_opp_cost.gz_name.unique():
        subset = bd_opp_cost[(bd_opp_cost.gz_name == v)&(bd_opp_cost.opp_cost_val>0)]["opp_cost_val"].dropna().values
        mean = np.exp(np.log(subset).mean())
        if v[-3:] == "err":
            global_bd_opp_cost_err.loc[v, "opp_cost_err_fallback"] = mean
        else:
            global_bd_opp_cost.loc[v, "opp_cost_val_fallback"] = mean


    # get gz_name to merge with life data
    animal_df = wdf[wdf.Animal_Product == "Primary"].copy()
    crop_df = wdf[wdf.Animal_Product != "Primary"].copy()
    animal_df = animal_df.merge(crop_database[["Item_Code", "animal_bd_name"]], on="Item_Code", how="left")
    animal_df = animal_df.rename(columns={"animal_bd_name":"gz_name"})
    crop_df = crop_df.merge(crop_database[["Item_Code", "GAEZres06"]], on="Item_Code", how="left")
    crop_df = crop_df.rename(columns={"GAEZres06":"gz_name"})
    wdf = pd.concat([animal_df, crop_df], axis=0)
    wdf["gz_name_err"] = wdf["gz_name"] + "_err"


    # merge in life data
    wdf = wdf.merge(bd_opp_cost, how="left", on=["Country_ISO", "gz_name"])
    bd_opp_cost = bd_opp_cost.rename(columns={"opp_cost_val":"opp_cost_err", "gz_name":"gz_name_err"})
    wdf = wdf.merge(bd_opp_cost, how="left", on=["Country_ISO", "gz_name_err"])


    # fallback 1 (global item averages)
    wdf = wdf.merge(global_bd_opp_cost, how="left", left_on=["gz_name"], right_index=True)
    wdf = wdf.merge(global_bd_opp_cost_err, how="left", left_on=["gz_name_err"], right_index=True)
    wdf.loc[wdf.opp_cost_val.isna(), "opp_cost_val"] = wdf.loc[wdf.opp_cost_val.isna(), "opp_cost_val_fallback"]
    wdf.loc[wdf.opp_cost_err.isna(), "opp_cost_err"] = wdf.loc[wdf.opp_cost_err.isna(), "opp_cost_err_fallback"]


    # fallback 2 (global type averages)
    wdf.loc[(wdf.opp_cost_val.isna())&(wdf.Animal_Product!="Primary"), "opp_cost_val"] = oc_crop
    wdf.loc[(wdf.opp_cost_val.isna())&(wdf.Animal_Product=="Primary"), "opp_cost_val"] = oc_past
    wdf.loc[(wdf.opp_cost_err.isna())&(wdf.Animal_Product!="Primary"), "opp_cost_err"] = oc_crop_err
    wdf.loc[(wdf.opp_cost_err.isna())&(wdf.Animal_Product=="Primary"), "opp_cost_err"] = oc_past_err
    wdf = wdf.drop(columns=["gz_name", "gz_name_err", "opp_cost_val_fallback", "opp_cost_err_fallback"])

    # convert opp cost from km2 to m2
    wdf["bd_opp_cost_m2"] = wdf["opp_cost_val"] / 1000000



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

