"""
Created on Wed Mar 15 16:41:04 2023

@author: Thomas Ball
@editor: Louis De Neve - edited to be vectorised and integrated into MRIO pipeline Nov 2025
"""

from pathlib import Path
import pandas as pd
import numpy as np
import os
import sys

from provenance._get_biodiversity_vals import fetch_biodiversity_vals_path

def get_wwf_pbd(datPath):
    file_name = "Planet-Based Diets - Data and Viewer.xlsx"
    sheet_name = "DATA - Product Level"
    file_path = f"{datPath}/{file_name}"
    if os.path.exists(file_path):
        
        with warnings.catch_warnings(): 
            warnings.simplefilter("ignore")
            df = pd.read_excel(file_path, sheet_name = sheet_name)
        return df
    else:
        sys.exit(f"""Couldn't find {file_name} in {datPath}""")
        return pd.DataFrame()
    
    
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
    wwf = get_wwf_pbd(datPath)
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
        rums = [867, 882, 947, 951, 977, 982, 1017, 1020, 1097]
        tb_pasture_vals = pd.read_csv(f"{results_dir}/{year}/.mrio/Pasture_calc.csv")[["Item_Code", "fp_m2_kg", "fp_m2_kg_perc", "Country_ISO"]]
        global_median_tb = {v: tb_pasture_vals[tb_pasture_vals["Item_Code"]==v]["fp_m2_kg"].median() for v in rums}
        global_median_tb_df = pd.DataFrame.from_dict(global_median_tb, orient='index', columns=['global_median_fp_m2_kg'])

        wdf = wdf.merge(tb_pasture_vals, how="left", on=["Country_ISO", "Item_Code"])
        wdf = wdf.merge(global_median_tb_df, how="left", left_on=["Item_Code"], right_index=True)
        wdf["Pasture_avg"] = wdf[["Pasture_avg","fp_m2_kg", "global_median_fp_m2_kg"]].min(axis=1)
        wdf = wdf.drop(columns=["fp_m2_kg", "global_median_fp_m2_kg"])

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

    # bd_path = os.path.join(datPath, "mapspam_outputs", "outputs", str(spam_yr), f"processed_results_{spam_yr}.csv")#
    bd_path, spam_yr = fetch_biodiversity_vals_path(year, datPath)

    bd_opp_cost = pd.read_csv(bd_path)

    bd_opp_cost = bd_opp_cost[bd_opp_cost.band_name=="all"]
    bd_opp_cost.deltaE_mean *= -bd_opp_cost.sp_count
    bd_opp_cost.deltaE_mean_sem *= bd_opp_cost.sp_count
    

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

    # get spam_name to merge with life data
    wdf = wdf.merge(commodity_crosswalk[["Item_Code", f"spam_{spam_yr}"]], on="Item_Code", how="left")
    wdf = wdf.rename(columns={f"spam_{spam_yr}":"spam_name"})


    # merge in life data
    wdf = wdf.merge(bd_opp_cost, how="left", on=["Country_ISO", "spam_name"])

    # fallback 1 (global item averages)
    wdf = wdf.merge(global_bd_opp_cost, how="left", left_on=["spam_name"], right_index=True)
    wdf.loc[wdf.opp_cost_val.isna(), "opp_cost_val"] = wdf.loc[wdf.opp_cost_val.isna(), "opp_cost_val_fallback"]
    wdf.loc[wdf.opp_cost_err.isna(), "opp_cost_err"] = wdf.loc[wdf.opp_cost_err.isna(), "opp_cost_err_fallback"]



    # fallback 2 (global type averages)
    wdf.loc[(wdf.opp_cost_val.isna()), "opp_cost_val"] = oc_crop
    wdf.loc[(wdf.opp_cost_err.isna()), "opp_cost_err"] = oc_crop_err
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
            hc = pd.read_csv(f"results/{year}/{country}/human_consumed.csv")
            get_impacts(hc, year, country, "human_consumed_impacts_wErr.csv")

