# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 16:41:04 2023

@author: tom
"""
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
try:
    import provenance.data_utils as data_utils
except ModuleNotFoundError:
    import data_utils
import os
from tqdm import tqdm

#%%
def get_impacts(wdf, year, coi, filename):

    # year=2013
    # coi = "AFG"
    # filename = "feed_impacts_wErr"
    #wdf = pd.read_csv(f"./results/{year}/{coi}/feed.csv")



    country_savefile_path = f"./results/{year}/{coi}"
    datPath = "./input_data"

    bd_path = f"{datPath}/country_opp_cost_v6.csv"
    rums = {"Meat; cattle" : "bvmeat",
            'Meat; sheep' : "sgmeat",
            'Meat; goat' : "sgmeat",
            'Milk; whole fresh cow' : "bvmilk",
            }   
    tb_pasture_vals = pd.read_csv(f"{datPath}/tb_pasture_factors_2.csv", index_col = 0)
    
    bd_opp_cost = pd.read_csv(bd_path, index_col = 0)
    
    wdf = wdf[np.logical_not(wdf.Item.isna())]
    wdf = wdf[wdf.Value >= 0.015]
    # area_codes = data_utils.get_area_codes(datPath)
    # item_codes = data_utils.get_item_codes(datPath)
    # code_list = data_utils.fbs_sua_item_codes(datPath)
    # coi_code = area_codes[area_codes["ISO3"] == coi][
    #     "FAOSTAT"].values[0]
    wwf = data_utils.get_wwf_pbd(datPath)
    Sm_wwf_items = pd.read_csv(f"{datPath}/schwarzmueller_wwf.csv",index_col = 0)

    fao_prod = pd.read_csv(f"{datPath}/Production_Crops_Livestock_E_All_Data_(Normalized).csv", encoding = "latin-1", low_memory=False)
    fao_prod = fao_prod[fao_prod.Year == year]
    yield_dat = fao_prod[fao_prod.Element == "Yield"]
    commodity_crosswalk = pd.read_csv(f"{datPath}/commodity_crosswalk.csv", index_col = 0)

    wdf = (wdf
        .merge(Sm_wwf_items[["Item_Code_FAO", "WWF_cat"]], left_on="Item_Code", right_on="Item_Code_FAO", how="left")
        .drop(columns=["Item_Code_FAO"]))


    oc_past = bd_opp_cost.past
    oc_past = oc_past[oc_past >0]
    oc_past = np.exp(np.log(oc_past).mean())
    oc_past_err = bd_opp_cost.past_err
    oc_past_err = oc_past_err[oc_past_err >0]
    oc_past_err = np.exp(np.log(oc_past_err).mean())
    oc_crop = bd_opp_cost.crop
    oc_crop = oc_crop[oc_crop >0]
    oc_crop = np.exp(np.log(oc_crop).mean())
    oc_crop_err = bd_opp_cost.crop_err
    oc_crop_err = oc_crop_err[oc_crop_err >0]
    oc_crop_err = np.exp(np.log(oc_crop_err).mean())




    for row in tqdm(wdf.iterrows()):
        
        idx = row[0]
        row = row[1]
        producer_iso = row.Country_ISO
        producer_code = row.Producer_Country_Code
        item_name = row.Item
        item_code = row.Item_Code
        provenance_val = row.provenance
        provenance_err = row.provenance_err
        # provenance_err = 0
        is_animal = (row.Animal_Product == "Primary")
        wwf_name = row.WWF_cat
        
        impacts = wwf[(wwf.Country_ISO==producer_iso)
                      &(wwf.Product==wwf_name)]

        # do seperate yield and LU calc
        item_yields = yield_dat[yield_dat["Item Code"] == item_code]
        if len(item_yields) == 0:
            pass
        elif is_animal == True:
            pass
        else:
            country_yield = item_yields[item_yields["Area Code"] == producer_code]
            if len(country_yield) == 0:
                wdf.loc[idx, "FAO_yield_kgm2"] = item_yields[
                    item_yields["Area Code"] == 5000].Value.values[0]*0.1/10000 
            else:
                wdf.loc[idx, "FAO_yield_kgm2"] = country_yield.Value.values[0]\
                    *0.1/10000
            yield_val = wdf.loc[idx, "FAO_yield_kgm2"]
            yield_err_perc = 0.0
            land_val = provenance_val * 1000 * (1/yield_val) # tonnes -> kg per m2
            land_val_err = land_val*np.sqrt((provenance_err/provenance_val)**2+yield_err_perc**2) 
            # land_val_err = 0
            wdf.loc[idx, "FAO_land_calc_m2"] = land_val
            wdf.loc[idx, "FAO_land_calc_m2_err"] = land_val_err

        # if there is no impact data, use the world value instead
        if impacts.size == 0:
            # print("No impact data for", item_name, item_code, "in", producer_iso,)
            impacts = wwf[(wwf.Country_ISO=="all-r")
                          &(wwf.Product==wwf_name)]
            # print(wwf.Country_ISO.unique(), wwf_name)
        for impact in impacts.columns:
            print(impacts.columns)
            impact_val = impacts[impact].values[0]
            impact_val_percerr = 0
            if "avg" in impact:
                try:
                    wdf.loc[idx, impact] = impact_val
                    if is_animal == False:
                        if impact == "Pasture_avg" \
                            or impact == "BD_pasture_avg_m2":
                            impact_val = 0
                    elif is_animal == True:
                        if impact == "Arable_avg" \
                            or impact == "BD_arable_avg_m2":
                            impact_val == 0
                        # change pasture vals to my own calculations:        
                        if impact == "Pasture_avg" and item_name in rums.keys():
                            producer_past_val = tb_pasture_vals[(tb_pasture_vals.Country_ISO==producer_iso)\
                                                                &(tb_pasture_vals.livestock==rums[item_name])]
                            if len(producer_past_val) > 0:
                                if producer_past_val.fp_m2_kg.values[0] < impact_val:
                                    impact_val = producer_past_val.fp_m2_kg.values[0]
                                impact_val_percerr = producer_past_val.fp_m2_kg_perc.values[0]
                                impact_val_percerr = 0
                            else:
                                if tb_pasture_vals[tb_pasture_vals.livestock==rums[item_name]].fp_m2_kg.median() < impact_val:
                                    impact_val = tb_pasture_vals[tb_pasture_vals.livestock==rums[item_name]].fp_m2_kg.median()
                                    impact_val_percerr = tb_pasture_vals[tb_pasture_vals.livestock==rums[item_name]]
                                    impact_val_percerr = 0
                            
                    wdf.loc[idx, impact+ "_calc"] = impact_val \
                        * (provenance_val * 1000) # impact per kg
                    wdf.loc[idx, impact+"_calc_err"] = wdf.loc[idx, impact+ "_calc"]\
                        * np.sqrt(impact_val_percerr**2+((provenance_err/provenance_val)**2))
                except TypeError:
                    pass
        if wdf.loc[idx, "FAO_yield_kgm2"] == 0:
            wdf.loc[idx, "FAO_land_calc_m2"] = wdf.loc[idx, "Arable_avg"+ "_calc"] = impacts[
                "Arable_avg"].values[0] * (provenance_val * 1000)
        # do biodiversity

        gz_name = 0
        opp_cost_err = oc_crop_err
        if is_animal == True:
            gz_name = commodity_crosswalk[commodity_crosswalk.Item_Code == item_code].animal_bd_name.squeeze()
        else: 
            gz_name = commodity_crosswalk[commodity_crosswalk.Item_Code == item_code].GAEZres06.squeeze()
        if len(gz_name) == 0:
            opp_cost_val = np.sqrt(oc_crop * oc_past)
        else:
            comm_vals = bd_opp_cost[gz_name]
            comm_err = bd_opp_cost[gz_name + "_err"]
            try:
                try:
                    opp_cost_val = comm_vals.loc[producer_iso].squeeze()
                    opp_cost_err = comm_err.loc[producer_iso].squeeze()
                except AttributeError:
                    opp_cost_val = comm_vals.loc[producer_iso]
                    opp_cost_err = comm_err.loc[producer_iso]
            except KeyError:
                try:
                    try:
                        opp_cost_val = np.exp(np.log(comm_vals[
                            comm_vals > 0].dropna()).mean()).squeeze()
                        opp_cost_err = np.exp(np.log(comm_err[
                            comm_err > 0].dropna()).mean()).squeeze()
                    except AttributeError:
                        opp_cost_val = np.exp(np.log(comm_vals[
                            comm_vals > 0].dropna()).mean())
                        opp_cost_err = np.exp(np.log(comm_err[
                            comm_err > 0].dropna()).mean())
                except KeyError:
                    if is_animal == True:
                        opp_cost_val = oc_past
                        opp_cost_err = oc_past_err
                    else:
                        opp_cost_val = oc_crop
                        opp_cost_err = oc_crop_err

        if isinstance(opp_cost_val, pd.Series):
            if len(opp_cost_val.unique()) > 1:
                print(f"Multiple values for {item_code} in {producer_iso}")
                continue
            else:
                opp_cost_val = opp_cost_val.unique().squeeze()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            wdf.loc[idx, "bd_opp_cost_m2"] = opp_cost_val / 1000000 # convert to per m2
            if is_animal == True:
                
                past_calc = wdf.loc[idx, "Pasture_avg_calc"]
                past_calc_err = wdf.loc[idx, "Pasture_avg_calc_err"]
                
                wdf.loc[idx, "bd_opp_cost_calc"] = wdf.loc[idx, "bd_opp_cost_m2"] * wdf.loc[idx,"Pasture_avg_calc"]
                err = wdf.loc[idx,"bd_opp_cost_calc"]*\
                    np.sqrt((opp_cost_err/opp_cost_val)**2+(past_calc_err/past_calc)**2)
                wdf.loc[idx,"bd_opp_cost_calc_err"]= err
            else:
                fao_land_calc = wdf.loc[idx, "FAO_land_calc_m2"]
                fao_land_calc_err = wdf.loc[idx, "FAO_land_calc_m2_err"]
                
                wdf.loc[idx, "bd_opp_cost_calc"] = wdf.loc[idx, "bd_opp_cost_m2"] * fao_land_calc
            
                err = wdf.loc[idx,"bd_opp_cost_calc"]*np.sqrt((
                    opp_cost_err/opp_cost_val)**2+(
                        fao_land_calc_err/fao_land_calc)**2)

                if isinstance(err, pd.Series):
                    if len(err.unique()) > 1:
                        err = np.nan
                    else:
                        err = err.unique().squeeze()

                wdf.loc[idx,"bd_opp_cost_calc_err"] = err
    wdf.to_csv(f"{country_savefile_path}/{filename}")
