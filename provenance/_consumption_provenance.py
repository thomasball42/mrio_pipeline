# -*- coding: utf-8 -*-
"""
Created on Mon Jul 25 16:55:34 2022

@author: Thomas Ball
"""
import pandas as pd
import numpy as np
import os
import time
    


def add_cols(indf, area_codes, item_codes):
    ac = area_codes[["ISO3", "FAOSTAT"]].rename(columns={"ISO3":"Country_ISO", "FAOSTAT":"Producer_Country_Code"})
    indf = indf.merge(ac, on="Producer_Country_Code", how="left")
    
    ic = item_codes[["Item Code", "Item"]].rename(columns={"Item Code":"Item_Code"})
    indf = indf.merge(ic, on="Item_Code", how="left")

    if "Animal_Product_Code" in indf.columns:
        ic = ic.rename(columns={"Item_Code":"Animal_Product_Code", "Item":"Animal_Product"})
        indf = indf.merge(ic, on="Animal_Product_Code", how="left")

    return indf


def calculate_conversion_factors(conversion_opt, content_factors, item_map_for_cf):
        """Calculate conversion factors from processed to primary items"""

        content_factors.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
        item_map_for_cf.rename(columns=lambda x: x.replace(" ", "_"), inplace=True)    
    
        # Calculate conversion factors
        if conversion_opt not in content_factors.columns:
                raise ValueError(f"Primary Conversion option ({conversion_opt}) not available")
                
        joined = item_map_for_cf.merge(
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

        joined["Conversion_factor"] = joined[f"{conversion_opt}_primary"] / joined[f"{conversion_opt}_processed"]
        joined.loc[~np.isfinite(joined["Conversion_factor"]), "Conversion_factor"] = 0
        joined = joined.dropna(subset=["Conversion_factor"])
        conversion_factors = joined[["FAO_code", "FAO_name_primary", "primary_item", "Conversion_factor"]]
        conversion_factors = conversion_factors.rename(columns={"primary_item":"primary_item_code",
                                           "FAO_name_primary":"primary_item",
                                           "Conversion_factor":"ratio"})
        conversion_factors.drop_duplicates(subset=["FAO_code"], inplace=True)
        return conversion_factors


def main(year, country_of_interest, sua, historic=""):

    
    datPath = "./input_data"
    trade_feed = f"./results/{year}/.mrio/TradeMatrixFeed_import_dry_matter.csv"
    trade_nofeed = f"./results/{year}/.mrio/TradeMatrix_import_dry_matter.csv"

    item_codes = pd.read_csv(f"{datPath}/SUA_Crops_Livestock_E_ItemCodes.csv", encoding = "latin-1", low_memory=False)

    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        area_codes = pd.read_excel(f"{datPath}/nocsDataExport_20251021-164754.xlsx", engine="openpyxl")  
        factors = pd.read_excel(f"{datPath}/content_factors_per_100g.xlsx", skiprows=1, engine="openpyxl")

    item_map = pd.read_csv(f"{datPath}/primary_item_map_feed.csv", encoding = "latin-1")
    weighing_factors = pd.read_csv(f"{datPath}/weighing_factors.csv", encoding = "latin-1")
    prov_mat_no_feed = pd.read_csv(trade_nofeed)
    prov_mat_feed = pd.read_csv(trade_feed)

    coi_code = area_codes[area_codes["ISO3"] == country_of_interest]["FAOSTAT"].values[0]

    item_codes.columns = [_.strip() for _ in item_codes.columns]
    add_palestine = pd.DataFrame({"ISO3":["PSE"], "FAOSTAT":[299]})
    area_codes = pd.concat([area_codes, add_palestine], ignore_index=True)
    ic = item_codes.rename(columns={"CPC Code":"Item Code (CPC)", "Item Code":"FAO_code", "Item":"item_name"})
    ic["Item Code (CPC)"] = ic["Item Code (CPC)"].astype("string")


    sua = sua[(sua["Area Code"]==coi_code)&(sua.Year == year)]
    
    if historic == "":
        sugar_cane = sua[(sua["Item Code"]==156)]
        sugar_beet = sua[(sua["Item Code"]==157)]
        palm_oil = sua[(sua["Item Code"]==257)]
        for sugar in [sugar_cane, sugar_beet, palm_oil]:
            sugar_p = sugar[sugar["Element"]=="Production"].Value.sum()
            sugar_i = sugar[sugar["Element"]=="Import quantity"].Value.sum()
            sugar_e = sugar[sugar["Element"]=="Export quantity"].Value.sum()
            sugar_l = sugar[sugar["Element"]=="Loss"].Value.sum()
            sugar_val = np.max([sugar_p + sugar_i - sugar_e - sugar_l, 0])
            new_entry = sugar.iloc[0].copy()
            new_entry['Element Code'] = 5141
            new_entry['Element'] = "Food supply quantity (tonnes)"
            new_entry['Value'] = sugar_val
            sua = pd.concat([sua, new_entry.to_frame().T], ignore_index=True)
        # print(sua[sua["Item Code"].isin([156,157])])
        fs = sua[sua["Element Code"]==5141].copy()
        t0 = time.perf_counter()
        fs["Item Code (CPC)"] = fs["Item Code (CPC)"].astype("string")
        t1 = time.perf_counter()
        # print(f"         Converted Item Code to string in {t1 - t0:.2f} seconds")
        fs = fs.merge(ic, on="Item Code (CPC)", how="left") 
        fs = fs.drop(columns=["Note"])

    else:
        if 511 in sua["Element Code"].unique():
            population = sua[(sua["Element Code"]==511)].Value.values[0]
            fs = sua[sua["Element Code"]==645].copy()
            fs["Value"] = fs["Value"] * population  # convert kg/capita/yr to tonnes by multiplying by population
            cb_conversion_map = pd.read_csv("./input_data/CB_code_FAO_code_for_conversion_factors.csv", encoding="Latin-1")
            fs = fs.merge(
                cb_conversion_map[["FAO_code", "CB_code"]],
                left_on="Item Code",
                right_on="CB_code",
                how="left")
            fs["FAO_code"] = fs["FAO_code"].fillna(fs["Item Code"])
            fs.drop(columns=["CB_code"], inplace=True)
            fs = fs.merge(ic[["FAO_code", "item_name"]], on="FAO_code", how="left")
        else:
            print(f"         No population data for ({area_codes[area_codes['ISO3'] == country_of_interest]['LIST NAME'].values[0]}) in  {year}")#
            return pd.DataFrame(), pd.DataFrame()
    if len(fs) == 0:
        print(f"         No food supply data for ({area_codes[area_codes['ISO3'] == country_of_interest]['LIST NAME'].values[0]}) in  {year}")
        return pd.DataFrame(), pd.DataFrame()


    country_savefile_path = f"./results/{year}/{country_of_interest}"
    if not os.path.isdir(country_savefile_path):
        os.makedirs(country_savefile_path)   



    

    # ERROR CALCULATION - commented out for now
    # fs = fs[(fs.Year <= year+2) & (fs.Year >= year-2)]
    # fserr = fserr[(fserr.Year <= year+2) & (fserr.Year >= year-2)]

    # means = fs.groupby(["Item", "Item Code"]).Value.mean().reset_index()
    # errs = fs.groupby(["Item", "Item Code"]).Value.sem().reset_index() # no groupings so this is just zero

    # for item in means.Item:
    #     fs.loc[fs.Item == item, "Value"] = means[means.Item==item].Value.values[0]
    #     fserr.loc[fserr.Item == item, "Value"] = errs[errs.Item==item].Value.values[0]

    

    


    fserr = fs.copy()

    imports_feed = prov_mat_feed[prov_mat_feed.Consumer_Country_Code == coi_code]
    imports_feed = add_cols(imports_feed, area_codes=area_codes, item_codes=item_codes)
    imports_feed = imports_feed[~imports_feed.Item.isna()]   
    imports_feed_crops = imports_feed[(imports_feed.Animal_Product.isna()) & (imports_feed.Value >= 0)]
    imports_feed_crops.loc[:, 'Animal_Product'] = ""

    imports_no_feed = prov_mat_no_feed[prov_mat_no_feed.Consumer_Country_Code == coi_code]
    imports_no_feed = add_cols(imports_no_feed, area_codes=area_codes, item_codes=item_codes)
    imports_no_feed = imports_no_feed[~imports_no_feed.Item.isna()]
    imports_no_feed = imports_no_feed[imports_no_feed.Item_Code.isin(imports_feed.Animal_Product_Code.unique())]
    imports_no_feed.loc[:, 'Animal_Product'] = "Primary"

    imports_total = pd.concat([imports_feed_crops, imports_no_feed])

    human_consumed_import_ratios = (imports_total
        .groupby(["Item_Code", "Animal_Product"])[imports_total.columns]
        .apply(lambda x: x.assign(Ratio=x["Value"] / x["Value"].sum()))
        .reset_index(drop=True)
        .drop(columns=["Value"]))
    
    human_consumed_import_ratios.loc[human_consumed_import_ratios.Animal_Product=="", "Animal_Product"] = np.nan



    df_hc, df_hc_err = fs.copy(), fserr.copy() # TODO remove copy
    cf = calculate_conversion_factors("dry_matter", factors.copy(), item_map.copy())
    df_hc = df_hc.merge(cf, on="FAO_code", how="left")
    df_hc_err = df_hc_err.merge(cf, on="FAO_code", how="left")
    df_hc=df_hc[df_hc["ratio"]!=0]

    df_hc.dropna(subset=["ratio"], inplace=True)
    
    df_hc["value_primary"] = df_hc.Value / df_hc.ratio
    df_hc_err["value_primary_err"] = df_hc_err["Value"]**2


    primary_consumption = (df_hc
        .groupby(["primary_item_code"])[df_hc.columns]
        .apply(lambda x: x.assign(value_primary=x["value_primary"].sum()))
        .reset_index(drop=True)
        [['primary_item_code', 'value_primary']]
        .drop_duplicates(subset=["primary_item_code"])
        .merge(
            item_codes[["Item Code", "Item"]].rename(columns={"Item Code":"primary_item_code", "Item":"item_name"}),
            on="primary_item_code",
            how="left"))
        
    df_hc_err = (df_hc_err
        .groupby(["primary_item_code"])[df_hc_err.columns]
        .apply(lambda x: x.assign(value_primary_err=np.sqrt(x["value_primary_err"].sum())))
        .reset_index(drop=True)
        [['primary_item_code', 'value_primary_err']]
        .drop_duplicates(subset=["primary_item_code"]))
    primary_consumption = primary_consumption.merge(df_hc_err, on="primary_item_code", how="left")

    
    # print("         Calculating human consumed provenance")
    cons_prov = (human_consumed_import_ratios
        .merge(primary_consumption, left_on="Item_Code", right_on="primary_item_code", how="left")
        .drop(columns=["primary_item_code", "item_name"]))
    cons_prov["provenance"] = cons_prov["Ratio"] * cons_prov["value_primary"]
    cons_prov["provenance_err"] = cons_prov.provenance * np.sqrt(1+(cons_prov.value_primary_err/cons_prov.value_primary)**2)
    cons_prov = (cons_prov
        .drop(columns=["value_primary", "value_primary_err"])
        .dropna(subset=["provenance"]))
    cons_prov["Value"] = cons_prov["Ratio"]


    # provenance of feed
    ##################################################
    # print("         Calculating feed provenance")


    primary_consumption_anim = primary_consumption[primary_consumption.primary_item_code.isin(weighing_factors.Item_Code)]   

    primary_consumption_anim = primary_consumption_anim.merge(weighing_factors, left_on="primary_item_code", right_on="Item_Code", how="left")
    primary_consumption_anim['value_primary'] = primary_consumption_anim['value_primary'] * primary_consumption_anim['Weighing factors']
    primary_consumption_anim['value_primary_err'] = primary_consumption_anim['value_primary_err'] * primary_consumption_anim['Weighing factors']
    primary_consumption_anim = primary_consumption_anim[primary_consumption_anim['Weighing factors'] > 0]
    primary_consumption_anim = primary_consumption_anim.drop(columns=["Item_Code", "Item", "Weighing factors"])
    
    prov_mat_feed.loc[prov_mat_feed["Animal_Product_Code"].isna(), "Animal_Product_Code"] = 0
    prov_mat_feed = prov_mat_feed[~(prov_mat_feed.Value.isna())&(prov_mat_feed.Value > 0)]
    prov_mat_feed = (prov_mat_feed
        .groupby(["Animal_Product_Code", "Consumer_Country_Code"])[prov_mat_feed.columns]
        .apply(lambda x: x.assign(prov_ratio=x["Value"] / x["Value"].sum()))
        .reset_index(drop=True))
    prov_mat_feed.loc[prov_mat_feed["Animal_Product_Code"]==0, "Animal_Product_Code"] = np.nan

    ##################################################

    animal_codes = primary_consumption_anim.primary_item_code.unique()
    sc2 = human_consumed_import_ratios[human_consumed_import_ratios.Item_Code.isin(animal_codes)]
    sc2 = sc2.merge(primary_consumption_anim, left_on="Item_Code", right_on="primary_item_code", how="left")
    sc2["cVal"] = sc2["Ratio"] * sc2["value_primary"]
    sc2["cVal_err"] = sc2["Ratio"] * sc2["value_primary_err"]
    sc2["valid"] = sc2.Item_Code.astype(int).astype(str) +"-"+ sc2.Producer_Country_Code.astype(int).astype(str)
    sc2["Animal_Product"] = sc2.item_name
    sc2 = sc2[["valid", "cVal", "cVal_err", "Animal_Product"]]
    prov_mat_feed = prov_mat_feed[~prov_mat_feed.Animal_Product_Code.isna()]
    prov_mat_feed["valid"] = prov_mat_feed.Animal_Product_Code.astype(int).astype(str) + "-" + prov_mat_feed.Consumer_Country_Code.astype(str)
    dfx = prov_mat_feed[prov_mat_feed["valid"].isin(sc2["valid"])]
    dfx = dfx.merge(sc2, on="valid", how="left")
    dfx["provenance"] = dfx.prov_ratio * dfx.cVal
    dfx.loc[dfx.cVal>0, "provenance_err"] = dfx.loc[dfx.cVal>0,"provenance"] * np.sqrt(1+(dfx.loc[dfx.cVal>0,"cVal_err"]/dfx.loc[dfx.cVal>0,"cVal"])**2)
    dfx.drop(columns=["valid", "cVal", "cVal_err"], inplace=True)
    dfx = dfx.merge(area_codes[["FAOSTAT", "ISO3"]].rename(columns={"ISO3":"Country_ISO", "FAOSTAT":"Producer_Country_Code"}), on="Producer_Country_Code", how="left")
    dfx = dfx.merge(item_codes[["Item Code", "Item"]].rename(columns={"Item Code":"Item_Code",}), on="Item_Code", how="left")
    feed_prov = dfx[(dfx.Value > 1E-8)&(dfx.provenance > 0)]


    cons_prov = cons_prov[(cons_prov.Ratio > 1E-8)&(cons_prov.provenance > 0)]
    cons_prov.to_csv(f"{country_savefile_path}/human_consumed.csv")
    feed_prov = feed_prov[(feed_prov.Value > 1E-8)&(feed_prov.provenance > 0)]
    feed_prov.to_csv(f"{country_savefile_path}/feed.csv")
    return cons_prov, feed_prov
