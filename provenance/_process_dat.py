
import pandas as pd
import numpy as np
import os
import warnings
import time

def main(year, coi_iso, bh, bf):

    datPath = "./input_data"
    scenPath = f"./results/{year}/{coi_iso}"

    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        country_code_data = pd.read_excel(f"{datPath}/nocsDataExport_20251021-164754.xlsx")
    coi = country_code_data.loc[country_code_data["ISO3"]==coi_iso]["FAOSTAT"].values[0]

    bd_path = f"{datPath}/country_opp_cost_v6.csv"
    grouping = "group_name_v7"
    
    # coi = 229

    cropdb = pd.read_csv(f"{datPath}/crop_db.csv")
    
    # bh = pd.read_csv(f"{scenPath}/human_consumed_impacts_wErr.csv", index_col = 0)
    # bf = pd.read_csv(f"{scenPath}/feed_impacts_wErr.csv", index_col = 0)
    # print(bf)

    bf["bd_opp_cost_calc"] = bf["bd_opp_cost_calc"].mask(bf["bd_opp_cost_calc"].lt(0),0)
    
    bh = bh[np.logical_not(np.isinf(bh.FAO_land_calc_m2))]
    bh["ItemT_Name"] = bh["Item"]
    bh["ItemT_Code"] = bh["Item_Code"]
    bh["Arable_m2"] = bh.FAO_land_calc_m2
    bh["Pasture_m2"] = bh.Pasture_avg_calc.fillna(0)
    bh["bd_perc_err"] = bh["bd_opp_cost_calc_err"] / bh["bd_opp_cost_calc"]
    
    bf = bf[np.logical_not(np.isinf(bf.FAO_land_calc_m2))]
    bf["ItemT_Code"] = bf["Animal_Product_Code"]
    bf["ItemT_Name"] = bf["Animal_Product"]
    bf["Arable_m2"] = bf.FAO_land_calc_m2
    bf["Pasture_m2"] = 0
    bf["bd_perc_err"] = bf["bd_opp_cost_calc_err"] / bf["bd_opp_cost_calc"]
    bf = bf[~np.isinf(bf.bd_perc_err)]
    xdf = pd.concat([bh,bf])
    

    lookup = xdf[["ItemT_Code", "ItemT_Name"]].drop_duplicates()


    xdfs_uk = xdf[xdf.Producer_Country_Code == coi]
    xdfs_os = xdf[~(xdf.Producer_Country_Code == coi)]
    xdfs_uk = xdfs_uk[["Pasture_m2", "Arable_m2", "SWWU_avg_calc", "ItemT_Name", "ItemT_Code", "provenance"]]
    xdfs_os = xdfs_os[["Pasture_m2", "Arable_m2", "SWWU_avg_calc", "ItemT_Name", "ItemT_Code", "provenance"]]
    
    
    xdfs_uk = xdfs_uk.groupby("ItemT_Name").sum()
    xdfs_os = xdfs_os.groupby("ItemT_Name").sum()


    df_uk = pd.DataFrame()
    missing_items = []
    for item in xdfs_uk.index.tolist():
        x = xdfs_uk.loc[item]
        try:
            item_code = lookup[lookup.ItemT_Name == item].ItemT_Code.values[0]
            df_uk.loc[item, "Group"] = cropdb[cropdb.Item_Code == item_code][grouping].values[0]
            df_uk.loc[item, "tonnage"] = x.provenance
            df_uk.loc[item, "Pasture_m2"] = x.Pasture_m2
            df_uk.loc[item, "Arable_m2"] = x.Arable_m2
            df_uk.loc[item, "Scarcity_weighted_water_l"] = x.SWWU_avg_calc.sum()
            df_uk.loc[item, "ghg_food"] = bh[(bh.Item == item)&(bh.Producer_Country_Code == coi)].GHG_avg_calc.sum()
            df_uk.loc[item, "ghg_feed"] = bf[(bf.Animal_Product == item)&(bf.Producer_Country_Code == coi)].GHG_avg_calc.sum()
            df_uk.loc[item, "ghg_total"] =  df_uk.loc[item, "ghg_feed"] + df_uk.loc[item, "ghg_food"]
            df_uk.loc[item, "bd_opp_food"] = bh[(bh.Item == item)&(bh.Producer_Country_Code == coi)]["bd_opp_cost_calc"].sum()
            df_uk.loc[item, "bd_opp_feed"] = bf[(bf.Animal_Product == item)&(bf.Producer_Country_Code == coi)]["bd_opp_cost_calc"].sum()
            df_uk.loc[item, "bd_opp_total"] = df_uk.loc[item, "bd_opp_feed"] + df_uk.loc[item, "bd_opp_food"]
            
            # bd opp food err
            df_uk.loc[item, "bd_opp_food_err"] = df_uk.loc[item, "bd_opp_food"] \
                * np.sqrt(np.nansum(np.array(bh[(bh.Item==item)&(bh.Producer_Country_Code==coi)].bd_perc_err) ** 2))
            # bd opp feed err
            df_uk.loc[item, "bd_opp_feed_err"] = df_uk.loc[item, "bd_opp_feed"] \
                * np.sqrt(
                    np.nansum(np.array(bf[(bf.Animal_Product==item)&(bf.Producer_Country_Code==coi)].bd_perc_err) ** 2)
                    )
            # bd opp total error
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                fe_err = df_uk.loc[item, "bd_opp_feed_err"]/df_uk.loc[item, "bd_opp_feed"]
                fo_err = df_uk.loc[item, "bd_opp_food_err"]/df_uk.loc[item, "bd_opp_food"]
            
            df_uk.loc[item, "bd_opp_total_err"] = df_uk.loc[item, "bd_opp_total"] \
                * np.sqrt(np.nansum(np.nansum([(fe_err)**2,(fo_err)**2])))
            
            
            df_uk.loc[item, "Cons"] = bh[(bh.Item == item)&(bh.Producer_Country_Code == coi)].provenance.sum()
            df_uk.loc[item, "Cons_err"] = np.sqrt(np.nansum(bh[(bh.Item == item)&(bh.Producer_Country_Code == coi)].provenance_err ** 2))
            
        except IndexError:
            item_code = lookup[lookup.ItemT_Name == item].ItemT_Code.values[0]
            missing_items.append((item, item_code))
    
    df_os = pd.DataFrame()
    for item in xdfs_os.index.tolist():
        x = xdfs_os.loc[item]
        try:
            item_code = lookup[lookup.ItemT_Name == item].ItemT_Code.values[0]
            df_os.loc[item, "Group"] = cropdb[cropdb.Item_Code == item_code][grouping].values[0]
            df_os.loc[item, "tonnage"] = x.provenance
            df_os.loc[item, "Pasture_m2"] = x.Pasture_m2
            df_os.loc[item, "Arable_m2"] = x.Arable_m2
            df_os.loc[item, "Scarcity_weighted_water_l"] = x.SWWU_avg_calc.sum()
            df_os.loc[item, "ghg_food"] = bh[(bh.Item == item)&(bh.Producer_Country_Code != coi)].GHG_avg_calc.sum()
            df_os.loc[item, "ghg_feed"] = bf[(bf.Animal_Product == item)&(bf.Producer_Country_Code != coi)].GHG_avg_calc.sum()
            df_os.loc[item, "ghg_total"] =  df_os.loc[item, "ghg_feed"] + df_os.loc[item, "ghg_food"]
            df_os.loc[item, "bd_opp_food"] = bh[(bh.Item == item)&(bh.Producer_Country_Code != coi)]["bd_opp_cost_calc"].sum()
            df_os.loc[item, "bd_opp_feed"] = bf[(bf.Animal_Product == item)&(bf.Producer_Country_Code != coi)]["bd_opp_cost_calc"].sum()
            df_os.loc[item, "bd_opp_total"] = df_os.loc[item, "bd_opp_feed"] + df_os.loc[item, "bd_opp_food"]
            # if item not in df_uk.index:
            df_os.loc[item, "Cons"] = bh[(bh.Item == item)&(bh.Producer_Country_Code !=coi)].provenance.sum()
            df_os.loc[item, "Cons_err"] = np.sqrt(np.nansum(bh[(bh.Item == item)&(bh.Producer_Country_Code !=coi)].provenance_err ** 2))
            
            # bd opp food err
            df_os.loc[item, "bd_opp_food_err"] = df_os.loc[item, "bd_opp_food"] \
                * np.sqrt(np.nansum(np.array(bh[(bh.Item==item)&(bh.Producer_Country_Code!=coi)].bd_perc_err) ** 2))
            # bd opp feed err
            df_os.loc[item, "bd_opp_feed_err"] = df_os.loc[item, "bd_opp_feed"] \
                * np.sqrt(
                    np.nansum(np.array(bf[(bf.Animal_Product==item)&(bf.Producer_Country_Code!=coi)].bd_perc_err) ** 2)
                    )
            # bd opp total error
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                fe_err = df_os.loc[item, "bd_opp_feed_err"]/df_os.loc[item, "bd_opp_feed"]
                fo_err = df_os.loc[item, "bd_opp_food_err"]/df_os.loc[item, "bd_opp_food"]
            
            df_os.loc[item, "bd_opp_total_err"] = df_os.loc[item, "bd_opp_total"] \
                * np.sqrt(np.nansum(np.nansum([(fe_err)**2,(fo_err)**2])))
    
        except IndexError:
            item_code = lookup[lookup.ItemT_Name == item].ItemT_Code.values[0]
            missing_items.append((item, item_code))
            



    kdf = pd.concat([df_uk,df_os])
    # print(kdf.index.tolist())
    # print(kdf.loc["Oil palm fruit"])

    kdf = kdf.groupby([kdf.index, "Group"]).sum().reset_index()

    # print(df_uk, df_os)


    df_uk.to_csv(f"{scenPath}/df_{coi_iso.lower()}.csv")
    df_os.to_csv(f"{scenPath}/df_os.csv")
    xdf.to_csv(f"{scenPath}/impacts_full.csv") # rename to impacts_aggregated
    
    if "Item" not in kdf.columns:
        
        kdf.columns = [_ if _ != "level_0" else "Item" for _ in kdf.columns]
    
    kdf.to_csv(f"{scenPath}/impacts_aggregated.csv")
    
    food_commodity_impacts = kdf[["Item", "tonnage", "ghg_total", "bd_opp_total", "bd_opp_total_err", "Scarcity_weighted_water_l"]].copy()
    food_commodity_impacts["kgCO2_per_kg"] = food_commodity_impacts.ghg_total / (food_commodity_impacts.tonnage * 1000)
    food_commodity_impacts["exp_extinctions_per_kg"] = food_commodity_impacts.bd_opp_total / (food_commodity_impacts.tonnage * 1000)
    food_commodity_impacts["exp_extinctions_err_per_kg"] = food_commodity_impacts.bd_opp_total_err / (food_commodity_impacts.tonnage * 1000)
    food_commodity_impacts["scarcity_weighted_water_use_litres_per_kg"] = food_commodity_impacts.Scarcity_weighted_water_l / (food_commodity_impacts.tonnage * 1000)

    food_commodity_impacts = food_commodity_impacts.drop(columns=["ghg_total", "bd_opp_total", "Scarcity_weighted_water_l"])
    last_row = food_commodity_impacts.iloc[-1].copy()
    last_row.iloc[1:] = 0
    last_row.iloc[0] = "Zero"
    food_commodity_impacts = pd.concat([food_commodity_impacts, last_row.to_frame().T], ignore_index=True)

    old_to_new = pd.read_csv(f"{datPath}/composition_old_vs_new.csv")
    old_to_new = old_to_new.merge(food_commodity_impacts, left_on="New", right_on="Item", how="left")
    old_to_new.drop(columns=["Item", "New", "tonnage"], inplace=True)
    old_to_new.rename(columns={"Old":""}, inplace=True)
    old_to_new.to_csv(f"{scenPath}/food_commodity_impacts.csv", index=False)

    return missing_items

    
if __name__ == "__main__":
    

    datPath = "dat"
    # scenPath = os.path.join(odPath, "Work\\Work for others\\Catherine CLR\\food_results\\gbr")

