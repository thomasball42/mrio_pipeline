import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns

results_dir = "../results"
year=2013

df = pd.DataFrame()
commodity_crosswalk = pd.read_csv("../input_data/commodity_crosswalk.csv")
item_codes = pd.read_csv("../input_data/SUA_Crops_Livestock_E_ItemCodes.csv")[[" Item Code", " Item"]].rename(columns={" Item Code": "Item_Code", " Item": "Item"})


colourdict =  {
    'Ruminant meat' : "#C90D75",
    'Pig meat'       : "#D64A98",
    'Poultry meat'   : "#D880B1",
    'Dairy'          : "#F7BDDD",
    'Eggs'           : "#FFEDF7",
    
    'Grains'             : "#D55E00",
    "Rice"               : "#D88E53",
    "Soybeans"           : "#DCBA9E",
    
    'Roots and tubers'   : "#0072B2",
    'Vegetables'         : "#4F98C1",
    'Legumes and pulses' : "#9EBFD2",
    
    'Bananas'           : "#FFED00",
    'Tropical fruit'    : "#FFF357",
    'Temperate fruit'   : "#FDF8B9",
    'Tropical nuts'     : "#27E2FF",
    'Temperate nuts'    : "#7DEEFF",
    
    'Sugar beet'    : "#FFC000",
    'Sugar cane'    : "#F7C93B",
    'Spices'        : "#009E73",
    'Coffee'        : "#33CCA2",
    'Cocoa'         : "#62DEBC",
    "Tea and maté"  : "#A2F5DE",
    
    "Oilcrops" : "#000000",
    "Other" : "#A2A2A2"}

def invert_color(hex_color):
    # Remove '#' if present
    hex_color = hex_color.lstrip('#')
    
    # Convert hex to RGB
    red = int(hex_color[0:2], 16)
    green = int(hex_color[2:4], 16)
    blue = int(hex_color[4:6], 16)
    
    # Invert RGB
    inverted_red = 255 - red
    inverted_green = 255 - green
    inverted_blue = 255 - blue
    
    # Convert back to hex
    inverted_hex = '#{0:02x}{1:02x}{2:02x}'.format(inverted_red, inverted_green, inverted_blue)
    
    return inverted_hex

fao_prod = pd.read_csv(f"../input_data/Production_Crops_Livestock_E_All_Data_(Normalized).csv", encoding = "latin-1", low_memory=False)
fao_prod = fao_prod[(fao_prod.Year == year)&(fao_prod["Element Code"]==5510)][["Area Code", "Item Code", "Value"]]
fao_prod = fao_prod.rename(columns={"Area Code":"Country_Code", "Item Code":"Item_Code", "Value":"Cons"})

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    area_codes = pd.read_excel(f"../input_data/nocsDataExport_20251021-164754.xlsx", engine="openpyxl")  
    area_codes = area_codes[["ISO3", "FAOSTAT"]].rename(columns={"ISO3":"Country_ISO", "FAOSTAT":"Country_Code"})
fao_prod = fao_prod.merge(area_codes, on="Country_Code", how="left")

print(fao_prod)

for country_iso in os.listdir(f"{results_dir}/{year}"):
    if len(country_iso) != 3 or not os.path.exists(f"{results_dir}/{year}/{country_iso}/df_{country_iso.lower()}.csv"):
        continue
    country_df = pd.read_csv(f"{results_dir}/{year}/{country_iso}/df_{country_iso.lower()}.csv")
    country_df = country_df.rename(columns={"Unnamed: 0": "Item"})
    country_df = country_df[["Item", "bd_opp_total"]].merge(item_codes, on="Item", how="left")
    country_df = country_df.merge(fao_prod[fao_prod.Country_ISO == country_iso][["Item_Code", "Cons"]], on="Item_Code", how="left")
    country_df = country_df.merge(commodity_crosswalk[["Item_Code", "group_name_v6"]], on="Item_Code", how="left")
    country_df = country_df.drop(columns=["Item"])
    country_df = country_df.groupby("group_name_v6").sum().reset_index()
    country_df["Impact_per_kg"] = country_df["bd_opp_total"] / (country_df["Cons"]*1000)
    country_df = country_df.drop(columns=["bd_opp_total"])
    df = pd.concat([df, country_df], ignore_index=True)

group_df = pd.DataFrame(columns=["group_name_v6", "median_impact", "q10", "q90", "colour", "count"])

for group in df["group_name_v6"].unique():
    group_data = df[df["group_name_v6"] == group]
    group_data = group_data.dropna(subset=["Impact_per_kg"])
    occurences = group_data.shape[0]
    median_impact = np.percentile(a=group_data["Impact_per_kg"], q=50, weights=group_data["Cons"], method="inverted_cdf")
    q10 = np.percentile(a=group_data["Impact_per_kg"], q=10, weights=group_data["Cons"], method="inverted_cdf")
    q90 = np.percentile(a=group_data["Impact_per_kg"], q=90, weights=group_data["Cons"], method="inverted_cdf")
    colour = colourdict.get(group, "#A2A2A2")
    group_df.loc[len(group_df)] = [group, median_impact, q10, q90, colour, occurences]


group_df = group_df.sort_values(by="median_impact", ignore_index=True)
group_df["range"] = group_df["q90"] - group_df["q10"]

fig, ax = plt.subplots(figsize=(8,7))
for i in group_df.index:
    group, median, q10, q90, colour, count, iqr = group_df.loc[i]

    ax.bar(x=i, height=iqr, bottom=q10, color=colour)
    ax.bar(x=i, height=0, bottom = median, fill=False, edgecolor = invert_color(colour), linewidth=2)

ticks = [f"(n={c}) {g}" for g, c in zip(group_df["group_name_v6"], group_df["count"])]

ax.set_xticks(ticks=range(len(group_df)), labels=ticks, rotation=90, ha="center")
ax.set_xlim(-0.6, len(group_df)-0.4)
ax.set_ylim(1e-13, 1e-7)
ax.set_yscale("log")
ax.set_ylabel("Extinction opportunity cost distribution \n ($\Delta$E per kilogram)")
plt.savefig("../outputs/Fig1_recreation.png", dpi=600, bbox_inches='tight')

    
