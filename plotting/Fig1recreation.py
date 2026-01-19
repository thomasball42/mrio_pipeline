import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns

results_dir = "../results"
year=2021

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

def weighted_quantile(values, quantiles, sample_weight=None, 
                      values_sorted=False, old_style=False):
    
    """ Very close to numpy.percentile, but supports weights.
    NOTE: quantiles should be in [0, 1]!
    :param values: numpy.array with data
    :param quantiles: array-like with many quantiles needed
    :param sample_weight: array-like of the same length as `array`
    :param values_sorted: bool, if True, then will avoid sorting of
        initial array
    :param old_style: if True, will correct output to be consistent
        with numpy.percentile.
    :return: numpy.array with computed quantiles.
    """
    values = np.array(values)
    quantiles = np.array(quantiles)
    if sample_weight is None:
        sample_weight = np.ones(len(values))
    sample_weight = np.array(sample_weight)
    assert np.all(quantiles >= 0) and np.all(quantiles <= 1), \
        'quantiles should be in [0, 1]'

    if not values_sorted:
        sorter = np.argsort(values)
        values = values[sorter]
        sample_weight = sample_weight[sorter]

    weighted_quantiles = np.cumsum(sample_weight) - 0.5 * sample_weight
    
    if old_style:
        # To be convenient with numpy.percentile
        weighted_quantiles -= weighted_quantiles[0]
        weighted_quantiles /= weighted_quantiles[-1]
    else:
        weighted_quantiles /= np.sum(sample_weight)
    return np.interp(quantiles, weighted_quantiles, values)


for country_iso in os.listdir(f"{results_dir}/{year}"):
    if len(country_iso) != 3 or not os.path.exists(f"{results_dir}/{year}/{country_iso}/df_{country_iso.lower()}.csv"):
        continue
    country_df = pd.read_csv(f"{results_dir}/{year}/{country_iso}/impacts_full.csv")[["Consumer_Country_Code", "Producer_Country_Code", "Animal_Product_Code", "ItemT_Code", "bd_opp_cost_calc", "provenance"]]
    
    country_df["Effective_Producer_Code"] = country_df["Consumer_Country_Code"]
    country_df.loc[country_df.Animal_Product_Code.isna(), "Effective_Producer_Code"] = country_df.loc[country_df.Animal_Product_Code.isna(), "Producer_Country_Code"]
    country_df.loc[country_df.Animal_Product_Code=="Primary", "Effective_Producer_Code"] = country_df.loc[country_df.Animal_Product_Code=="Primary", "Producer_Country_Code"]
    country_df["TotalProduction"] = 0.0
    country_df.loc[country_df.Animal_Product_Code.isna(), "TotalProduction"] = country_df.loc[country_df.Animal_Product_Code.isna(), "provenance"]
    country_df.loc[country_df.Animal_Product_Code=="Primary", "TotalProduction"] = country_df.loc[country_df.Animal_Product_Code=="Primary", "provenance"]
    country_df = country_df[["ItemT_Code", "bd_opp_cost_calc", "TotalProduction", "Effective_Producer_Code"]]


    df = pd.concat([df, country_df], ignore_index=True)


df = df.groupby(["ItemT_Code", "Effective_Producer_Code"]).sum().reset_index()


df = df.merge(commodity_crosswalk[["Item_Code", "group_name_v6"]], left_on="ItemT_Code", right_on=["Item_Code"], how="left")
df = df.drop(columns=["ItemT_Code", "Item_Code"])
# df = df.groupby(["group_name_v6", "Effective_Producer_Code"]).sum().reset_index()
df["Impact_per_kg"] = df["bd_opp_cost_calc"] / (df["TotalProduction"]*1000)
# df = df.drop(columns=["bd_opp_cost_calc"])
# print(df)

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    area_codes = pd.read_excel(f"../input_data/nocsDataExport_20251021-164754.xlsx", engine="openpyxl")  
    area_codes = area_codes[["ISO3", "FAOSTAT"]].rename(columns={"ISO3":"Country", "FAOSTAT":"Effective_Producer_Code"})
df = df.merge(area_codes, on="Effective_Producer_Code", how="left")

group_df = pd.DataFrame(columns=["group_name_v6", "median_impact", "q10", "q90", "colour", "count"])
quants=[0.1, 0.5, 0.9]

groups = list(df["group_name_v6"].unique())
try:
    groups.remove(np.nan)
except:
    pass

for group in groups:
    group_data = df[df["group_name_v6"] == group]
    # if group == "Dairy":
    #     print(group_data.sort_values(by="TotalProduction", ascending=False)[["Country", "Impact_per_kg", "TotalProduction"]]["TotalProduction"].sum())
    group_data = group_data.dropna(subset=["Impact_per_kg"])
    group_data = group_data[group_data["Impact_per_kg"]>0]
    occurences = group_data.shape[0]
    q10, median_impact, q90 = weighted_quantile(values=group_data["Impact_per_kg"], quantiles=quants, sample_weight=group_data["TotalProduction"], values_sorted=False)
    median_impact = np.percentile(a=group_data["Impact_per_kg"], q=50, weights=group_data["TotalProduction"], method="inverted_cdf")
    q10 = np.percentile(a=group_data["Impact_per_kg"], q=10, weights=group_data["TotalProduction"], method="inverted_cdf")
    q90 = np.percentile(a=group_data["Impact_per_kg"], q=90, weights=group_data["TotalProduction"], method="inverted_cdf")
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
plt.savefig(f"../outputs/Fig1_recreation{year}.png", dpi=600, bbox_inches='tight')

    
