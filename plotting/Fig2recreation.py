import pandas as pd
import pylab as plt
import seaborn as sns
import seaborn.objects as so
import os

color_dict = {'Grains, roots, starchy carbohydrates' : "#E69F00",
                'Legumes, beans, nuts' : "#F0E442",
                'Fruit and vegetables' : "#009E73",
                'Stimulants and spices' : "#56B4E9",
                'Ruminant meat' : "#D55E00", 
                'Dairy and eggs' : "#0072B2",
                'Poultry and pig meat' : "#CC79A7", 
                'Sugar crops' : "#93F840",
                'Total' : "#000000"
                }
order = pd.DataFrame({'Grains, roots, starchy carbohydrates' : 3,
                'Legumes, beans, nuts' : 5,
                'Fruit and vegetables' : 4,
                'Stimulants and spices' : 6,
                'Ruminant meat' : 0, 
                'Dairy and eggs' : 2,
                'Poultry and pig meat' : 1, 
                'Sugar crops' : 7,
                'Total' : 8,
                }.items(), columns=["Group", "Order"])

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    area_codes = pd.read_excel(f"../input_data/nocsDataExport_20251021-164754.xlsx", engine="openpyxl")  
    area_codes = area_codes[["ISO3", "FAOSTAT"]].rename(columns={"ISO3":"Country", "FAOSTAT":"FAO_Code"})

pop_data = pd.read_csv("../input_data/SUA_Crops_Livestock_E_All_Data_(Normalized).csv", encoding="latin-1", low_memory=False)
pop_data = pop_data[pop_data["Element Code"] == 511][["Area Code", "Year", "Value"]]
pop_data["Value"] *= 1000  # convert to individuals


master_df = pd.DataFrame()

results_dir = "../results/"
for year in os.listdir(results_dir):
    if year == "impacts":
        continue
    for country in ["USA", "IND", "BRA", "JPN", "UGA", "GBR"]:
        if country == ".mrio" or country == "missing_items.txt":
            continue
        df = pd.read_csv(f"{results_dir}{year}/{country}/impacts_aggregated.csv", index_col=0)
        df = df[["Group", "bd_opp_total", "bd_opp_total_err"]]
        df = df.groupby(["Group"]).sum().reset_index()
        df["Year"] = int(year)
        df["Country"] = country
        df = df.merge(area_codes, on="Country", how="left")
        df = df.merge(pop_data, left_on=["FAO_Code", "Year"], right_on=["Area Code", "Year"], how="left")
        df["bd_opp_total"] /= (df["Value"] * 365)  # per capita per day
        df["bd_opp_total_err"] /= (df["Value"] * 365)
        df = df.drop(columns=["FAO_Code", "Area Code", "Value"])
        master_df = pd.concat([master_df, df], ignore_index=True)
master_df = master_df.merge(order, on="Group")
master_df = master_df.sort_values(["Country", "Year", "Order"])
master_df = master_df.drop(columns=["Order"])
fig, axs = plt.subplots(nrows=3, ncols=2, figsize=(10,10))
axs = axs.flatten()

RELATIVE = False

if RELATIVE:
    totals = master_df.groupby(["Country", "Year"])["bd_opp_total"].sum().reset_index()
    totals = totals.rename(columns={"bd_opp_total":"Total"})
    master_df = master_df.merge(totals[["Country", "Year", "Total"]], on=["Country", "Year"])
    master_df["bd_opp_total"] /= master_df["Total"] / 100
    master_df_imports = master_df.drop(columns=["Total"])





for i, country in enumerate(master_df["Country"].unique()):
    country_df = master_df[master_df["Country"] == country]
    plot = so.Plot(country_df, x="Year", y="bd_opp_total", color="Group").add(so.Area(alpha=1), so.Stack(), legend=False)
    plot = plot.scale(color=color_dict)  # pyright: ignore[reportArgumentType]
    plot.on(axs[i]).plot()
    axs[i].set_title(country)
    axs[i].set_xlim(master_df["Year"].min(), master_df["Year"].max())

for ax in axs:
    ax.set_ylabel("")
    if RELATIVE:
        ax.set_ylim(0, 100)
for ax in axs[2:3]:
    ax.set_ylabel("Biodiversity impact\nopportunity per capita per day" + (" (%)" if RELATIVE else " (species)"))
for ax in axs[0:1]:
    for k, v in sorted(color_dict.items(), key=lambda x: 8-order[order["Group"] == x[0]]["Order"].values[0]):
        if k != "Total":
            axs[0].fill_between([], [], [], color=v, label=k)
    ax.legend(fontsize=7)#, bbox_to_anchor=(1.05, 1), loc='upper left')

for ax in axs[:-2]:
    ax.set_xlabel("")
    ax.set_xticklabels([])
fig.tight_layout()
plt.savefig(f"../outputs/Fig2_{"relative" if RELATIVE else "total"}.png", dpi=600)
# plt.show()
