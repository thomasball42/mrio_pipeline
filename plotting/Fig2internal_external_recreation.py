import pandas as pd
import pylab as plt
import numpy as np
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
order = pd.DataFrame({'Grains, roots, starchy carbohydrates' : "3",
                'Legumes, beans, nuts' : "5",
                'Fruit and vegetables' : "4",
                'Stimulants and spices' : "6",
                'Ruminant meat' : "0", 
                'Dairy and eggs' : "2",
                'Poultry and pig meat' : "1", 
                'Sugar crops' : "7",
                }.items(), columns=["Group", "Order"])

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    area_codes = pd.read_excel(f"../input_data/nocsDataExport_20251021-164754.xlsx", engine="openpyxl")  
    area_codes = area_codes[["ISO3", "FAOSTAT"]].rename(columns={"ISO3":"Country", "FAOSTAT":"FAO_Code"})

pop_data = pd.read_csv("../input_data/SUA_Crops_Livestock_E_All_Data_(Normalized).csv", encoding="latin-1", low_memory=False)
pop_data = pop_data[pop_data["Element Code"] == 511][["Area Code", "Year", "Value"]]
pop_data["Value"] *= 1000  # convert to individuals


master_df_local = pd.DataFrame()
master_df_imports = pd.DataFrame()

results_dir = "../results/"
for year in os.listdir(results_dir):
    for country in os.listdir(f"{results_dir}{year}"):
        if country == ".mrio" or country == "missing_items.txt" or country == "UGA":
            continue

        df1 = pd.read_csv(f"{results_dir}{year}/{country}/df_{country.lower()}.csv", index_col=0)
        df1 = df1[["Group", "bd_opp_total", "bd_opp_total_err"]]
        df1 = df1.groupby(["Group"]).sum().reset_index()
        df1["Year"] = int(year)
        df1["Country"] = country
        df1 = df1.merge(area_codes, on="Country", how="left")
        df1 = df1.merge(pop_data, left_on=["FAO_Code", "Year"], right_on=["Area Code", "Year"], how="left")
        df1["bd_opp_total"] /= (df1["Value"] * 365)  # per capita per day
        df1["bd_opp_total_err"] /= (df1["Value"] * 365)
        df1 = df1.drop(columns=["FAO_Code", "Area Code", "Value"])
        master_df_local = pd.concat([master_df_local, df1], ignore_index=True)

        df2 = pd.read_csv(f"{results_dir}{year}/{country}/df_os.csv", index_col=0)
        df2 = df2[["Group", "bd_opp_total", "bd_opp_total_err"]]
        df2 = df2.groupby(["Group"]).sum().reset_index()
        df2["Year"] = int(year)
        df2["Country"] = country
        df2 = df2.merge(area_codes, on="Country", how="left")
        df2 = df2.merge(pop_data, left_on=["FAO_Code", "Year"], right_on=["Area Code", "Year"], how="left")
        df2["bd_opp_total"] /= (df2["Value"] * 365)  # per capita per day
        df2["bd_opp_total_err"] /= (df2["Value"] * 365)
        df2 = df2.drop(columns=["FAO_Code", "Area Code", "Value"])
        master_df_imports = pd.concat([master_df_imports, df2], ignore_index=True)



master_df_local = master_df_local.merge(order, on="Group")
master_df_local = master_df_local.sort_values(["Country", "Year", "Order"])
master_df_local = master_df_local.drop(columns=["Order"])
master_df_local["bd_opp_total"] *= -1
master_df_local["bd_opp_total_err"] *= -1

master_df_imports = master_df_imports.merge(order, on="Group")
master_df_imports = master_df_imports.sort_values(["Country", "Year", "Order"])
master_df_imports = master_df_imports.drop(columns=["Order"])

fig, axs = plt.subplots(nrows=3, ncols=2)
axs = axs.flatten()


for i, country in enumerate(master_df_local["Country"].unique()):
    country_df_local = master_df_local[master_df_local["Country"] == country]
    plot = so.Plot(country_df_local, x="Year", y="bd_opp_total", color="Group").add(so.Area(alpha=1), so.Stack(), legend=False)
    plot = plot.scale(color=color_dict)
    plot.on(axs[i]).plot()

    country_df_imports = master_df_imports[master_df_imports["Country"] == country]
    plot = so.Plot(country_df_imports, x="Year", y="bd_opp_total", color="Group").add(so.Area(alpha=1), so.Stack(), legend=False)
    plot = plot.scale(color=color_dict)
    plot.on(axs[i]).plot()

    y = np.abs(axs[i].get_ylim()).max()
    axs[i].set_ylim(-y, y)
    axs[i].set_title(country)
    axs[i].set_xlim(master_df_local["Year"].min(), master_df_local["Year"].max())
    axs[i].axhline(0, color="black", linewidth=0.8)
    # axs[i].set_ylim(0, 5e-9)

for ax in axs[:-2]:
    ax.set_xlabel("")
    ax.set_xticklabels([])
plt.show()
