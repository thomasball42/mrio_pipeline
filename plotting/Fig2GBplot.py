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

variable = "Cons"

results_dir = "../results/"
for year in os.listdir(results_dir):
    if year == "impacts":
        continue
    for country in ["USA", "IND", "BRA", "JPN", "UGA", "GBR"]:
        if country == ".mrio" or country == "missing_items.txt" or country == "AUS":
            continue

        df1 = pd.read_csv(f"{results_dir}{year}/{country}/df_{country.lower()}.csv", index_col=0)
        df1 = df1[["Group", variable, "bd_opp_total", "bd_opp_total_err"]]
        df1 = df1.groupby(["Group"]).sum().reset_index()
        df1["Year"] = int(year)
        df1["Country"] = country
        df1 = df1.merge(area_codes, on="Country", how="left")
        df1 = df1.merge(pop_data, left_on=["FAO_Code", "Year"], right_on=["Area Code", "Year"], how="left")
        df1[variable] /= (df1["Value"] * 365 / 1000)  # per capita per day and convert to kg
        df1["bd_opp_total"] /= (df1["Value"] * 365)
        df1["bd_opp_total_err"] /= (df1["Value"] * 365)
        df1 = df1.drop(columns=["FAO_Code", "Area Code", "Value"])
        master_df_local = pd.concat([master_df_local, df1], ignore_index=True)

        df2 = pd.read_csv(f"{results_dir}{year}/{country}/df_os.csv", index_col=0)
        df2 = df2[["Group", variable, "bd_opp_total", "bd_opp_total_err"]]
        df2 = df2.groupby(["Group"]).sum().reset_index()
        df2["Year"] = int(year)
        df2["Country"] = country
        df2 = df2.merge(area_codes, on="Country", how="left")
        df2 = df2.merge(pop_data, left_on=["FAO_Code", "Year"], right_on=["Area Code", "Year"], how="left")
        df2[variable] /= (df2["Value"] * 365/1000)  # per capita per day
        df2["bd_opp_total_err"] /= (df2["Value"] * 365)
        df2["bd_opp_total"] /= (df2["Value"] * 365)
        df2 = df2.drop(columns=["FAO_Code", "Area Code", "Value"])
        master_df_imports = pd.concat([master_df_imports, df2], ignore_index=True)



master_df_local = master_df_local.merge(order, on="Group")
master_df_local = master_df_local.sort_values(["Country", "Year", "Order"])
master_df_local = master_df_local.drop(columns=["Order"])

groups = master_df_imports["Group"].unique()

master_df_imports = master_df_imports.merge(order, on="Group")
master_df_imports = master_df_imports.sort_values(["Country", "Year", "Order"])
master_df_imports = master_df_imports.drop(columns=["Order"])

fig, axs = plt.subplots(figsize=(6, 8))
country = "GBR"
master_df_local = master_df_local[master_df_local["Country"] == country]
master_df_imports = master_df_imports[master_df_imports["Country"] == country]


variable2 = "Impact per kg"
master_df_local[variable2] = master_df_local["bd_opp_total"] / master_df_local[variable]
master_df_imports[variable2] = master_df_imports["bd_opp_total"] / master_df_imports[variable]

for i, Group in enumerate(groups):
    master_df_imports_group = master_df_imports[master_df_imports["Group"] == Group].sort_values(by="Year")
    master_df_local_group = master_df_local[master_df_local["Group"] == Group].sort_values(by="Year")


    print(master_df_imports_group)
    # try:
    #     master_df_imports_group[variable2] = (master_df_imports_group[variable2] /
    #                                                master_df_imports_group.loc[master_df_imports_group.first_valid_index(), variable2] - 1)*100
    # except KeyError:
    #     pass
    # try:
    #     master_df_local_group[variable2] = (master_df_local_group[variable2] / 
    #                                              master_df_local_group.loc[master_df_local_group.first_valid_index(), variable2] - 1)*100
    # except KeyError:
    #     pass

    # master_df_imports_group = master_df_imports_group[master_df_imports_group["Year"].isin([2010, 2021])]
    # master_df_local_group = master_df_local_group[master_df_local_group["Year"].isin([2010, 2021])]


    start = 0
    plot = axs.plot(master_df_imports_group["Cons"].iloc[start:], master_df_imports_group[variable2].iloc[start:], color=color_dict[Group], label=Group, zorder=2)
    plot = axs.plot(master_df_local_group["Cons"].iloc[start:], master_df_local_group[variable2].iloc[start:], color=color_dict[Group], linestyle=":",zorder=2)
    try:
        plt.scatter(master_df_imports_group["Cons"].iloc[start], master_df_imports_group[variable2].iloc[start],
                    color="black", marker="o", s=40, edgecolor=color_dict[Group], zorder=3)
        plt.scatter(master_df_imports_group["Cons"].iloc[-1], master_df_imports_group[variable2].iloc[-1],
                    color=color_dict[Group], marker="o", s=40, edgecolor=color_dict[Group], zorder=3)
    except:
        pass

    try:
        plt.scatter(master_df_local_group["Cons"].iloc[start], master_df_local_group[variable2].iloc[start],
                    color="black", marker="o", s=40, edgecolor=color_dict[Group], zorder=3)
        plt.scatter(master_df_local_group["Cons"].iloc[-1], master_df_local_group[variable2].iloc[-1],
                    color=color_dict[Group], marker="o", s=40, edgecolor=color_dict[Group], zorder=3)
    except:
        pass

total_grid_color = "#2F7FF8"
for i in range(-14, -8):
    i = 10 ** (i)
    x = np.logspace(-2, 0, 50)
    y = i/x
    axs.plot(x, y, color=total_grid_color, alpha=0.4, linewidth=0.8, zorder=1)

for j in range(-15, -8):
    for i in np.linspace(1*(10**j), 9*(10**j), 9):
        
        x = np.logspace(-2, 0, 50)
        y = i/x
        axs.plot(x, y, ls="dashed", color=total_grid_color, alpha=0.2, linewidth=0.8, zorder=1)
    



y = np.abs(axs.get_ylim()).max()
axs.axhline(0, color="black", linewidth=0.8)
axs.set_ylabel("Impact per kg")
axs.set_xlabel("Daily consumption per capita, kg")
axs.set_title(f"Change in daily biodiversity impacts per capita - {country}\nImports (solid) vs Local production (dashed)\n2011 (black dot) to 2021 (colored)")
axs.set_ylim(1e-13, 1e-8)
axs.set_xlim(1e-2, 1)
axs.set_yscale("log")
axs.set_xscale("log")


plt.show()
