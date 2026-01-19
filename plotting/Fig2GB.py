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
        df1[variable] /= (df1["Value"] * 365)  # per capita per day
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
        df2[variable] /= (df2["Value"] * 365)  # per capita per day
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
    master_df_imports_group = master_df_imports[master_df_imports["Group"] == Group]
    master_df_local_group = master_df_local[master_df_local["Group"] == Group]

    try:
        master_df_imports_group[variable2] = (master_df_imports_group[variable2] /
                                                   master_df_imports_group.loc[master_df_imports_group.first_valid_index(), variable2] - 1)*100
        plot = sns.lineplot(master_df_imports_group, x="Year", y=variable2, color=color_dict[Group], ax=axs, label=Group,)
    except KeyError:
        pass
    try:
        master_df_local_group[variable2] = (master_df_local_group[variable2] / 
                                                 master_df_local_group.loc[master_df_local_group.first_valid_index(), variable2] - 1)*100
        plot = sns.lineplot(master_df_local_group, x="Year", y=variable2, color=color_dict[Group], ax=axs, linestyle=":")
    except KeyError:
        pass


y = np.abs(axs.get_ylim()).max()
axs.set_ylim(-100, 100)
axs.set_xlim(master_df_local["Year"].min(), master_df_local["Year"].max())
axs.axhline(0, color="black", linewidth=0.8)
axs.set_ylabel("% Change since 2000")
axs.set_title(f"Change in Impact per kg (%) since 2000 - {country}\nImports (solid) vs Local production (dashed)")
# axs[i].set_ylim(0, 5e-9)


plt.show()
