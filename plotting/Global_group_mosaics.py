import math
import pandas as pd
import matplotlib.transforms as transforms
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap
import numpy as np


# Figure setup
fig = plt.figure(figsize=(10, 15))

fwidth = 0.5
fheight = 1/3
fpad = 0.1
xpad = fpad * fwidth
ypad = fpad * fheight
left_margin = 0.03

offset3 = 0.06
shift34 = 0.03

ax1 = fig.add_axes((xpad+left_margin, 2*fheight + ypad, fwidth - 2*xpad, fheight - 2*ypad))
ax2 = fig.add_axes((fwidth + xpad+left_margin/2, 2*fheight + ypad, fwidth - 2*xpad, fheight - 2*ypad))

ax3 = fig.add_axes((xpad+left_margin, fheight + ypad + offset3 + shift34, fwidth - 2*xpad, fheight - 2*ypad - offset3))
ax4 = fig.add_axes((fwidth + xpad+left_margin/2, fheight + ypad + shift34, fwidth - 2*xpad, fheight - 2*ypad))

ax5 = fig.add_axes((xpad+left_margin, ypad, fwidth - 2*xpad, fheight - 2*ypad))
ax6 = fig.add_axes((fwidth + xpad+left_margin/2, ypad, fwidth - 2*xpad, fheight - 2*ypad))

axs = np.array([[ax1, ax2], [ax3, ax4], [ax5, ax6]])


# reference data & functions
colors = pd.DataFrame({'Grains, roots, starchy carbohydrates' : "#E69F00",
                'Legumes, beans, nuts' : "#F0E442",
                'Fruit and vegetables' : "#009E73",
                'Stimulants and spices' : "#56B4E9",
                'Ruminant meat' : "#D55E00", 
                'Dairy and eggs' : "#0072B2",
                'Poultry and pig meat' : "#CC79A7", 
                'Sugar crops' : "#93F840"
                }.items(), columns=["Group", "Color"])

colors2 = pd.DataFrame({'Grains, roots, starchy carbohydrates' : "#ffd066",
                'Legumes, beans, nuts' : "#f7f1a1",
                'Fruit and vegetables' : "#00ffba",
                'Stimulants and spices' : "#a5d7f3",
                'Ruminant meat' : "#ffaa66", 
                'Dairy and eggs' : "#33b6ff",
                'Poultry and pig meat' : "#e3b5ce", 
                'Sugar crops' : "#c7fb9d"
                }.items(), columns=["Group", "Color2"])

order = pd.DataFrame({'Grains, roots, starchy carbohydrates' : 3,
                'Legumes, beans, nuts' : 5,
                'Fruit and vegetables' : 4,
                'Stimulants and spices' : 6,
                'Ruminant meat' : 0, 
                'Dairy and eggs' : 2,
                'Poultry and pig meat' : 1, 
                'Sugar crops' : 7
                }.items(), columns=["Group", "Order"])

def label_formatting(label):
    if "Other" in label:
        label = "Other"
    if ";" in label:
        label = label.split(";")[0]
    if "with the bone" in label:
        label = label.replace("with the bone", "")
    if "Cashew" in label:
        label = "Cashews"
    if "Oil palm" in label:
        label = "Palm oil"
    return label


# Data import
df_2010 = pd.read_csv(f'../results/{2010}/GBR/impacts_aggregated.csv')
df_2021 = pd.read_csv(f'../results/{2021}/GBR/impacts_aggregated.csv')

df_2010 = pd.read_csv(f'../results/{2010}/world_aggregate_impacts.csv')
df_2021 = pd.read_csv(f'../results/{2021}/world_aggregate_impacts.csv')

for col in ["bd_opp_total", "Cons", "bd_opp_total_err"]:
    df_2010[col] = df_2010[col] / 6948574560
    df_2021[col] = df_2021[col] / 7927332080

print(df_2010["bd_opp_total"].sum())
print(df_2021["bd_opp_total"].sum())

print(df_2010["Cons"].sum())
print(df_2021["Cons"].sum())


# Axes manipulation for scale
total_2010 = df_2010["bd_opp_total"].sum()
total_2021 = df_2021["bd_opp_total"].sum()

if total_2010 > total_2021:
    axis_to_change = axs[0,1]
else:
    axis_to_change = axs[0,0]
larger_total = max(total_2010, total_2021)
smaller_total = min(total_2010, total_2021)
new_length_ratio = np.sqrt(smaller_total / larger_total)
delta = (1 - new_length_ratio)/2

current_pos = axis_to_change.get_position()
current_dim = (current_pos.x1 - current_pos.x0, current_pos.y1 - current_pos.y0)
delta_dim = (current_dim[0]*delta, current_dim[1]*delta)
new_pos = transforms.Bbox([[current_pos.x0 + delta_dim[0], current_pos.y0 + 2*delta_dim[1]], [current_pos.x1 - delta_dim[0], current_pos.y1]])
axis_to_change.set_position(new_pos)


# initialisation for plotting
default_pad = 0.005
other_condition = 10
plot_order = []
color_order = []

# Top two axes: mosaics by group + item for 2010 and 2021
for i, df in enumerate([df_2010, df_2021]):
    if df["bd_opp_total"].sum() == larger_total:
        pad = default_pad* new_length_ratio
    else:
        pad = default_pad
    xpad = pad
    df = df[["Item", "Group", "bd_opp_total", "Cons"]]
    df2 = df[["Group", "bd_opp_total", "Cons"]].copy()
    df_grouped = df2.groupby("Group").sum().reset_index()
    df_grouped = df_grouped.merge(colors, on="Group", how="left")
    df_grouped = df_grouped.merge(colors2, on="Group", how="left")
    df_grouped = df_grouped.merge(order, on="Group", how="left")
    df_grouped = df_grouped.sort_values("Order")

    total_bd_opp = df_grouped["bd_opp_total"].sum()
    df_grouped["bd_opp_perc"] = df_grouped["bd_opp_total"] / total_bd_opp
    
    left = 0
    for _, row in df_grouped.iterrows():
        group_df = df[df["Group"] == row["Group"]].copy()
        group_df = group_df.sort_values("bd_opp_total", ascending=False)
        group_df["group_bd_opp_perc"] = group_df["bd_opp_total"] / group_df["bd_opp_total"].sum()


        other_categories = group_df[group_df["Item"].str.contains("Other")]["Item"].tolist()
        others = group_df[(group_df["group_bd_opp_perc"] < other_condition*pad)|(group_df["Item"].isin(other_categories))]
        if len(others) > 1:
            others_sum = others["group_bd_opp_perc"].sum()
            group_df = group_df[(group_df["group_bd_opp_perc"] >= other_condition*pad) & (~group_df["Item"].isin(other_categories))]
            others_row = pd.DataFrame({"Item": [f"Others_{row["Group"]}"], "Group": [row["Group"]], "bd_opp_total": [0], "Cons": [0], "group_bd_opp_perc": [others_sum]})
            group_df = pd.concat([group_df, others_row], ignore_index=True)

        cmap = LinearSegmentedColormap.from_list("custom_cmap", [row["Color"], row["Color2"]])
        up = 0

        for j, item_row in group_df.iterrows():
            if item_row["group_bd_opp_perc"]-2*pad > 0:
                rect = mpatches.Rectangle((left+xpad, up+pad), row["bd_opp_perc"]-2*xpad, item_row["group_bd_opp_perc"]-2*pad, color=cmap(up))
                axs[0,i].add_patch(rect)
                if i == 0:
                    plot_order.append(item_row["Item"])
                    color_order.append(cmap(up))


            text_color = "white" if row["Group"] == "Dairy and eggs" else "black"
            if (row["bd_opp_perc"] > 0.1) and (item_row["group_bd_opp_perc"] > 0.05):
                label = label_formatting(item_row["Item"])
                axs[0,i].text(left + row["bd_opp_perc"]/2, up + item_row["group_bd_opp_perc"]/2, label, ha="center", va="center", fontsize=6, color=text_color)

            elif (row["bd_opp_perc"] > 0.04) and (item_row["group_bd_opp_perc"] > 0.1):
                label = label_formatting(item_row["Item"])
                axs[0,i].text(left + row["bd_opp_perc"]/2, up + item_row["group_bd_opp_perc"]/2, label, ha="center", va="center", fontsize=6, rotation=90, color=text_color)
            
            up += item_row["group_bd_opp_perc"]
        left += row["bd_opp_perc"]
    

    axs[0, i].set_xlim(0,1)
    axs[0, i].set_ylim(0,1)
    axs[0, i].set_xticks([])
    axs[0, i].set_yticks([])
    axs[0, i].axis('off')
axs[0,0].set_title("2010", fontsize=14)
axs[0,1].set_title("2021", fontsize=14)
plot_order.append("Sugar beet")
color_order.append("#c7fb9d")


# Middle two axes: Waterfall charts for bd change and consumption change

ax = axs[1,0]
df = df_2010.copy()
df = df.merge(df_2021[["Item", "bd_opp_total",  "bd_opp_total_err"]], on="Item", suffixes=("_2010", "_2021"))
df["bd_opp_change"] = df["bd_opp_total_2021"] - df["bd_opp_total_2010"]
df["bd_opp_perc_err"] = np.sqrt((df["bd_opp_total_err_2021"]/df["bd_opp_total_2021"])**2 + (df["bd_opp_total_err_2010"]/df["bd_opp_total_2010"])**2)

non_other_df = df[df["Item"].isin(plot_order)].copy()
other_df = df[~df["Item"].isin(plot_order)].copy()
other_df = other_df.copy().groupby("Group").sum().reset_index()
other_df["bd_opp_perc_err"] = (other_df["bd_opp_total_err_2021"]+other_df["bd_opp_total_err_2010"]) / (other_df["bd_opp_total_2010"]+other_df["bd_opp_total_2021"])
other_df["Item"] = other_df["Group"].apply(lambda x: f"Others_{x}")

non_other_df = non_other_df[["Item", "bd_opp_total_2010", "bd_opp_change", "bd_opp_perc_err"]]
other_df = other_df[["Item", "bd_opp_total_2010", "bd_opp_change", "bd_opp_perc_err"]]
df = pd.concat([non_other_df, other_df], ignore_index=True)
df = df[df["Item"].isin(plot_order)]
df = df.sort_values("Item", key=lambda x: x.map({item: i for i, item in enumerate(plot_order)}))

color_map = {item: color for item, color in zip(plot_order, color_order)}
df["Color"] = df["Item"].map(color_map)

df["bd_opp_relative_change"] = df["bd_opp_change"] / df["bd_opp_total_2010"]
df["bd_opp_relative_err"] = (df["bd_opp_perc_err"]) * np.abs(df["bd_opp_relative_change"])

left = 0
pad = 0.008
width = 1/(len(df)) - pad + pad/len(df)
for _, row in df.iterrows():
    # width = row["bd_opp_total_2010"]/df["bd_opp_total_2010"].sum()
    rect = mpatches.Rectangle((left, 0), width, row["bd_opp_relative_change"], color=row["Color"])
    ax.add_patch(rect)

    ax.errorbar(left + width/2, row["bd_opp_relative_change"], yerr=row["bd_opp_relative_err"], color="black", capsize=2, fmt="none", linewidth=0.8)

    left += width+pad

tags = []
for _, row in df.iterrows():
    if row["Item"].startswith("Others_"):
        label = row["Item"].replace("Others_", "Other ")
        if "pig meat" in label:
            label = "Other meats"
        if "Plantains" in label:
            label = "Plantains"
        if "carbohydrates" in label:
            label = "Other Grains, roots, carbs"
    else:
        label = label_formatting(row["Item"])
    tags.append(label)
ax.set_xticks(np.linspace(width/2, 1-width/2, len(df)), tags, rotation=-75, fontsize=6, ha="left", va="top")


ax.hlines(0, xmin=0, xmax=1, color="black", linewidth=0.8)
ax.set_ylim(-0.7,0.7)
ax.set_xlim(0,1)
ax.set_yticks([0.6, 0.4, 0.2, 0, -0.2, -0.4, -0.6], ["+60%", "+40%", "+20%", "0%", "-20%", "-40%", "-60%"])
ax.set_ylabel("Global Biodiversity footprint change")
ax.set_title("Biodiversity footprint change from 2010 to 2021")


# plot 4
ax = axs[1,1]
df = df_2010.copy()
df = df.merge(df_2021[["Item", "Cons", "Cons_err", "bd_opp_total",  "bd_opp_total_err"]], on="Item", suffixes=("_2010", "_2021"))
df["Cons_2010"] *= 1000
df["Cons_err_2010"] *= 1000
df["Cons_2021"] *= 1000
df["Cons_err_2021"] *= 1000
non_other_df = df[df["Item"].isin(plot_order)].copy()
other_df = df[~df["Item"].isin(plot_order)].copy()
other_df = other_df.copy().groupby("Group").sum().reset_index()
other_df["Item"] = other_df["Group"].apply(lambda x: f"Others_{x}")
df = pd.concat([non_other_df, other_df], ignore_index=True)
df = df[df["Item"]!="Sugar beet"]
color_map = {item: color for item, color in zip(plot_order, color_order)}
df["Color"] = df["Item"].map(color_map)
beef_cons_2010 = df[df["Item"]=="Meat of cattle with the bone; fresh or chilled"]["Cons_2010"].values[0] * 6948574560
for _, row in df.iterrows():
    x = row["Cons_2010"]
    y = row["bd_opp_total_2010"]/row["Cons_2010"]
    x2 = row["Cons_2021"]
    y2 = row["bd_opp_total_2021"]/row["Cons_2021"]
    dx = x2 - x
    dy = y2 - y
    ax.plot([x,x2],[y,y2], color=row["Color"])
    
    angle = math.atan2(np.log10(y)-np.log10(y2), np.log10(x)-np.log10(x2))*(180/np.pi)+90+120


    ax.plot(x2, y2, color=row["Color"], marker=(3,1,angle))
    # ax.annotate("", xytext=(x, y), xy=(x2, y2),
    #         arrowprops=dict(arrowstyle="->"))
ax.set_yscale("log")
ax.set_xscale("log")
# ax.set_xlim(df["Cons_2010"].min()*0.8, df["Cons_2021"].max()*1.4)
# ax.set_ylim((df["bd_opp_total_2010"]/df["Cons_2010"]).min()*0.8, (df["bd_opp_total_2021"]/df["Cons_2021"]).max()*1.2)

total_grid_color = "#2F7FF8"
for i in range(-12, -4):
    i = 10 ** (i)
    x = np.logspace(-4, 3, 50)
    y = i/x
    ax.plot(x, y, color=total_grid_color, alpha=0.4, linewidth=0.8, zorder=1)

for j in range(-12, -4):
    for i in np.linspace(1*(10**j), 9*(10**j), 9):
        x = np.logspace(-4, 3, 50)
        y = i/x
        ax.plot(x, y, ls="dashed", color=total_grid_color, alpha=0.2, linewidth=0.8, zorder=1)
    
ax.set_xlim(1e-1, 1e3)
ax.set_ylim(1e-11, 1e-7)
ax.set_ylabel("Impact per kg")
ax.set_xlabel("Annual consumption per capita, kg")
ax.set_title("Change in global commodity consumption and impact\nbetween 2010 and 2021")

ax2 = ax.twiny()
ax2.set_xscale('log')
x1, x2 = ax.get_xlim()
y1, y2 = ax.get_ylim()
ax2.set_xlim(x1*y2, x2*y2)
ax2.set_xlabel("Total Annual Extinctions per capita", color=total_grid_color)
ax2.tick_params(axis='x', colors=total_grid_color)


# Plots 5 and 6
ax = axs[2, 0]
ax2 = axs[2, 1]

cdat = pd.read_excel("../input_data/nocsDataExport_20251021-164754.xlsx")
COUNTRIES = [_.upper() for _ in cdat["ISO3"].unique().tolist() if isinstance(_, str)]

feed_df_2010 = pd.DataFrame()
feed_df_2021 = pd.DataFrame()
pasture_df_2010 = pd.DataFrame()
pasture_df_2021 = pd.DataFrame()

for country in COUNTRIES:
    try:
        cdf_2010 = pd.read_csv(f'../results/{2010}/{country}/impacts_full.csv')
    except FileNotFoundError:
        continue
    try:
        cdf_2021 = pd.read_csv(f'../results/{2021}/{country}/impacts_full.csv')
    except FileNotFoundError:
        continue

    cdf_2010 = cdf_2010[["Producer_Country_Code", "Consumer_Country_Code", "Item", "ItemT_Name", "bd_opp_cost_calc", "provenance", "bd_opp_cost_m2", "Pasture_avg_calc", "FAO_land_calc_m2", "Country_ISO"]]
    cdf_2021 = cdf_2021[["Producer_Country_Code", "Consumer_Country_Code", "Item", "ItemT_Name", "bd_opp_cost_calc", "provenance", "bd_opp_cost_m2", "Pasture_avg_calc", "FAO_land_calc_m2", "Country_ISO"]]
    feed_2010 = cdf_2010[(cdf_2010["ItemT_Name"]=="Meat of cattle with the bone; fresh or chilled")&(cdf_2010["FAO_land_calc_m2"]!=0)].copy()
    feed_2021 = cdf_2021[(cdf_2021["ItemT_Name"]=="Meat of cattle with the bone; fresh or chilled")&(cdf_2021["FAO_land_calc_m2"]!=0)].copy()
    past_2010 = cdf_2010[(cdf_2010["ItemT_Name"]=="Meat of cattle with the bone; fresh or chilled")&(cdf_2010["FAO_land_calc_m2"]==0)].copy()
    past_2021 = cdf_2021[(cdf_2021["ItemT_Name"]=="Meat of cattle with the bone; fresh or chilled")&(cdf_2021["FAO_land_calc_m2"]==0)].copy()

    feed_df_2010 = pd.concat([feed_df_2010, feed_2010], ignore_index=True)
    feed_df_2021 = pd.concat([feed_df_2021, feed_2021], ignore_index=True)
    pasture_df_2010 = pd.concat([pasture_df_2010, past_2010], ignore_index=True)
    pasture_df_2021 = pd.concat([pasture_df_2021, past_2021], ignore_index=True)

Country_df_2010 = pd.DataFrame()
Country_df_2021 = pd.DataFrame()


for country in pasture_df_2010["Producer_Country_Code"].unique():
    country_feed_2010 = feed_df_2010[feed_df_2010["Consumer_Country_Code"]==country]
    country_pasture_2010 = pasture_df_2010[pasture_df_2010["Producer_Country_Code"]==country]
    iso = country_pasture_2010["Country_ISO"].iloc[0]

    E = country_feed_2010["bd_opp_cost_calc"].sum() + country_pasture_2010["bd_opp_cost_calc"].sum()
    Production = country_pasture_2010["provenance"].sum()*1000
    E_per_kg = E / Production

    Pasture_m2_per_kg = country_pasture_2010["Pasture_avg_calc"].sum() / Production
    Pasture_E_per_m2 = country_pasture_2010["bd_opp_cost_calc"].sum() / country_pasture_2010["Pasture_avg_calc"].sum()

    Feed_m2_per_kg = country_feed_2010["FAO_land_calc_m2"].sum() / Production
    Feed_E_per_m2 = country_feed_2010["bd_opp_cost_calc"].sum() / country_feed_2010["FAO_land_calc_m2"].sum()

    cdf_2010 = pd.DataFrame({"ISO":[iso],
                            "E":[E],
                            "E_per_kg":[E_per_kg],
                            "Production_kg":[Production],
                            "Pasture_m2_per_kg":[Pasture_m2_per_kg],
                            "Pasture_E_per_m2":[Pasture_E_per_m2],
                            "Feed_m2_per_kg":[Feed_m2_per_kg],
                            "Feed_E_per_m2":[Feed_E_per_m2]
                            })

    Country_df_2010 = pd.concat([Country_df_2010, cdf_2010], ignore_index=True)

for country in pasture_df_2021["Producer_Country_Code"].unique():
    country_feed_2021 = feed_df_2021[feed_df_2021["Consumer_Country_Code"]==country]
    country_pasture_2021 = pasture_df_2021[pasture_df_2021["Producer_Country_Code"]==country]
    iso = country_pasture_2021["Country_ISO"].iloc[0]

    E = country_feed_2021["bd_opp_cost_calc"].sum() + country_pasture_2021["bd_opp_cost_calc"].sum()
    Production = country_pasture_2021["provenance"].sum()*1000
    E_per_kg = E / Production

    Pasture_m2_per_kg = country_pasture_2021["Pasture_avg_calc"].sum() / Production
    Pasture_E_per_m2 = country_pasture_2021["bd_opp_cost_calc"].sum() / country_pasture_2021["Pasture_avg_calc"].sum()

    Feed_m2_per_kg = country_feed_2021["FAO_land_calc_m2"].sum() / Production
    Feed_E_per_m2 = country_feed_2021["bd_opp_cost_calc"].sum() / country_feed_2021["FAO_land_calc_m2"].sum()

    cdf_2021 = pd.DataFrame({"ISO":[iso],
                            "E":[E],
                            "E_per_kg":[E_per_kg],
                            "Production_kg":[Production],
                            "Pasture_m2_per_kg":[Pasture_m2_per_kg],
                            "Pasture_E_per_m2":[Pasture_E_per_m2],
                            "Feed_m2_per_kg":[Feed_m2_per_kg],
                            "Feed_E_per_m2":[Feed_E_per_m2]
                            })

    Country_df_2021 = pd.concat([Country_df_2021, cdf_2021], ignore_index=True)


ax.set_xlim(1e-8, 1)
ax.set_ylim(5e-11, 2e-5)
ax.set_yscale("log")
ax.set_xscale("log")

ax2.set_xlim(1e-13, 1e-8)
ax2.set_ylim(1e0, 1e6)
ax2.set_yscale("log")
ax2.set_xscale("log")

for country in Country_df_2021["ISO"].unique():
    try:
        row_2010 = Country_df_2010[Country_df_2010["ISO"]==country].iloc[0]
        row_2021 = Country_df_2021[Country_df_2021["ISO"]==country].iloc[0]
    except IndexError:
        continue

    x = row_2010["Production_kg"]/beef_cons_2010
    y = row_2010["E_per_kg"]
    x2 = row_2021["Production_kg"]/beef_cons_2010
    y2 = row_2021["E_per_kg"]
    dx = x2 - x
    dy = y2 - y
    line = ax.plot([x,x2],[y,y2])
    color = line[0].get_color()
  

    def t1(z): return (ax.transData + ax.transAxes.inverted()).transform(z)
    def t2(z): return (ax.transAxes + ax.transData.inverted()).transform(z)
    def t3(z): return (ax2.transData + ax2.transAxes.inverted()).transform(z)
    def t4(z): return (ax2.transAxes + ax2.transData.inverted()).transform(z)

    x_axes, y_axes = t1((x,y))
    x2_axes, y2_axes = t1((x2,y2))
    dx_axes = x2_axes - x_axes
    dy_axes = y2_axes - y_axes
    l = np.sqrt(dx_axes**2 + dy_axes**2)
    
    text_coords = t1((x,y))
    text_coords = text_coords - np.array([0.02 * dx_axes/l, 0.02 * dy_axes/l])
    text_coords = t2(text_coords)

    arrow_height = 0.015
    if arrow_height+0.005 > l:
        arrow_height = l - 0.005

    patch = mpatches.FancyArrow(x_axes,y_axes,dx_axes,dy_axes, width=0.001, ec=color, fc=color,
                                linewidth=1, transform=ax.transAxes, length_includes_head=True,
                                head_width=0.015, head_length=arrow_height)
    ax.add_patch(patch)

    angle = math.atan2(dy_axes, dx_axes)*(180/np.pi)+90
    if angle > 90 and angle < 270:
        angle = angle - 180

    line[0].set_color("#00000000")
    ax.text(*text_coords, s=row_2010["ISO"], fontsize=6, ha="center", va="center", color=color, rotation=angle)



    area_2010 = (row_2010["Pasture_m2_per_kg"] + row_2010["Feed_m2_per_kg"]) * row_2010["Production_kg"]
    area_2021 = (row_2021["Pasture_m2_per_kg"] + row_2021["Feed_m2_per_kg"]) * row_2021["Production_kg"]
    bd_cost1 = row_2010["E"] / area_2010
    eff1 = area_2010 / row_2010["Production_kg"]
    bd_cost2 = row_2021["E"] / area_2021
    eff2 = area_2021 / row_2021["Production_kg"]

    x_axes, y_axes = t3((bd_cost1,eff1))
    x2_axes, y2_axes = t3((bd_cost2,eff2))
    dx_axes = x2_axes - x_axes
    dy_axes = y2_axes - y_axes
    l = np.sqrt(dx_axes**2 + dy_axes**2)
    
    text_coords = t3((bd_cost1,eff1))
    text_coords = text_coords - np.array([0.02 * dx_axes/l, 0.02 * dy_axes/l])
    text_coords = t4(text_coords)

    arrow_height = 0.015
    if arrow_height+0.005 > l:
        arrow_height = l - 0.005

    patch = mpatches.FancyArrow(x_axes,y_axes,dx_axes,dy_axes, width=0.001, ec=color, fc=color,
                                linewidth=1, transform=ax2.transAxes, length_includes_head=True,
                                head_width=0.015, head_length=arrow_height)
    ax2.add_patch(patch)

    angle = math.atan2(dy_axes, dx_axes)*(180/np.pi)+90
    if angle > 90 and angle < 270:
        angle = angle - 180
   
    ax2.text(*text_coords, s=row_2010["ISO"], fontsize=6, ha="center", va="center", color=color, rotation=angle)



ax.set_title("Change in Beef impact between 2010 and 2021")
ax.set_ylabel("Impact, Annual Extinctions per kg")
ax.set_xlabel("Share of Global Beef Production")

x1, x2 = ax.get_xlim()
y1, y2 = ax.get_ylim()
total_grid_color = "#2F7FF8"
for i in range(int(np.log10(x1*y1))-2, int(np.log10(x2*y2))+2):
    i = 10 ** (i)
    x = np.logspace(np.log10(x1)-1, np.log10(x2)+1, 50)
    y = i/x
    ax.plot(x, y, color=total_grid_color, alpha=0.4, linewidth=0.8, zorder=1)
for j in range(int(np.log10(x1*y1))-2, int(np.log10(x2*y2))+2):
    for i in np.linspace(1*(10**j), 9*(10**j), 9):
        x = np.logspace(np.log10(x1)-1, np.log10(x2)+1, 50)
        y = i/x
        ax.plot(x, y, ls="dashed", color=total_grid_color, alpha=0.2, linewidth=0.8, zorder=1)
ax3 = ax.twiny()
ax3.set_xscale('log')
x1, x2 = ax.get_xlim()
y1, y2 = ax.get_ylim()
ax3.set_xlim(x1*y2, x2*y2)
ax3.set_xlabel("Annual Extinctions per kg", color=total_grid_color)
ax3.tick_params(axis='x', colors=total_grid_color)
    

ax2.set_title("Change in Beef impact per kg between 2010 and 2021")
ax2.set_ylabel(r"Biodiversity Opportunity Cost, Extinctions per m$^2$")
ax2.set_xlabel(r"Land use efficiency, m$^2$ per kg")

x1, x2 = ax2.get_xlim()
y1, y2 = ax2.get_ylim()
total_grid_color = "#2F7FF8"
for i in range(int(np.log10(x1*y1))-2, int(np.log10(x2*y2))+2):
    i = 10 ** (i)
    x = np.logspace(np.log10(x1)-1, np.log10(x2)+1, 50)
    y = i/x
    ax2.plot(x, y, color=total_grid_color, alpha=0.4, linewidth=0.8, zorder=1)
for j in range(int(np.log10(x1*y1))-2, int(np.log10(x2*y2))+2):
    for i in np.linspace(1*(10**j), 9*(10**j), 9):
        x = np.logspace(np.log10(x1)-1, np.log10(x2)+1, 50)
        y = i/x
        ax2.plot(x, y, ls="dashed", color=total_grid_color, alpha=0.2, linewidth=0.8, zorder=1)
ax4 = ax2.twiny()
ax4.set_xscale('log')
x1, x2 = ax2.get_xlim()
y1, y2 = ax2.get_ylim()
ax4.set_xlim(x1*y2, x2*y2)
ax4.set_xlabel("Annual Extinctions per kg", color=total_grid_color)
ax4.tick_params(axis='x', colors=total_grid_color)



plt.savefig('../outputs/Global_group_mosaics.png', dpi=1200)