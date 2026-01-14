import pandas as pd 
import os
import numpy as np


def interpolate_vals(year, spam_years, datPath):
    year1 = max([yr for yr in spam_years if yr < year])
    year2 = min([yr for yr in spam_years if yr > year])

    df1 = pd.read_csv(os.path.join(datPath, "mapspam_outputs", "outputs", str(year1), f"processed_results_{year1}.csv"))
    df2 = pd.read_csv(os.path.join(datPath, "mapspam_outputs", "outputs", str(year2), f"processed_results_{year2}.csv"))

    # duplicates rows and changes names for commodity where the crosswalk changes
    commodity_crosswalk = pd.read_csv(f"{datPath}/commodity_crosswalk.csv", index_col = 0)
    differences = commodity_crosswalk[commodity_crosswalk[f'spam_{year1}'] != commodity_crosswalk[f'spam_{year2}']][[f'spam_{year1}', f'spam_{year2}']].drop_duplicates()
    df1_missing = df1[df1["item_name"].isin(differences[f'spam_{year1}'])].copy()
    df1_missing = df1_missing.merge(differences, left_on="item_name", right_on=f'spam_{year1}', how="left")
    df1_missing["item_name"] = df1_missing[f'spam_{year2}']
    df1_missing.drop(columns=[f'spam_{year1}', f'spam_{year2}'], inplace=True)
    df1 = pd.concat([df1, df1_missing], ignore_index=True)

    interp_cols = ["deltaE_mean", "deltaE_mean_sem", "sp_count", "pixel_count"]
    index_cols = ["ISO3", "item_name", "band_name"]

    merged = df1[index_cols + interp_cols].merge(
        df2[index_cols + interp_cols],
        on=index_cols,
        suffixes=(f"_{year1}", f"_{year2}"),
        how="outer", 
        indicator=True
    )

    matched = merged[merged["_merge"] == "both"].copy()
    interp_cols = ["deltaE_mean", "sp_count", "pixel_count"]
    for col in interp_cols:
        matched[col] = matched[f"{col}_{year1}"] + (matched[f"{col}_{year2}"] - matched[f"{col}_{year1}"]) * (year - year1) / (year2 - year1)

    matched[f"perc_err_{year1}"] = (matched[f"deltaE_mean_sem_{year1}"]/matched[f"deltaE_mean_{year1}"]).replace(np.inf, 0).fillna(0)
    matched[f"perc_err_{year2}"] = (matched[f"deltaE_mean_sem_{year2}"]/matched[f"deltaE_mean_{year2}"]).replace(np.inf, 0).fillna(0)
    matched["deltaE_mean_sem"] = (np.abs(matched["deltaE_mean"]) * np.sqrt(matched[f"perc_err_{year1}"]**2 + matched[f"perc_err_{year2}"]**2))
    
    interp_cols = ["deltaE_mean", "deltaE_mean_sem", "sp_count", "pixel_count"]
    matched["FLAG"] = "interpolated"

    unmatched_1 = merged[merged["_merge"] == "left_only"].copy()
    unmatched_2 = merged[merged["_merge"] == "right_only"].copy()

    for col in interp_cols:
        unmatched_1[col] = unmatched_1[f"{col}_{year1}"]
        unmatched_2[col] = unmatched_2[f"{col}_{year2}"]
        
    unmatched_1["FLAG"] = f"from_{year1}"
    unmatched_2["FLAG"] = f"from_{year2}"

    df_interp = pd.concat([
                        matched[index_cols + interp_cols + ["FLAG"]],
                        unmatched_1[index_cols + interp_cols + ["FLAG"]],
                        unmatched_2[index_cols + interp_cols + ["FLAG"]]
                        ],
                        ignore_index=True)

    os.makedirs(os.path.join(datPath, "mapspam_outputs", "interpolated"), exist_ok=True)

    outpath = os.path.join(datPath, "mapspam_outputs", "interpolated", f"interpolated_results_{year}.csv")
    df_interp.to_csv(outpath, index=False)

    return outpath


def fetch_biodiversity_vals_path(year, datPath):

    spam_years = os.listdir(os.path.join(datPath, "mapspam_outputs", "outputs"))
    spam_years = [int(_) for _ in spam_years if _.isdigit()]

    distance = min([np.abs(yr-year) for yr in spam_years])
    spam_yr = [year + distance * i for i in [-1, 1] if year + distance * i in spam_years][0]

    if year in spam_years: # exact data
        file_path = os.path.join(datPath, "mapspam_outputs", "outputs", str(year), f"processed_results_{year}.csv")

    elif year < min(spam_years): # use earliest as can't interpolate
        file_path = os.path.join(datPath, "mapspam_outputs", "outputs", str(min(spam_years)), f"processed_results_{min(spam_years)}.csv")

    elif year > max(spam_years): # use latest as can't interpolate
        file_path = os.path.join(datPath, "mapspam_outputs", "outputs", str(max(spam_years)), f"processed_results_{max(spam_years)}.csv")

    else: # use interpolated vals
        file_path = os.path.join(datPath, "mapspam_outputs", "interpolated", f"interpolated_results_{year}.csv")
        
        if not os.path.exists(file_path):
            interpolate_vals(year, spam_years, datPath)
    
    return file_path, spam_yr

if __name__ == "__main__":
    import os
    os.chdir("../")
    interpolate_vals(2015, [2000, 2005, 2010, 2020], "./input_data")