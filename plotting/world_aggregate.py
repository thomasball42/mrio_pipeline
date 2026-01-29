import pandas as pd
import os
df = []
year = 2021
for country in os.listdir(f'../results/{year}/'):
    if len(country) == 3:
        try:
            df_country = pd.read_csv(f'../results/{year}/{country}/impacts_aggregated.csv')
            df.append(df_country)
        except FileNotFoundError:
            pass
df = pd.concat(df)
df = df.groupby(["Item", "Group"], as_index=False).sum()
df = df.drop(columns=["Unnamed: 0"])
df.to_csv(f'../results/{year}/world_aggregate_impacts.csv', index=False)