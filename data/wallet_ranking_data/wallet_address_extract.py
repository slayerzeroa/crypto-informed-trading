import pandas as pd

pd.set_option('display.max_columns', None)

df = pd.read_csv("data/wallet_ranking/status/wallet_ranking_250103_test.csv")
print(df)
# df['filtered_address'].to_csv("wall_ranking_address_only.csv", index=False)