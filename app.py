from config import EDGE_CASES, COL_RENAME, NEW_COLS
import gc
from module import outageProcessor
import pandas as pd

csv_data = 'data/outage.csv'
output_file_name = "outage_cleaned"

op = outageProcessor(csv_data)
df = op.df

for k, v in EDGE_CASES.items():
    df = op.clean_edge_cases(v[0], v[1], v[2])

df = op.clean_geo_area(df)

df = op.calc_dur(df)

df = op.clean_states(df)

df = op.set_outage_type(df)

df.rename(
    COL_RENAME,
    axis=1,
    inplace=True
    )

new_df = df[NEW_COLS][df['state'] != '']

new_df['duration'].fillna('', inplace=True)

df_final = op.drop_negative_durs(new_df)

if __name__ == "__main__":
    df_final.to_csv(f'{output_file_name}.csv', index=False)
    del df, new_df, df_final
    gc.collect()