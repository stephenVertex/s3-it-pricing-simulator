import sims3 as sims3


s = sims3.S3Sim()
s.simulate_day()
df = s.generate_dataframe()
df_sum = s. sum_df_by_date(df)