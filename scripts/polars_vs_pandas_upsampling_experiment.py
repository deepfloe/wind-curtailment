import pandas as pd
import polars as pl

# Creating a date range
date_range = pd.date_range(start='2026-01-01', periods=3, freq='5min')

# Creating random float data
float_data = range(3)

# Creating the DataFrame
df = pd.DataFrame({
    'datetime': date_range,
    'float': float_data
})

#print()
upsample_pandas = df.set_index("datetime").resample("T").mean(numeric_only=True).interpolate()
upsample_polars = pl.from_pandas(df).upsample(every='1m', time_column="datetime",maintain_order=True).interpolate()
print(upsample_pandas)
print(upsample_polars)
