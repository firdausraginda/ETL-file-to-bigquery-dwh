from datetime import datetime
import pandas as pd
from datetime import datetime

precipitation_df = pd.read_csv("./src/weather/USW00023169-LAS_VEGAS_MCCARRAN_INTL_AP-precipitation-inch.csv",
                               na_values="T", dtype={"precipitation": float}, parse_dates=["date"])
temperature_df = pd.read_csv(
    "./src/weather/USW00023169-temperature-degreeF.csv", parse_dates=["date"])

print(precipitation_df.info())
print(temperature_df.info())
