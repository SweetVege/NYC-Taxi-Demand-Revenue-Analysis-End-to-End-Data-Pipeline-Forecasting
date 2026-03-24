from datetime import datetime
from meteostat import Point, Hourly
import os

def main():
    os.makedirs("data_raw/weather", exist_ok=True)

    nyc = Point(40.7829, -73.9654)

    start = datetime(2023, 1, 1)
    end   = datetime(2025, 12, 31, 23, 59)

    dfw = Hourly(nyc, start, end).fetch().reset_index()

    out = "data_raw/weather/weather_hourly_nyc_2023_2025.parquet"
    dfw.to_parquet(out, index=False)

    print("saved:", out)
    print("shape:", dfw.shape)

if __name__ == "__main__":
    main()
