import pandas as pd
import ee
from datetime import datetime, timedelta

# Initialize Earth Engine
ee.Initialize(project="agro-tech-467404")

# Load SMAP soil moisture collection
smap = ee.ImageCollection("NASA_USDA/HSL/SMAP10KM_soil_moisture")

df = pd.read_excel('subset.xlsx')
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]  # Remove Unnamed columns
df["ssm(m³/m³)"] = None
df["ssm_date"] = None  # to store which SMAP date was used

for i in range(len(df)):
    temp = df.iloc[i]
    lon, lat, date_str = temp["LONGITUDE"], temp["LATITUDE"], str(temp["ACQ_DATE"]).split(" ")[0]
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    print(f"-------------------  {i}  ----------------------")
    print(lon, lat, date_str)

    if date_obj < datetime(2015, 4, 1):
        print(f"Row {i}: No SMAP data before April 2015, skipping.")
        continue

    # Try ±1 day range first
    start = date_obj - timedelta(days=1)
    end = date_obj + timedelta(days=1)
    collection = smap.filterDate(str(start.date()), str(end.date()))

    if collection.size().getInfo() == 0:
        # If no data in ±1 day, getting the last available before date
        print(f"Row {i}: No SMAP in ±1 day, fetching last recorded before {date_str}")
        collection = smap.filterDate("2015-04-01", str(date_obj.date())).sort("system:time_start", False)  # sort descending
        if collection.size().getInfo() == 0:
            print(f"Row {i}: Still no SMAP data, skipping.")
            continue

    filtered = collection.first()

    point = ee.Geometry.Point(lon, lat)
    data = filtered.reduceRegion(
        reducer=ee.Reducer.first(),
        geometry=point,
        scale=10000
    ).get("ssm").getInfo()

    if data is not None:
        ssm_fraction = data / 100
        ssm_date = ee.Date(filtered.get("system:time_start")).format("YYYY-MM-dd").getInfo()
        df.at[i, "ssm(m³/m³)"] = ssm_fraction
        df.at[i, "ssm_date"] = ssm_date
        print(f"Row {i}: SSM={ssm_fraction:.3f} m³/m³ from {ssm_date}")
    else:
        print(f"Row {i}: No soil moisture value for point.")

df.to_excel("subset_with_ssm.xlsx", index=False)
