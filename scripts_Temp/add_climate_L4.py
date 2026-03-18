import ee
import pandas as pd

ee.Initialize(project='glacier-risk-project')

# -------------------------------
# LAKE CONFIG (L4 - Lumding)
# -------------------------------
lake_id = "L4"

bbox = [86.58, 27.74, 86.67, 27.82]
geometry = ee.Geometry.Rectangle(bbox)

start_date = '2015-01-01'
end_date = '2021-12-31'

# -------------------------------
# LOAD LAKE DATA
# -------------------------------
lake_df = pd.read_csv("data/raw/L4_lumding_clean.csv")
lake_df["date"] = pd.to_datetime(lake_df["date"])

# -------------------------------
# ERA5 DATASET
# -------------------------------
era5 = (
    ee.ImageCollection("ECMWF/ERA5_LAND/MONTHLY")
    .filterDate(start_date, end_date)
    .filterBounds(geometry)
)

# -------------------------------
# EXTRACT CLIMATE DATA
# -------------------------------
def extract_climate(image):

    temp = image.select('temperature_2m')
    precip = image.select('total_precipitation')

    stats = image.addBands(temp).addBands(precip).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geometry,
        scale=10000,
        maxPixels=1e9
    )

    return ee.Feature(None, {
        "date": image.date().format("YYYY-MM-dd"),
        "temperature": ee.Number(stats.get("temperature_2m")).subtract(273.15),
        "precipitation": ee.Number(stats.get("total_precipitation")).multiply(1000)
    })

features = era5.map(extract_climate)
data = features.getInfo()

# -------------------------------
# CONVERT TO DATAFRAME
# -------------------------------
climate_data = []

for f in data["features"]:
    props = f["properties"]

    climate_data.append({
        "date": props["date"],
        "temperature": props["temperature"],
        "precipitation": props["precipitation"]
    })

climate_df = pd.DataFrame(climate_data)
climate_df["date"] = pd.to_datetime(climate_df["date"])

# -------------------------------
# MERGE WITH LAKE DATA
# -------------------------------
df = pd.merge(
    lake_df,
    climate_df,
    on="date",
    how="left"
)

# -------------------------------
# FINAL CLEANING
# -------------------------------
df = df.sort_values("date")

df["temperature"] = df["temperature"].interpolate().bfill().ffill()
df["precipitation"] = df["precipitation"].interpolate().bfill().ffill()

# -------------------------------
# SAVE
# -------------------------------
output_path = "data/with_temperature/L4_with_climate.csv"
df.to_csv(output_path, index=False)

print(f"✅ L4 climate dataset saved at: {output_path}")