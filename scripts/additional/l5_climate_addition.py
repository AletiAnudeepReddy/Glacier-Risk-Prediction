import ee
import pandas as pd

ee.Initialize(project='glacier-risk-project')

# -------------------------------
# LAKE CONFIG (L5 ONLY)
# -------------------------------
lake = {
    "lake_id": "L5",
    "name": "Gokyo Lake",
    "bbox": [86.67, 27.95, 86.72, 27.99]
}

lake_id = lake["lake_id"]

geometry = ee.Geometry.Rectangle(lake["bbox"]).buffer(5000)
bbox = lake["bbox"]
centroid = ee.Geometry.Point([
    (bbox[0] + bbox[2]) / 2,
    (bbox[1] + bbox[3]) / 2
])

# -------------------------------
# LOAD NDWI DATA (L5 ONLY)
# -------------------------------
lake_df = pd.read_csv("data/additional/L5_weekly_ndwi.csv")
lake_df["date"] = pd.to_datetime(lake_df["date"])

if lake_df.empty:
    raise ValueError("❌ No NDWI data for L5")

# -------------------------------
# DATE RANGE
# -------------------------------
start_date = lake_df["date"].min().strftime("%Y-%m-%d")
end_date = (lake_df["date"].max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

print(f"L5 date range: {start_date} → {end_date}")

# -------------------------------
# ERA5 COLLECTION
# -------------------------------
era5 = (
    ee.ImageCollection("ECMWF/ERA5_LAND/DAILY_AGGR")
    .filterDate(start_date, end_date)
    .filterBounds(geometry)
)

print("ERA5 size:", era5.size().getInfo())

# -------------------------------
# EXTRACT DAILY CLIMATE
# -------------------------------
def extract(image):

    stats = image.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geometry,
        scale=10000,
        maxPixels=1e9
    )

    fallback = image.reduceRegion(
        reducer=ee.Reducer.first(),
        geometry=centroid,
        scale=10000
    )

    temp = ee.Algorithms.If(
        ee.Algorithms.IsEqual(stats.get("temperature_2m"), None),
        fallback.get("temperature_2m"),
        stats.get("temperature_2m")
    )

    precip = ee.Algorithms.If(
        ee.Algorithms.IsEqual(stats.get("total_precipitation_sum"), None),
        fallback.get("total_precipitation_sum"),
        stats.get("total_precipitation_sum")
    )

    return ee.Feature(None, {
        "date": image.date().format("YYYY-MM-dd"),
        "temperature": temp,
        "precipitation": precip
    })


features = era5.map(extract).getInfo()

# -------------------------------
# CONVERT TO DATAFRAME
# -------------------------------
rows = []
for f in features["features"]:
    props = f["properties"]

    rows.append({
        "date": pd.to_datetime(props["date"]),
        "temperature": props.get("temperature"),
        "precipitation": props.get("precipitation")
    })

climate_df = pd.DataFrame(rows)

print("Daily rows:", len(climate_df))

if climate_df.empty:
    lake_df["temperature"] = None
    lake_df["precipitation"] = None
else:
    # Convert units
    climate_df["temperature"] = (
        pd.to_numeric(climate_df["temperature"], errors="coerce") - 273.15
    )

    climate_df["precipitation"] = (
        pd.to_numeric(climate_df["precipitation"], errors="coerce")
        .fillna(0) * 1000
    )

    climate_df = climate_df.sort_values("date").reset_index(drop=True)

    # -------------------------------
    # WEEKLY AGGREGATION
    # -------------------------------
    weekly_rows = []

    for week_date in sorted(lake_df["date"].unique()):
        week_start = week_date - pd.Timedelta(days=6)

        week_df = climate_df[
            (climate_df["date"] >= week_start) &
            (climate_df["date"] <= week_date)
        ]

        weekly_rows.append({
            "date": week_date,
            "lake_id": lake_id,
            "temperature": week_df["temperature"].mean() if not week_df.empty else None,
            "precipitation": week_df["precipitation"].sum() if not week_df.empty else None
        })

    weekly_df = pd.DataFrame(weekly_rows)

    # -------------------------------
    # MERGE
    # -------------------------------
    merged = pd.merge(
        lake_df,
        weekly_df,
        on=["date", "lake_id"],
        how="left"
    )

    # Fill missing
    merged["temperature"] = merged["temperature"].interpolate().bfill().ffill()
    merged["precipitation"] = merged["precipitation"].interpolate().bfill().ffill()

# -------------------------------
# SAVE
# -------------------------------
output_path = "data/additional/L5_with_climate.csv"

merged.to_csv(output_path, index=False)

print("🔥 L5 climate data added successfully")