import ee
import pandas as pd
import json

ee.Initialize(project='glacier-risk-project')

# -------------------------------
# LOAD LAKES
# -------------------------------
with open("config/lakes.json") as f:
    lakes = json.load(f)

# -------------------------------
# LOAD NDWI DATA
# -------------------------------
lake_df = pd.read_csv("data/continuous/weekly_ndwi_final.csv")
lake_df["date"] = pd.to_datetime(lake_df["date"])

all_results = []

# -------------------------------
# PROCESS EACH LAKE
# -------------------------------
for lake in lakes:

    lake_id = lake["lake_id"]
    print(f"Processing {lake_id}...")

    geometry = ee.Geometry.Rectangle(lake["bbox"]).buffer(20000)
    bbox = lake["bbox"]
    centroid = ee.Geometry.Point([(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2])

    lake_data = lake_df[lake_df["lake_id"] == lake_id].copy()
    if lake_data.empty:
        continue

    # -------------------------------
    # DATE RANGE
    # -------------------------------
    start_date = lake_data["date"].min().strftime("%Y-%m-%d")
    end_date = (lake_data["date"].max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"{lake_id} date range: {start_date} to {end_date}")

    # -------------------------------
    # ERA5 DAILY
    # -------------------------------
    era5 = (
        ee.ImageCollection("ECMWF/ERA5_LAND/DAILY_AGGR")
        .filterDate(start_date, end_date)
        .filterBounds(geometry)
    )

    print(f"{lake_id} ERA5 collection size: {era5.size().getInfo()}")

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

    features = era5.map(extract)
    data = features.getInfo()

    climate_rows = []
    if "features" in data:
        for f in data["features"]:
            props = f["properties"]
            climate_rows.append({
                "date": pd.to_datetime(props["date"]),
                "temperature": props.get("temperature"),
                "precipitation": props.get("precipitation")
            })

    climate_df = pd.DataFrame(climate_rows)
    print(f"{lake_id} daily rows:", len(climate_df))

    if climate_df.empty:
        lake_data["temperature"] = None
        lake_data["precipitation"] = None
        all_results.append(lake_data[["date", "lake_id", "lake_area_km2", "temperature", "precipitation"]])
        continue

    climate_df["temperature"] = pd.to_numeric(climate_df["temperature"], errors="coerce") - 273.15
    climate_df["precipitation"] = pd.to_numeric(climate_df["precipitation"], errors="coerce") * 1000
    climate_df = climate_df.sort_values("date").reset_index(drop=True)

    # -------------------------------
    # AGGREGATE DAILY TO WEEKLY
    # -------------------------------
    weekly_rows = []
    for week_date in sorted(lake_data["date"].unique()):
        week_start = week_date - pd.Timedelta(days=6)
        mask = (climate_df["date"] >= week_start) & (climate_df["date"] <= week_date)
        week_df = climate_df[mask]
        weekly_rows.append({
            "date": week_date,
            "lake_id": lake_id,
            "temperature": week_df["temperature"].mean() if not week_df.empty else None,
            "precipitation": week_df["precipitation"].sum() if not week_df.empty else None
        })

    weekly_df = pd.DataFrame(weekly_rows)
    print(f"{lake_id} weekly rows:", len(weekly_df))

    merged = pd.merge(
        lake_data[["date", "lake_id", "lake_area_km2"]],
        weekly_df,
        on=["date", "lake_id"],
        how="left"
    )

    merged["temperature"] = merged.groupby("lake_id")["temperature"].transform(
        lambda x: x.interpolate(limit_direction="both").bfill().ffill()
    )
    merged["precipitation"] = merged.groupby("lake_id")["precipitation"].transform(
        lambda x: x.interpolate(limit_direction="both").bfill().ffill()
    )

    all_results.append(merged)

# -------------------------------
# FINAL CONCAT
# -------------------------------
if len(all_results) == 0:
    raise ValueError("❌ No data generated")

final_df = pd.concat(all_results, ignore_index=True)
final_df = final_df[["date", "lake_id", "lake_area_km2", "temperature", "precipitation"]]
final_df.to_csv("data/continuous/with_climate/all_lakes_with_climate.csv", index=False)
print("🔥 FINAL DATASET READY")