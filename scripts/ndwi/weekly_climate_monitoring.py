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

# 🔥 KEY FIX: month column
lake_df["month"] = lake_df["date"].dt.to_period("M").dt.to_timestamp()

all_results = []

# -------------------------------
# PROCESS EACH LAKE
# -------------------------------
for lake in lakes:

    lake_id = lake["lake_id"]
    print(f"Processing {lake_id}...")

    geometry = ee.Geometry.Rectangle(lake["bbox"]).buffer(20000)

    lake_data = lake_df[lake_df["lake_id"] == lake_id].copy()

    if lake_data.empty:
        continue

    # -------------------------------
    # DATE RANGE (MONTH BASED)
    # -------------------------------
    start_date = lake_data["month"].min().strftime("%Y-%m-%d")
    end_date   = (lake_data["month"].max() + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")

    # -------------------------------
    # ERA5 MONTHLY
    # -------------------------------
    era5 = (
        ee.ImageCollection("ECMWF/ERA5_LAND/MONTHLY_AGGR")
        .filterDate(start_date, end_date)
        .filterBounds(geometry)
    )

    print(f"{lake_id} ERA5 collection size: {era5.size().getInfo()}")

    # -------------------------------
    # EXTRACT CLIMATE (MONTHLY)
    # -------------------------------
    def extract(image):

        stats = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geometry,
            scale=10000,
            maxPixels=1e9
        )

        # If reduceRegion fails (null), use centroid sampling
        temp = stats.get("temperature_2m")
        precip = stats.get("total_precipitation_sum")
        
        if temp is None or precip is None:
            centroid = geometry.centroid()
            point_stats = image.reduceRegion(
                reducer=ee.Reducer.first(),
                geometry=centroid,
                scale=10000
            )
            temp = point_stats.get("temperature_2m")
            precip = point_stats.get("total_precipitation_sum")

        return ee.Feature(None, {
            "date": image.date().format("YYYY-MM-dd"),
            "temperature": ee.Number(temp).subtract(273.15),
            "precipitation": ee.Number(precip).multiply(1000)
        })

    features = era5.map(extract)
    data = features.getInfo()

    print(f"{lake_id} features extracted: {len(data.get('features', []))}")

    climate_rows = []

    if "features" in data:
        for f in data["features"]:
            props = f["properties"]

            climate_rows.append({
                "month": pd.to_datetime(props["date"]).to_period("M").to_timestamp(),
                "temperature": props.get("temperature"),
                "precipitation": props.get("precipitation")
            })

    climate_df = pd.DataFrame(climate_rows)

    print(f"{lake_id} climate rows:", len(climate_df))

    # -------------------------------
    # HANDLE EMPTY CLIMATE (IMPORTANT)
    # -------------------------------
    if climate_df.empty:
        lake_data["temperature"] = None
        lake_data["precipitation"] = None
        all_results.append(lake_data)
        continue

    climate_df["temperature"] = pd.to_numeric(climate_df["temperature"], errors="coerce")
    climate_df["precipitation"] = pd.to_numeric(climate_df["precipitation"], errors="coerce")

    # Sort climate_df by month
    climate_df = climate_df.sort_values("month").reset_index(drop=True)

    # -------------------------------
    # INTERPOLATE WEEKLY CLIMATE
    # -------------------------------
    unique_dates = lake_data["date"].unique()
    interpolated_climate = []

    for date in unique_dates:
        month = date.to_period("M").to_timestamp()
        idx = climate_df[climate_df["month"] == month].index
        if len(idx) > 0:
            i = idx[0]
            if i < len(climate_df) - 1:
                next_month = climate_df.iloc[i+1]["month"]
                days_in_period = (next_month - month).days
                day_of_month = date.day
                fraction = (day_of_month - 1) / days_in_period
                temp = climate_df.iloc[i]["temperature"] + fraction * (climate_df.iloc[i+1]["temperature"] - climate_df.iloc[i]["temperature"])
                precip = climate_df.iloc[i]["precipitation"] + fraction * (climate_df.iloc[i+1]["precipitation"] - climate_df.iloc[i]["precipitation"])
            else:
                temp = climate_df.iloc[i]["temperature"]
                precip = climate_df.iloc[i]["precipitation"]
        else:
            temp = None
            precip = None

        interpolated_climate.append({
            "date": date,
            "temperature": temp,
            "precipitation": precip
        })

    climate_df = pd.DataFrame(interpolated_climate)

    # -------------------------------
    # MERGE USING DATE
    # -------------------------------
    merged = pd.merge(
        lake_data,
        climate_df,
        on="date",
        how="left"
    )

    # -------------------------------
    # FILL MISSING VALUES
    # -------------------------------
    merged["temperature"] = (
        merged["temperature"]
        .interpolate(limit_direction="both")
        .bfill()
        .ffill()
    )

    merged["precipitation"] = (
        merged["precipitation"]
        .interpolate(limit_direction="both")
        .bfill()
        .ffill()
    )

    merged = merged

    all_results.append(merged)

# -------------------------------
# FINAL CONCAT
# -------------------------------
if len(all_results) == 0:
    raise ValueError("❌ No data generated")

final_df = pd.concat(all_results, ignore_index=True)

# -------------------------------
# SAVE
# -------------------------------
output_path = "data/continuous/with_climate/all_lakes_with_climate.csv"
final_df.to_csv(output_path, index=False)

print("🔥 FINAL DATASET READY")