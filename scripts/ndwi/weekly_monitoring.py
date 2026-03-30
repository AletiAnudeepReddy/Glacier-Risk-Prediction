import ee
import pandas as pd
import json
import datetime

ee.Initialize(project='glacier-risk-project')

# -------------------------------
# LOAD LAKES
# -------------------------------
with open("config/lakes.json") as f:
    lakes = json.load(f)

# -------------------------------
# LAST 20 WEEKS (LATEST DATA)
# -------------------------------
end_date = ee.Date(datetime.datetime.utcnow())
start_date = end_date.advance(-140, 'day')   # 20 weeks

results = []

# -------------------------------
# CLOUD + SNOW MASK (FIXED)
# -------------------------------
def mask_clouds(image):

    scl = image.select('SCL')

    mask = (
        scl.neq(3)   # shadow
        .And(scl.neq(8))   # cloud
        .And(scl.neq(9))
        .And(scl.neq(10))  # cirrus
        .And(scl.neq(11))  # snow
    )

    return image.updateMask(mask)

# -------------------------------
# PROCESS WEEK (NO MONTH SKIP)
# -------------------------------
def process_week(start, end, geometry, lake_id):

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR")
        .filterBounds(geometry)
        .filterDate(start, end)
        .map(mask_clouds)
    )

    # Skip if no images
    if collection.size().getInfo() == 0:
        return None

    image = collection.median().clip(geometry)

    # -------------------------------
    # WATER DETECTION (STABLE)
    # -------------------------------
    ndwi = image.normalizedDifference(['B3', 'B8'])
    mndwi = image.normalizedDifference(['B3', 'B11'])

    water = ndwi.gt(0.15).And(mndwi.gt(0.05))

    # -------------------------------
    # REMOVE NOISE
    # -------------------------------
    water = (
        water.updateMask(water)
        .connectedPixelCount(200, True)
        .gte(150)
    )

    # -------------------------------
    # AREA
    # -------------------------------
    area_img = water.multiply(ee.Image.pixelArea()).rename("area")

    stats = area_img.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=10,
        maxPixels=1e9
    )

    area = stats.get('area')

    if area is None:
        return None

    area_km2 = ee.Number(area).divide(1e6).getInfo()

    if area_km2 == 0:
        return None

    return {
        "lake_id": lake_id,
        "date": start.format("YYYY-MM-dd").getInfo(),
        "lake_area_km2": area_km2
    }

# -------------------------------
# MAIN LOOP
# -------------------------------
for lake in lakes:

    lake_id = lake["lake_id"]
    geometry = ee.Geometry.Rectangle(lake["bbox"])

    current = start_date

    while current.difference(end_date, 'day').getInfo() < 0:

        week_end = current.advance(7, 'day')

        try:
            result = process_week(current, week_end, geometry, lake_id)

            if result:
                results.append(result)

        except Exception as e:
            print(f"{lake_id} error: {e}")

        current = week_end

# -------------------------------
# DATAFRAME
# -------------------------------
df = pd.DataFrame(results)

if df.empty:
    print("⚠ No data extracted")
    exit()

df = df.drop_duplicates(subset=["lake_id", "date"])
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["lake_id", "date"])

# -------------------------------
# CLEAN PER LAKE
# -------------------------------
final_dfs = []

for lake_id, group in df.groupby("lake_id"):

    group = group.copy()

    # -------------------------------
    # REMOVE EXTREME OUTLIERS
    # -------------------------------
    q1 = group["lake_area_km2"].quantile(0.25)
    q3 = group["lake_area_km2"].quantile(0.75)
    iqr = q3 - q1

    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    group = group[
        (group["lake_area_km2"] >= lower) &
        (group["lake_area_km2"] <= upper)
    ]

    # -------------------------------
    # REMOVE SUDDEN SPIKES
    # -------------------------------
    group["diff"] = group["lake_area_km2"].diff().abs()

    group = group[
        (group["diff"] < group["lake_area_km2"].rolling(3).mean() * 1.5) |
        (group["diff"].isna())
    ]

    group = group.drop(columns=["diff"])

    # -------------------------------
    # INTERPOLATION (WEEKLY)
    # -------------------------------
    group = group.set_index("date")

    full_range = pd.date_range(
        start=group.index.min(),
        end=group.index.max(),
        freq="7D"
    )

    group = group.reindex(full_range)

    group["lake_id"] = lake_id

    group["lake_area_km2"] = (
        group["lake_area_km2"]
        .interpolate(method="linear", limit_direction="both")
        .bfill()
        .ffill()
    )

    # -------------------------------
    # SMOOTHING
    # -------------------------------
    group["lake_area_km2"] = (
        group["lake_area_km2"]
        .rolling(3, min_periods=1)
        .mean()
    )

    group = group.reset_index().rename(columns={"index": "date"})

    final_dfs.append(group)

df = pd.concat(final_dfs, ignore_index=True)

# -------------------------------
# SAVE
# -------------------------------
df.to_csv("data/continuous/weekly_ndwi_clean.csv", index=False)

print("🔥 FINAL CONTINUOUS NDWI PIPELINE READY (LATEST DATA)")