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
# LAST 20 WEEKS
# -------------------------------
end_date = ee.Date(datetime.datetime.utcnow())
start_date = end_date.advance(-140, 'day')

results = []

# -------------------------------
# CLOUD MASK USING SCL (CORRECT)
# -------------------------------
def mask_clouds(image):

    scl = image.select('SCL')

    # Keep only clear pixels
    mask = scl.neq(3) \
        .And(scl.neq(8)) \
        .And(scl.neq(9)) \
        .And(scl.neq(10)) \
        .And(scl.neq(11))

    return image.updateMask(mask)

# -------------------------------
# PROCESS WEEK
# -------------------------------
def process_week(start, end, geometry, lake_id):

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR")
        .filterBounds(geometry)
        .filterDate(start, end)
        .map(mask_clouds)
    )

    if collection.size().getInfo() == 0:
        return None

    image = collection.median().clip(geometry)

    # -------------------------------
    # WATER DETECTION (TUNED)
    # -------------------------------
    ndwi = image.normalizedDifference(['B3', 'B8'])
    mndwi = image.normalizedDifference(['B3', 'B11'])

    water = ndwi.gt(0.1).And(mndwi.gt(0))

    # -------------------------------
    # REMOVE SMALL NOISE
    # -------------------------------
    water = water.updateMask(water) \
        .connectedPixelCount(100, True) \
        .gte(50)

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
    print("⚠ No data extracted — check thresholds or bbox")
    exit()

df = df.drop_duplicates(subset=["lake_id", "date"])

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["lake_id", "date"])

# -------------------------------
# PER-LAKE OUTLIER REMOVAL
# -------------------------------
final_dfs = []

for lake_id, group in df.groupby("lake_id"):

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
    # INTERPOLATION
    # -------------------------------
    group = group.set_index("date")

    full_range = pd.date_range(
        start=group.index.min(),
        end=group.index.max(),
        freq="7D"
    )

    group = group.reindex(full_range)

    group["lake_id"] = lake_id

    group["lake_area_km2"] = group["lake_area_km2"] \
        .interpolate(method="linear", limit_direction="both") \
        .bfill().ffill()

    group = group.reset_index().rename(columns={"index": "date"})

    final_dfs.append(group)

df = pd.concat(final_dfs, ignore_index=True)

# -------------------------------
# SAVE
# -------------------------------
df.to_csv("data/continuous/weekly_ndwi_data.csv", index=False)

print("🔥 FINAL NDWI monitoring dataset ready!")