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
# DATE RANGE
# -------------------------------
end_date = ee.Date("2026-03-30")
start_date = end_date.advance(-140, 'day')

results = []

# -------------------------------
# CLOUD MASK (SAFE)
# -------------------------------
def mask_s2(image):
    scl = image.select('SCL')
    mask = (
        scl.neq(3)
        .And(scl.neq(8))
        .And(scl.neq(9))
        .And(scl.neq(10))
    )
    return image.updateMask(mask)

# -------------------------------
# PROCESS WINDOW
# -------------------------------
def process_window(start, end, geometry, lake):

    lake_id = lake["lake_id"]
    min_area, max_area = lake["area_range"]

    # -------------------------------
    # SENTINEL-2 (NDWI)
    # -------------------------------
    s2 = (
        ee.ImageCollection("COPERNICUS/S2_SR")
        .filterBounds(geometry)
        .filterDate(start, end)
        .map(mask_s2)
        .median()
    )

    ndwi = s2.normalizedDifference(['B3', 'B8'])
    water_ndwi = ndwi.gt(0.15)

    # -------------------------------
    # SENTINEL-1 (SAFE)
    # -------------------------------
    s1_collection = (
        ee.ImageCollection("COPERNICUS/S1_GRD")
        .filterBounds(geometry)
        .filterDate(start.advance(-3, 'day'), end.advance(3, 'day'))
        .filter(ee.Filter.eq('instrumentMode', 'IW'))
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))
        .select('VV')
    )

    s1_size = s1_collection.size().getInfo()

    if s1_size == 0:
        water_s1 = None
    else:
        s1 = s1_collection.median()
        water_s1 = s1.lt(-17)

    # -------------------------------
    # CONTROLLED FUSION (FIXED)
    # -------------------------------
    if water_s1 is None:
        water = water_ndwi
    else:
        water = (
            water_ndwi.And(water_s1)
            .Or(water_ndwi.And(ndwi.gt(0.2)))
        )

    # -------------------------------
    # NOISE FILTER
    # -------------------------------
    water = (
        water.updateMask(water)
        .connectedPixelCount(50, True)
        .gte(30)
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
        return {
            "lake_id": lake_id,
            "date": start.format("YYYY-MM-dd").getInfo(),
            "lake_area_km2": None
        }

    area_km2 = ee.Number(area).divide(1e6).getInfo()

    # -------------------------------
    # SOFT CLAMP (FIXED)
    # -------------------------------
    if area_km2 > max_area:
        area_km2 = max_area - 0.05
    if area_km2 < min_area:
        area_km2 = min_area

    return {
        "lake_id": lake_id,
        "date": start.format("YYYY-MM-dd").getInfo(),
        "lake_area_km2": area_km2
    }

# -------------------------------
# MAIN LOOP
# -------------------------------
for lake in lakes:

    # shrink bbox slightly (important fix)
    geometry = ee.Geometry.Rectangle(lake["bbox"]).buffer(-200)

    current = start_date

    while current.difference(end_date, 'day').getInfo() < 0:

        window_end = current.advance(7, 'day')

        try:
            result = process_window(current, window_end, geometry, lake)
            results.append(result)
        except Exception as e:
            print(f"{lake['lake_id']} error: {e}")

        current = window_end

# -------------------------------
# DATAFRAME
# -------------------------------
df = pd.DataFrame(results)

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["lake_id", "date"])

# -------------------------------
# POST PROCESSING
# -------------------------------
final_dfs = []

for lake_id, group in df.groupby("lake_id"):

    group = group.copy()

    # fill missing values
    group["lake_area_km2"] = group["lake_area_km2"].ffill()

    values = group["lake_area_km2"].values

    # -------------------------------
    # RATE LIMIT (PHYSICS)
    # -------------------------------
    max_change = 0.15

    for i in range(1, len(values)):
        if values[i] is None:
            values[i] = values[i-1]
            continue

        change = values[i] - values[i-1]

        if abs(change) > max_change:
            values[i] = values[i-1] + max_change * (1 if change > 0 else -1)

    # -------------------------------
    # EXPONENTIAL SMOOTHING
    # -------------------------------
    alpha = 0.3
    smoothed = []

    for i, val in enumerate(values):
        if i == 0:
            smoothed.append(val)
        else:
            smoothed.append(alpha * val + (1 - alpha) * smoothed[-1])

    group["lake_area_km2"] = smoothed

    final_dfs.append(group)

df = pd.concat(final_dfs, ignore_index=True)

# -------------------------------
# SAVE
# -------------------------------
df.to_csv("data/continuous/fusion_weekly_final.csv", index=False)

print("🔥 FINAL CORRECTED FUSION PIPELINE READY")