import ee
import pandas as pd
import datetime
import numpy as np

ee.Initialize(project='glacier-risk-project')

# -------------------------------
# L5 CONFIG (GOKYO LAKE)
# -------------------------------
lake = {
    "lake_id": "L5",
    "name": "Gokyo Lake",
    "bbox": [86.67, 27.95, 86.72, 27.99],
    "area_range": [0.3, 2.5]
}

geometry = ee.Geometry.Rectangle(lake["bbox"])
lake_id = lake["lake_id"]

# -------------------------------
# DATE RANGE
# -------------------------------
end_date = ee.Date(datetime.datetime.utcnow())
start_date = end_date.advance(-140, 'day')

results = []

# -------------------------------
# CLOUD + SNOW MASK
# -------------------------------
def mask_clouds(image):
    scl = image.select('SCL')
    mask = (
        scl.neq(3)
        .And(scl.neq(8))
        .And(scl.neq(9))
        .And(scl.neq(10))
        .And(scl.neq(11))
    )
    return image.updateMask(mask)

# -------------------------------
# PROCESS WEEK (EXACT SAME LOGIC)
# -------------------------------
def process_week(start, end):

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR")
        .filterBounds(geometry)
        .filterDate(start, end)
        .map(mask_clouds)
    )

    # HANDLE EMPTY (same as original)
    if collection.size().getInfo() == 0:
        return {
            "lake_id": lake_id,
            "date": start.format("YYYY-MM-dd").getInfo(),
            "lake_area_km2": None
        }

    image = collection.median().clip(geometry)

    ndwi = image.normalizedDifference(['B3', 'B8'])
    mndwi = image.normalizedDifference(['B3', 'B11'])

    # SAME THRESHOLDS (DO NOT CHANGE)
    water = ndwi.gt(0.15).And(mndwi.gt(0.05))

    # SAME PIXEL FILTER
    water = (
        water.updateMask(water)
        .connectedPixelCount(200, True)
        .gte(150)
    )

    # 🔥 CRITICAL: SAME rename("area")
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

    if area_km2 == 0:
        return {
            "lake_id": lake_id,
            "date": start.format("YYYY-MM-dd").getInfo(),
            "lake_area_km2": None
        }

    return {
        "lake_id": lake_id,
        "date": start.format("YYYY-MM-dd").getInfo(),
        "lake_area_km2": area_km2
    }

# -------------------------------
# LOOP
# -------------------------------
current = start_date

while current.millis().getInfo() < end_date.millis().getInfo():

    week_end = current.advance(7, 'day')

    try:
        result = process_week(current, week_end)
        results.append(result)
    except Exception as e:
        print(f"L5 error: {e}")

    current = week_end

# -------------------------------
# DATAFRAME
# -------------------------------
df = pd.DataFrame(results)

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

# -------------------------------
# CLEANING (SAME AS ORIGINAL)
# -------------------------------

# REMOVE OUTLIERS
q1 = df["lake_area_km2"].quantile(0.25)
q3 = df["lake_area_km2"].quantile(0.75)
iqr = q3 - q1

lower = q1 - 1.5 * iqr
upper = q3 + 1.5 * iqr

df = df[
    (df["lake_area_km2"] >= lower) &
    (df["lake_area_km2"] <= upper)
]

# REMOVE SPIKES
df["diff"] = df["lake_area_km2"].diff().abs()

df = df[
    (df["diff"] < df["lake_area_km2"].rolling(3).mean() * 1.5) |
    (df["diff"].isna())
]

df = df.drop(columns=["diff"])

# REINDEX FULL RANGE
df = df.set_index("date")

full_range = pd.date_range(
    start=df.index.min(),
    end=pd.to_datetime(end_date.getInfo()['value'], unit='ms'),
    freq="7D"
)

df = df.reindex(full_range)
df["lake_id"] = lake_id

# INTERPOLATE
df["lake_area_km2"] = (
    df["lake_area_km2"]
    .interpolate(method="linear")
    .ffill()
    .bfill()
)

# REMOVE UNREALISTIC JUMPS (>0.25)
values = df["lake_area_km2"].values.astype(float)
max_change = 0.25

for i in range(1, len(values)):
    if abs(values[i] - values[i-1]) > max_change:
        values[i] = np.nan

df["lake_area_km2"] = values

# INTERPOLATE AGAIN
df["lake_area_km2"] = (
    df["lake_area_km2"]
    .interpolate(method="linear")
    .ffill()
    .bfill()
)

# SMOOTH
df["lake_area_km2"] = (
    df["lake_area_km2"]
    .rolling(3, min_periods=1)
    .mean()
)

df = df.reset_index().rename(columns={"index": "date"})

# -------------------------------
# SAVE (SEPARATE PATH)
# -------------------------------
output_path = "data/additional/L5_weekly_ndwi.csv"
df.to_csv(output_path, index=False)

print(f"🔥 L5 NDWI MONITORING DONE → {output_path}")