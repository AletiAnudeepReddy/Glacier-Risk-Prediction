import ee
import pandas as pd
import json

ee.Initialize(project='glacier-risk-project')

# Load lakes
with open("config/lakes.json") as f:
    lakes = json.load(f)

start_year = 2017
end_year = 2024

results = []

# -------------------------------
# Cloud Mask
# -------------------------------
def mask_clouds(image):
    qa = image.select('QA60')
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11

    mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(
           qa.bitwiseAnd(cirrus_bit_mask).eq(0))

    return image.updateMask(mask)

# -------------------------------
# Process Time Window (HALF MONTH)
# -------------------------------
def process_window(start, end, geometry, lake_id):

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR")
        .filterBounds(geometry)
        .filterDate(start, end)
        .map(mask_clouds)
    )

    if collection.size().getInfo() == 0:
        return None

    image = collection.median().clip(geometry)

    # Water indices
    ndwi = image.normalizedDifference(['B3','B8'])
    mndwi = image.normalizedDifference(['B3','B11'])

    water = ndwi.gt(0.1).And(mndwi.gt(0))

    # -------------------------------
    # CORRECT: Largest connected region
    # -------------------------------
    connected = water.selfMask().connectedComponents(
        connectedness=ee.Kernel.plus(1),
        maxSize=1000
    )

    pixel_count = connected.select('labels').connectedPixelCount(100, True)

    main_water = pixel_count.gte(200)

    # Area calculation
    area_img = main_water.multiply(ee.Image.pixelArea())

    stats = area_img.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=10,
        maxPixels=1e9
    )

    area = stats.get('labels')

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
    bbox = lake["bbox"]
    geometry = ee.Geometry.Rectangle(bbox)

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):

            # Skip winter months
            if month in [1, 2, 3]:
                continue

            try:
                # First half (1–15)
                start1 = ee.Date.fromYMD(year, month, 1)
                end1 = ee.Date.fromYMD(year, month, 15)

                result1 = process_window(start1, end1, geometry, lake_id)
                if result1:
                    results.append(result1)

                # Second half (16–end)
                start2 = ee.Date.fromYMD(year, month, 16)
                end2 = start2.advance(1, 'month')

                result2 = process_window(start2, end2, geometry, lake_id)
                if result2:
                    results.append(result2)

            except Exception as e:
                print(f"{lake_id} {year}-{month} error: {e}")

# -------------------------------
# DATAFRAME CLEANING
# -------------------------------
df = pd.DataFrame(results)

df = df.drop_duplicates(subset=["lake_id", "date"])

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["lake_id", "date"])

# -------------------------------
# CRITICAL PHYSICAL FILTER
# -------------------------------
df = df[(df["lake_area_km2"] > 0.5) & (df["lake_area_km2"] < 2.5)]

df.to_csv("data/raw/lake_area_timeseries.csv", index=False)

print("Final cleaned dataset saved!")