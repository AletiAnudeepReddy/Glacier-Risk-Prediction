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

# --- Cloud Mask Function ---
def mask_clouds(image):
    qa = image.select('QA60')

    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11

    mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(
           qa.bitwiseAnd(cirrus_bit_mask).eq(0))

    return image.updateMask(mask)

# --- Process each month ---
def process_month(year, month, geometry, lake_id):

    # Skip winter months (snow + frozen lake)
    if month in [1, 2, 3]:
        return None

    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, 'month')

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR")
        .filterBounds(geometry)
        .filterDate(start, end)
        .map(mask_clouds)
    )

    if collection.size().getInfo() == 0:
        return None

    image = collection.median().clip(geometry)

    # NDWI
    ndwi = image.normalizedDifference(['B3','B8'])

    # MNDWI
    mndwi = image.normalizedDifference(['B3','B11'])

    # Stronger water condition
    water = ndwi.gt(0.1).And(mndwi.gt(0))

    # ---- KEY FIX: Keep only largest water body ----
    connected = water.selfMask().connectedComponents(
        connectedness=ee.Kernel.plus(1),
        maxSize=1000
    )

    largest_label = connected.select('labels').reduceRegion(
        reducer=ee.Reducer.mode(),
        geometry=geometry,
        scale=10,
        maxPixels=1e9
    ).get('labels')

    main_water = connected.select('labels').eq(largest_label)

    # ---- Area calculation ----
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
        "date": f"{year}-{month:02d}-01",
        "lake_area_km2": area_km2
    }

# --- Main Loop ---
for lake in lakes:

    lake_id = lake["lake_id"]
    lat = lake["lat"]
    lon = lake["lon"]
    buffer = 5000  # Increased buffer

    geometry = ee.Geometry.Rectangle([86.90, 27.88, 86.96, 27.92])

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):

            try:
                result = process_month(year, month, geometry, lake_id)

                if result:
                    results.append(result)

            except Exception as e:
                print(f"{lake_id} {year}-{month} error: {e}")

# Convert to dataframe
df = pd.DataFrame(results)

# Remove duplicates
df = df.drop_duplicates(subset=["lake_id","date"])

# Sort
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["lake_id","date"])

df.to_csv("data/raw/lake_area_timeseries.csv", index=False)

print("Final clean dataset saved!")