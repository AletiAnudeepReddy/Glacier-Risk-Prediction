import ee
import pandas as pd
import json

ee.Initialize(project='glacier-risk-project')

# Load lakes
with open("config/lakes.json") as f:
    lakes = json.load(f)

start_date = '2015-01-01'
end_date = '2024-01-01'

results = []

# -------------------------------
# PROCESS JRC IMAGE
# -------------------------------
def process_image(image, geometry, lake_id):

    water = image.select('water')

    # water == 2 → water pixels
    water_mask = water.eq(2)

    area_img = water_mask.multiply(ee.Image.pixelArea())

    stats = area_img.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=30,
        maxPixels=1e9
    )

    area = stats.get('water')

    if area is None:
        return None

    area_km2 = ee.Number(area).divide(1e6).getInfo()

    return {
        "lake_id": lake_id,
        "date": image.date().format("YYYY-MM-dd").getInfo(),
        "lake_area_km2": area_km2
    }

# -------------------------------
# MAIN LOOP
# -------------------------------
for lake in lakes:

    lake_id = lake["lake_id"]
    bbox = lake["bbox"]
    geometry = ee.Geometry.Rectangle(bbox)

    collection = (
        ee.ImageCollection("JRC/GSW1_4/MonthlyHistory")
        .filterDate(start_date, end_date)
        .filterBounds(geometry)
    )

    images = collection.toList(collection.size())

    for i in range(collection.size().getInfo()):

        try:
            image = ee.Image(images.get(i))

            result = process_image(image, geometry, lake_id)

            if result:
                results.append(result)

        except Exception as e:
            print(f"{lake_id} error: {e}")

# -------------------------------
# DATAFRAME PROCESSING
# -------------------------------
df = pd.DataFrame(results)

df = df.drop_duplicates(subset=["lake_id", "date"])

df["date"] = pd.to_datetime(df["date"])

df = df.sort_values(["lake_id", "date"])

# Remove only noise
df = df[df["lake_area_km2"] > 0.5]

# -------------------------------
# CREATE FULL DATE RANGE PER LAKE
# -------------------------------
final_dfs = []

for lake_id, group in df.groupby("lake_id"):

    group = group.set_index("date")

    # Create full monthly range
    full_range = pd.date_range(
        start=group.index.min(),
        end=group.index.max(),
        freq="MS"
    )

    group = group.reindex(full_range)

    group["lake_id"] = lake_id

    # -------------------------------
    # INTERPOLATION (CORRECT)
    # -------------------------------
    group["lake_area_km2"] = group["lake_area_km2"].interpolate(
        method="linear",
        limit_direction="both"
    )

    # Final fill (edge safety)
    group["lake_area_km2"] = group["lake_area_km2"].bfill().ffill()

    group = group.reset_index().rename(columns={"index": "date"})

    final_dfs.append(group)

# Combine all lakes
df = pd.concat(final_dfs, ignore_index=True)

# Sort again
df = df.sort_values(["lake_id", "date"])

# Save
df.to_csv("data/raw/L1_imja_tsho_clean.csv", index=False)

print("✅ Final CLEAN dataset created successfully!")