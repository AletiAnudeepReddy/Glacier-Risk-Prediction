import ee
import pandas as pd

ee.Initialize(project='glacier-risk-project')

# -------------------------------
# L5 CONFIG
# -------------------------------
lake = {
    "lake_id": "L5",
    "name": "Gokyo Lake",
    "bbox": [86.67, 27.95, 86.72, 27.99],
    "area_range": [0.3, 2.5]
}

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
# MAIN (ONLY L5)
# -------------------------------
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

# Remove only noise (same as your code)
df = df[df["lake_area_km2"] > 0.5]

# -------------------------------
# CREATE FULL DATE RANGE
# -------------------------------
df = df.set_index("date")

full_range = pd.date_range(
    start=df.index.min(),
    end=df.index.max(),
    freq="MS"
)

df = df.reindex(full_range)

df["lake_id"] = lake_id

# -------------------------------
# INTERPOLATION (SAME LOGIC)
# -------------------------------
df["lake_area_km2"] = df["lake_area_km2"].interpolate(
    method="linear",
    limit_direction="both"
)

df["lake_area_km2"] = df["lake_area_km2"].bfill().ffill()

df = df.reset_index().rename(columns={"index": "date"})

# -------------------------------
# SAVE
# -------------------------------
df.to_csv("data/raw/L5_gokyo_clean.csv", index=False)

print("✅ L5 dataset created successfully!")