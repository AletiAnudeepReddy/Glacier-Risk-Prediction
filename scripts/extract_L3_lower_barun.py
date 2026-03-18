import ee
import pandas as pd

ee.Initialize(project='glacier-risk-project')

# -------------------------------
# LAKE CONFIG (ONLY L3)
# -------------------------------
lake_id = "L3"
lake_name = "Lower Barun"

# Tight bbox (important for small lake)
bbox = [87.06, 27.80, 87.12, 27.86]
geometry = ee.Geometry.Rectangle(bbox)

start_date = '2015-01-01'
end_date = '2024-01-01'

results = []

# -------------------------------
# PROCESS IMAGE
# -------------------------------
def process_image(image):

    water = image.select('water')
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
# LOAD DATA
# -------------------------------
collection = (
    ee.ImageCollection("JRC/GSW1_4/MonthlyHistory")
    .filterDate(start_date, end_date)
    .filterBounds(geometry)
)

images = collection.toList(collection.size())

print(f"Total images found: {collection.size().getInfo()}")

# -------------------------------
# EXTRACT DATA
# -------------------------------
for i in range(collection.size().getInfo()):

    try:
        image = ee.Image(images.get(i))

        result = process_image(image)

        if result:
            results.append(result)

    except Exception as e:
        print(f"Error at index {i}: {e}")

# -------------------------------
# DATAFRAME PROCESSING
# -------------------------------
df = pd.DataFrame(results)

df = df.drop_duplicates(subset=["lake_id", "date"])

df["date"] = pd.to_datetime(df["date"])

df = df.sort_values("date")

# -------------------------------
# 🔥 IMPORTANT FILTER (L3 specific)
# -------------------------------
df = df[(df["lake_area_km2"] > 0.5) & (df["lake_area_km2"] < 1.5)]
# -------------------------------
# CREATE FULL TIMELINE
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
# INTERPOLATION
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
output_path = "data/raw/L3_lower_barun_clean.csv"
df.to_csv(output_path, index=False)

print(f"✅ L3 dataset saved at: {output_path}")