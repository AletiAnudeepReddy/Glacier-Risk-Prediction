import ee
import pandas as pd

ee.Initialize(project='glacier-risk-project')

# -------------------------------
# LAKE CONFIG (L4 - Lumding)
# -------------------------------
lake_id = "L4"
lake_name = "Lumding"

# Correct bbox (keep slightly larger for safety)
bbox = [86.58, 27.74, 86.67, 27.82]
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

if df.empty:
    print("❌ No data extracted — check bbox")
    exit()

df = df.drop_duplicates(subset=["lake_id", "date"])

df["date"] = pd.to_datetime(df["date"])

df = df.sort_values("date")

# -------------------------------
# 🔍 DEBUG FIRST (IMPORTANT)
# -------------------------------
print("\n📊 RAW DATA STATS:")
print("Min:", df["lake_area_km2"].min())
print("Max:", df["lake_area_km2"].max())
print(df.head(10))

# -------------------------------
# 🔥 FILTER (ADJUST AFTER CHECK)
# -------------------------------
df = df[(df["lake_area_km2"] > 0.4) & (df["lake_area_km2"] < 2.5)]
if df.empty:
    print("❌ All data removed after filtering — adjust thresholds")
    exit()

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
output_path = "data/raw/L4_lumding_clean.csv"
df.to_csv(output_path, index=False)

print(f"\n✅ L4 (Lumding) dataset saved at: {output_path}")