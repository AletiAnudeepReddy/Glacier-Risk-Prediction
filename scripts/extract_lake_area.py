import ee
import pandas as pd
import json

ee.Initialize(project='glacier-risk-project')

# Load lake config
with open("config/lakes.json") as f:
    lakes = json.load(f)

start_date = '2017-01-01'
end_date = '2024-01-01'

results = []

for lake in lakes:

    lake_id = lake["lake_id"]
    lat = lake["lat"]
    lon = lake["lon"]
    buffer = lake["buffer"]

    geometry = ee.Geometry.Point([lon, lat]).buffer(buffer)

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR")
        .filterBounds(geometry)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
    )

    def process_image(image):

        ndwi = image.normalizedDifference(["B3", "B8"]).rename("NDWI")
        water = ndwi.gt(0)

        area = water.multiply(ee.Image.pixelArea())

        stats = area.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=10,
            maxPixels=1e9
        )

        return ee.Feature(None, {
            "lake_id": lake_id,
            "date": image.date().format("YYYY-MM-dd"),
            "water_area_m2": stats.get("NDWI")
        })

    features = collection.map(process_image)

    fc = features.getInfo()

    for f in fc["features"]:
        props = f["properties"]

        if props["water_area_m2"] is not None:

            results.append({
                "lake_id": props["lake_id"],
                "date": props["date"],
                "lake_area_km2": props["water_area_m2"] / 1e6
            })

df = pd.DataFrame(results)

df.to_csv("data/raw/lake_area_timeseries.csv", index=False)

print("Dataset saved: data/raw/lake_area_timeseries.csv")