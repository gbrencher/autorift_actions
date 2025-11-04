import rioxarray
import os
import pystac_client
import json
import argparse
import odc.stac
import geopandas as gpd
from shapely.geometry import shape
import numpy as np
from osgeo import gdal

def get_parser():
    parser = argparse.ArgumentParser(description="Search for HLS images")
    parser.add_argument("cloud_cover", type=str, help="percent cloud cover allowed in images (0-100)")
    parser.add_argument("start_month", type=str, help="first month of year to search for images")
    parser.add_argument("stop_month", type=str, help="last month of year to search for images")
    parser.add_argument("npairs", type=str, help="number of pairs per image")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()

    gdal.SetConfigOption('GDAL_HTTP_COOKIEFILE','~/cookies.txt')
    gdal.SetConfigOption('GDAL_HTTP_COOKIEJAR', '~/cookies.txt')
    gdal.SetConfigOption('GDAL_DISABLE_READDIR_ON_OPEN','EMPTY_DIR')
    gdal.SetConfigOption('CPL_VSIL_CURL_ALLOWED_EXTENSIONS','TIF')
    gdal.SetConfigOption('GDAL_HTTP_UNSAFESSL', 'YES')
    
    # hardcode bbox for now
    aoi = {
        "type": "Polygon",
        "coordinates": [
            [[-121.76644001937807,46.83837147698088],
            [-121.6594983841296,46.83837147698088],
            [-121.6594983841296,46.8948204721259],
            [-121.76644001937807,46.8948204721259],
            [-121.76644001937807,46.83837147698088]]
        ]
    }

    aoi_gpd = gpd.GeoDataFrame({'geometry':[shape(aoi)]}).set_crs(crs="EPSG:4326")
    crs = aoi_gpd.estimate_utm_crs()

    stac = pystac_client.Client.open("https://cmr.earthdata.nasa.gov/stac/LPCLOUD")

    search = stac.search(
        collections=["HLSS30.v2.0", "HLSL30.v2.0"],
        intersects=aoi,
        query={"eo:cloud_cover": {"lt": float(args.cloud_cover)}},
        max_items=2000,
    )
    
    items = search.item_collection()

    ds = odc.stac.load(
        items,
        chunks={"x": 2048, "y": 2048},
        bbox=aoi_gpd.total_bounds,
        resolution=30,
        crs=crs.to_string(),
        groupby="solar_day"
    ).where(lambda x: x > 0, other=np.nan)
    
    print(f"Loaded {len(ds.time)} acquisitions.")

    # calculate number of valid pixels in each image
    total_pixels = len(ds.y)*len(ds.x)
    nan_count = (~np.isnan(ds.B04)).sum(dim=['x', 'y']).compute()
    # keep only images with 90% or more valid pixels
    ds = ds.where(nan_count >= total_pixels*0.9, drop=True)

    # filter to specified month range
    ds_study_period = ds.where((ds.time.dt.month >= int(args.start_month)) & (ds.time.dt.month <= int(args.stop_month)), drop=True)

    # get dates of acceptable images
    image_dates = ds_study_period.time.dt.strftime('%Y-%m-%d').values.tolist()
    print('\n'.join(image_dates))
    
    # Create Matrix Job Mapping (JSON Array)
    pairs = []
    for r in range(len(ds_study_period.time) - int(args.npairs)):
        for s in range(1, int(args.npairs) + 1 ):
            t_baseline = ds_study_period.isel(time=r+s).time - ds_study_period.isel(time=r).time
            if t_baseline.dt.days <= 100: #t baseline threshold
                img1_date = image_dates[r]
                img2_date = image_dates[r+s]
                shortname = f'{img1_date}_{img2_date}'
                pairs.append({'img1_date': img1_date, 'img2_date': img2_date, 'name':shortname})
    matrixJSON = f'{{"include":{json.dumps(pairs)}}}'
    print(f'number of image pairs: {len(pairs)}')
    
    with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
        print(f'IMAGE_DATES={image_dates}', file=f)
        print(f'MATRIX_PARAMS_COMBINATIONS={matrixJSON}', file=f)

if __name__ == "__main__":
   main()


    