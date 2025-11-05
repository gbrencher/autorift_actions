import xarray as xr
import rasterio
import rioxarray
import os
import pystac_client
import json
import pandas as pd
import argparse
import odc.stac
import planetary_computer
import geopandas as gpd
from shapely.geometry import shape
import numpy as np

def get_parser():
    parser = argparse.ArgumentParser(description="Search for Sentinel-2 images")
    parser.add_argument("cloud_cover", type=str, help="percent cloud cover allowed in images (0-100)")
    parser.add_argument("start_year", type=str, help="first year to search for images (min 2015)")
    parser.add_argument("stop_year", type=str, help="last year to search for images")
    parser.add_argument("start_month", type=str, help="first month of year to search for images")
    parser.add_argument("stop_month", type=str, help="last month of year to search for images")
    parser.add_argument("min_days", type=str, help="minumum temporal baseline (days)")
    parser.add_argument("max_days", type=str, help="maximum temporal baseline (days)")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    
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
    
    stac = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace)

    # search planetary computer
    search = stac.search(
        intersects=aoi,
        datetime=f'{args.start_year}-01-01/{args.stop_year}-12-31',
        collections=["sentinel-2-l2a"],
        query={"eo:cloud_cover": {"lt": float(args.cloud_cover)}})

    items = search.item_collection()
    
    s2_ds = odc.stac.load(items,chunks={"x": 2048, "y": 2048},
                          bbox=aoi_gpd.total_bounds,
                          groupby='solar_day').where(lambda x: x > 0, other=np.nan)
    print(f"Returned {len(s2_ds.time)} acquisitions")

    start_m = int(args.start_month)
    stop_m  = int(args.stop_month)

    if start_m <= stop_m:
        # simple case (e.g., May–September)
        s2_ds = s2_ds.where((s2_ds.time.dt.month >= start_m) & (s2_ds.time.dt.month <= stop_m), drop=True)
    else:
        # wrap-around case (e.g., December–February)
        s2_ds = s2_ds.where((s2_ds.time.dt.month >= start_m) | (s2_ds.time.dt.month <= stop_m), drop=True)
    
    # calculate number of valid pixels in each image
    total_pixels = len(s2_ds.y)*len(s2_ds.x)
    nan_count = (~np.isnan(s2_ds.B08)).sum(dim=['x', 'y']).compute()
    # keep only images with 90% or more valid pixels
    s2_ds = s2_ds.where(nan_count >= total_pixels*0.9, drop=True)

    # get dates of acceptable images
    image_dates = s2_ds.time.dt.strftime('%Y-%m-%d').values.tolist()
    time_vals = s2_ds.time.values
    print('\n'.join(image_dates))
    
    # Create Matrix Job Mapping (JSON Array)
    pairs = []
    # For each anchor index i, advance j>i while baseline <= max_days
    n = len(time_vals)
    for i in range(n - 1):
        ti = np.datetime64(time_vals[i], 'D')
        # start j at i+1 and walk forward until baseline exceeds max_days
        for j in range(i + 1, n):
            tj = np.datetime64(time_vals[j], 'D')
            dt_days = (tj - ti).astype(int)
            if dt_days < int(args.min_days):
                continue
            if dt_days > int(args.max_days):
                break  # further j will only increase baseline
            # baseline within range
            img1_date = image_dates[i]
            img2_date = image_dates[j]
            shortname = f"{img1_date}_{img2_date}"
            pairs.append({'img1_date': img1_date, 'img2_date': img2_date, 'name': shortname})
    matrixJSON = f'{{"include":{json.dumps(pairs)}}}'
    print(f'number of image pairs: {len(pairs)}')
    
    with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
        print(f'IMAGE_DATES={image_dates}', file=f)
        print(f'MATRIX_PARAMS_COMBINATIONS={matrixJSON}', file=f)

if __name__ == "__main__":
   main()
