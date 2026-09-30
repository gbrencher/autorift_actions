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
import time
from rasterio.env import Env

def retry_call(fn, n=20, delay=2):
    for i in range(n):
        try:
            return fn()
        except Exception:
            if i == n - 1:
                raise
            time.sleep(delay * (2 ** i))

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
    # Emmons glacier 
    # aoi = {
    #     "type": "Polygon",
    #     "coordinates": [
    #         [[-121.76644001937807,46.83837147698088],
    #         [-121.6594983841296,46.83837147698088],
    #         [-121.6594983841296,46.8948204721259],
    #         [-121.76644001937807,46.8948204721259],
    #         [-121.76644001937807,46.83837147698088]]
    #     ]
    # }
    # Langtang Lirung / rasuwa_2026_velocity_v1
    # (S2 autoRIFT request to Quinn, 2026-09-29; UTM 32645 xmin/xmax/ymin/ymax
    # 348175/362195/3122495/3139515, edges on the 20 m lattice origin 348175,3134515)
    aoi = {
        "type": "Polygon",
        "coordinates": [
            [[85.4528042, 28.219569],
             [85.4603216, 28.2196538],
             [85.4678391, 28.2197382],
             [85.4753565, 28.2198223],
             [85.482874, 28.2199059],
             [85.4903916, 28.219989],
             [85.4979091, 28.2200718],
             [85.5054267, 28.2201542],
             [85.5129443, 28.2202361],
             [85.520462, 28.2203176],
             [85.5279797, 28.2203988],
             [85.5354974, 28.2204795],
             [85.5430152, 28.2205598],
             [85.5505329, 28.2206396],
             [85.5580508, 28.2207191],
             [85.5655686, 28.2207982],
             [85.5730865, 28.2208768],
             [85.5806044, 28.220955],
             [85.5881223, 28.2210329],
             [85.5956403, 28.2211103],
             [85.5955344, 28.2291941],
             [85.5954286, 28.237278],
             [85.5953227, 28.2453618],
             [85.5952167, 28.2534456],
             [85.5951107, 28.2615294],
             [85.5950047, 28.2696132],
             [85.5948986, 28.277697],
             [85.5947924, 28.2857808],
             [85.5946863, 28.2938646],
             [85.59458, 28.3019484],
             [85.5944738, 28.3100321],
             [85.5943675, 28.3181158],
             [85.5942611, 28.3261996],
             [85.5941547, 28.3342833],
             [85.5940483, 28.342367],
             [85.5939418, 28.3504507],
             [85.5938353, 28.3585344],
             [85.5937287, 28.3666181],
             [85.5936221, 28.3747018],
             [85.5860933, 28.3746239],
             [85.5785646, 28.3745456],
             [85.5710359, 28.3744668],
             [85.5635072, 28.3743877],
             [85.5559786, 28.3743081],
             [85.54845, 28.3742281],
             [85.5409214, 28.3741478],
             [85.5333929, 28.3740669],
             [85.5258643, 28.3739857],
             [85.5183359, 28.3739041],
             [85.5108074, 28.373822],
             [85.503279, 28.3737396],
             [85.4957506, 28.3736567],
             [85.4882223, 28.3735734],
             [85.4806939, 28.3734897],
             [85.4731656, 28.3734055],
             [85.4656374, 28.373321],
             [85.4581092, 28.373236],
             [85.450581, 28.3731506],
             [85.4506984, 28.3650675],
             [85.4508158, 28.3569843],
             [85.4509332, 28.3489011],
             [85.4510505, 28.340818],
             [85.4511677, 28.3327348],
             [85.4512849, 28.3246516],
             [85.4514021, 28.3165683],
             [85.4515192, 28.3084851],
             [85.4516363, 28.3004019],
             [85.4517533, 28.2923186],
             [85.4518702, 28.2842354],
             [85.4519872, 28.2761521],
             [85.452104, 28.2680688],
             [85.4522209, 28.2599856],
             [85.4523376, 28.2519023],
             [85.4524544, 28.243819],
             [85.452571, 28.2357356],
             [85.4526877, 28.2276523],
             [85.4528042, 28.219569]]
        ]
    }
    
    
    
    # # Juneau Icefield
    # aoi = {
    #     "type": "Polygon",
    #     "coordinates": [
    #         [[-135.27061670682534,59.57305870964015],
    #         [-133.51060124884273,59.57188884388103],
    #         [-133.51037046328878,58.33925381751183],
    #         [-135.27088827541,58.33767437010192],
    #         [-135.27061670682534,59.57305870964015]]
    #     ]
    # }
    # Blue Glacier
    # aoi = {
    #     "type": "Polygon",
    #     "coordinates": [
    #         [[-123.79055865037546,47.758365021326654],
    #         [-123.6270429827974,47.758365021326654],
    #         [-123.6270429827974,47.83696563729873],
    #         [-123.79055865037546,47.83696563729873],
    #         [-123.79055865037546,47.758365021326654]]
    #     ]
    # }

    # Nisqually glacier
    # aoi = {
    #     "type": "Polygon",
    #     "coordinates": [
    #         [[-121.7772944,46.8520726],
    #         [-121.7174423,46.8520726],
    #         [-121.7174423,46.792772],
    #         [-121.7772944,46.792772],
    #         [-121.7772944,46.8520726]]
    #     ]
    # }

    aoi_gpd = gpd.GeoDataFrame({'geometry':[shape(aoi)]}).set_crs(crs="EPSG:4326")
    crs = aoi_gpd.estimate_utm_crs()
    
    stac = retry_call(lambda: pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace
    ))

    with Env(
        GDAL_HTTP_MAX_RETRY="5",
        GDAL_HTTP_RETRY_DELAY="2",
        GDAL_HTTP_TIMEOUT="60",
    ):
        # search planetary computer
        search = stac.search(
            intersects=aoi,
            datetime=f'{args.start_year}-01-01/{args.stop_year}-12-31',
            collections=["sentinel-2-l2a"],
            query={"eo:cloud_cover": {"lt": float(args.cloud_cover)}}
        )
    
        items = retry_call(lambda: search.item_collection())
        
        s2_ds = odc.stac.load(
            items,
            bands=["B08", "SCL"],
            chunks={"x": 2048, "y": 2048},
            resolution=100,          # coarse screening resolution, meters
            resampling={"B08": "average", "SCL": "nearest"},
            bbox=aoi_gpd.total_bounds,
            groupby='solar_day'
        ).where(lambda x: x > 0, other=np.nan)
    
        print(f"Returned {len(s2_ds.time)} acquisitions")
    
        start_m = int(args.start_month)
        stop_m  = int(args.stop_month)
    
        if start_m <= stop_m:
            s2_ds = s2_ds.where(
                (s2_ds.time.dt.month >= start_m) & 
                (s2_ds.time.dt.month <= stop_m),
                drop=True
            )
        else:
            s2_ds = s2_ds.where(
                (s2_ds.time.dt.month >= start_m) | 
                (s2_ds.time.dt.month <= stop_m),
                drop=True
            )
    
        # mask cloud
        s2_ds = s2_ds.where(~s2_ds.SCL.isin([8, 9]), other=np.nan)
        
        # calculate number of valid pixels in each image
        total_pixels = len(s2_ds.y) * len(s2_ds.x)
    
        nan_count = retry_call(
            lambda: (~np.isnan(s2_ds.B08)).sum(dim=['x', 'y']).compute()
        )
    
        # keep only images with 75% or more valid pixels
        s2_ds = s2_ds.where(nan_count >= total_pixels * 0.75, drop=True)

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
