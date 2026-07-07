#! /usr/bin/env python

import xarray as xr
import rasterio as rio
import rioxarray
import numpy as np
import os
from autoRIFT import autoRIFT
from scipy.interpolate import interpn
import pystac_client
import odc.stac
import planetary_computer
import geopandas as gpd
from shapely.geometry import shape
import warnings
import argparse
import time
from rasterio.env import Env

def retry_call(fn, n=5, delay=2):
    for i in range(n):
        try:
            return fn()
        except Exception:
            if i == n - 1:
                raise
            time.sleep(delay * (2 ** i))

# silence some warnings from stackstac and autoRIFT
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=UserWarning)

def download_s2(img1_date, img2_date, aoi):
    '''
    Download a pair of Sentinel-2 images acquired on given dates over a given area of interest
    '''
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
        search = retry_call(lambda: stac.search(
            intersects=aoi,
            datetime=img1_date,
            collections=["sentinel-2-l2a"]
        ))
        
        img1_items = retry_call(lambda: search.item_collection())

        img1_ds = retry_call(lambda: odc.stac.load(
            img1_items,
            bands=["B08", "SCL"],
            chunks={"x": 2048, "y": 2048},
            bbox=aoi_gpd.total_bounds,
            groupby='solar_day'
        )).where(lambda x: x > 0, other=np.nan)

        img1_ds = img1_ds.where(~img1_ds.SCL.isin([8, 9]), other=np.nan)
        
        search = retry_call(lambda: stac.search(
            intersects=aoi,
            datetime=img2_date,
            collections=["sentinel-2-l2a"]
        ))
        
        img2_items = retry_call(lambda: search.item_collection())

        img2_ds = retry_call(lambda: odc.stac.load(
            img2_items,
            bands=["B08", "SCL"],
            chunks={"x": 2048, "y": 2048},
            bbox=aoi_gpd.total_bounds,
            groupby='solar_day'
        )).where(lambda x: x > 0, other=np.nan)

        img2_ds = img2_ds.where(~img2_ds.SCL.isin([8, 9]), other=np.nan)

    return img1_ds, img2_ds 

def run_autoRIFT(img1, img2, skip_x=5, skip_y=5, min_x_chip=8, max_x_chip=32,
                 preproc_filter_width=3, mpflag=4, search_limit_x=30, search_limit_y=30):
    '''
    Configure and run autoRIFT feature tracking with Sentinel-2 data for large mountain glaciers
    ''' 
    obj = autoRIFT()
    obj.MultiThread = mpflag

    obj.I1 = img1
    obj.I2 = img2

    obj.SkipSampleX = skip_x
    obj.SkipSampleY = skip_y

    # Kernel sizes to use for correlation
    obj.ChipSizeMinX = min_x_chip
    obj.ChipSizeMaxX = max_x_chip
    obj.ChipSize0X = min_x_chip
    # oversample ratio, balancing precision and performance for different chip sizes
    #obj.OverSampleRatio = {obj.ChipSize0X:8, obj.ChipSize0X*2:16, obj.ChipSize0X*4:32}

    # generate grid
    m,n = obj.I1.shape
    xGrid = np.arange(obj.SkipSampleX+10,n-obj.SkipSampleX,obj.SkipSampleX)
    yGrid = np.arange(obj.SkipSampleY+10,m-obj.SkipSampleY,obj.SkipSampleY)
    nd = xGrid.__len__()
    md = yGrid.__len__()
    obj.xGrid = np.int32(np.dot(np.ones((md,1)),np.reshape(xGrid,(1,xGrid.__len__()))))
    obj.yGrid = np.int32(np.dot(np.reshape(yGrid,(yGrid.__len__(),1)),np.ones((1,nd))))
    noDataMask = np.invert(np.logical_and(obj.I1[:, xGrid-1][yGrid-1, ] > 0, obj.I2[:, xGrid-1][yGrid-1, ] > 0))

    # set search limits
    obj.SearchLimitX = np.full_like(obj.xGrid, search_limit_x)
    obj.SearchLimitY = np.full_like(obj.xGrid, search_limit_y)

    # set search limit and offsets in nodata areas
    obj.SearchLimitX = obj.SearchLimitX * np.logical_not(noDataMask)
    obj.SearchLimitY = obj.SearchLimitY * np.logical_not(noDataMask)
    obj.Dx0 = obj.Dx0 * np.logical_not(noDataMask)
    obj.Dy0 = obj.Dy0 * np.logical_not(noDataMask)
    obj.Dx0[noDataMask] = 0
    obj.Dy0[noDataMask] = 0
    obj.NoDataMask = noDataMask

    # Consensus gate (NDC) — require more neighbor agreement in a larger window
    obj.FiltWidth = 17        # was 5; must be odd
    obj.FracValid = 0.32     # was 0.32 (=8/25)
    
    # Displacement-distance count tolerance (stricter local consistency)
    obj.FracSearch = 0.20    # was 0.20
    
    # MAD outlier gate (tighter)
    obj.MadScalar = 2.5      # was 4

    print("preprocessing images")
    obj.WallisFilterWidth = preproc_filter_width
    obj.preprocess_filt_lap() # preprocessing with laplacian filter
    obj.uniform_data_type()

    print("starting autoRIFT")
    obj.runAutorift()
    print("autoRIFT complete")

    # convert displacement to m
    obj.Dx_m = obj.Dx * 10
    obj.Dy_m = obj.Dy * 10
        
    return obj

def _interp_blockwise(
    values,
    src_y_idx,
    src_x_idx,
    dst_y_idx,
    dst_x_idx,
    block_rows=1024,
    fill_value=np.nan,
):
    """
    Interpolate autoRIFT output from its sparse search grid to a target pixel grid,
    using row blocks to reduce peak memory use.
    """

    values = np.asarray(values, dtype=np.float32)

    interp = RegularGridInterpolator(
        (src_y_idx, src_x_idx),
        values,
        method="linear",
        bounds_error=False,
        fill_value=fill_value,
    )

    out = np.empty((len(dst_y_idx), len(dst_x_idx)), dtype=np.float32)

    for row0 in range(0, len(dst_y_idx), block_rows):
        row1 = min(row0 + block_rows, len(dst_y_idx))

        yy, xx = np.meshgrid(
            dst_y_idx[row0:row1],
            dst_x_idx,
            indexing="ij",
        )

        pts = np.column_stack([
            yy.ravel(),
            xx.ravel(),
        ])

        out[row0:row1, :] = interp(pts).reshape(row1 - row0, len(dst_x_idx)).astype(np.float32)

    return out


def prep_outputs(obj, img1_ds, img2_ds, output_resolution=20, block_rows=1024):
    """
    Interpolate autoRIFT pixel offsets to a lower-resolution output grid and
    calculate velocity.

    Assumes obj.Dx_m and obj.Dy_m are already in meters.
    For your current workflow, input Sentinel-2 B08 is 10 m, so output_resolution=20
    means every 2nd input pixel.

    Returns an xarray Dataset with 20 m x/y coordinates and float32 outputs.
    """

    # Get native pixel spacing from the loaded image coordinates
    xres = float(abs(img1_ds.x.values[1] - img1_ds.x.values[0]))
    yres = float(abs(img1_ds.y.values[1] - img1_ds.y.values[0]))

    if not np.isclose(xres, yres):
        raise ValueError(f"Non-square pixels detected: xres={xres}, yres={yres}")

    native_res = xres
    output_step_px = int(round(output_resolution / native_res))

    if output_step_px < 1:
        raise ValueError(
            f"output_resolution={output_resolution} is finer than native_res={native_res}"
        )

    actual_output_res = native_res * output_step_px

    if not np.isclose(actual_output_res, output_resolution):
        print(
            f"Warning: requested {output_resolution} m output, but native resolution "
            f"{native_res} m gives {actual_output_res} m using integer pixel step "
            f"{output_step_px}."
        )

    # Source autoRIFT grid, in original image pixel coordinates
    src_x_idx = obj.xGrid[0, :].astype(np.float32)
    src_y_idx = obj.yGrid[:, 0].astype(np.float32)

    # Target output grid, also in original image pixel coordinates
    # Start/end chosen to stay inside the autoRIFT interpolation domain.
    dst_x_idx = np.arange(
        src_x_idx.min(),
        src_x_idx.max() + 1,
        output_step_px,
        dtype=np.float32,
    )

    dst_y_idx = np.arange(
        src_y_idx.min(),
        src_y_idx.max() + 1,
        output_step_px,
        dtype=np.float32,
    )

    print(
        f"Interpolating autoRIFT output to ~{actual_output_res:.1f} m grid: "
        f"{len(dst_y_idx)} rows x {len(dst_x_idx)} cols"
    )

    # Interpolate only meter offsets, not both pixel and meter offsets.
    # This saves memory.
    Dx_m = _interp_blockwise(
        obj.Dx_m,
        src_y_idx,
        src_x_idx,
        dst_y_idx,
        dst_x_idx,
        block_rows=block_rows,
    )

    Dy_m = _interp_blockwise(
        obj.Dy_m,
        src_y_idx,
        src_x_idx,
        dst_y_idx,
        dst_x_idx,
        block_rows=block_rows,
    )

    # Convert target pixel indices to real-world x/y coordinates.
    # Use nearest integer pixel indices because output_step_px is integer.
    dst_x_pix = dst_x_idx.astype(int)
    dst_y_pix = dst_y_idx.astype(int)

    x_coords = img1_ds.x.values[dst_x_pix]
    y_coords = img1_ds.y.values[dst_y_pix]

    # Temporal baseline
    dt_days = (
        img2_ds.time.isel(time=0) - img1_ds.time.isel(time=0)
    ).dt.days.item()

    if dt_days == 0:
        raise ValueError("Image pair has zero-day temporal baseline.")

    veloc_x = (Dx_m / dt_days * 365.25).astype(np.float32)
    veloc_y = (Dy_m / dt_days * 365.25).astype(np.float32)
    veloc_horizontal = np.sqrt(veloc_x**2 + veloc_y**2).astype(np.float32)

    out_ds = xr.Dataset(
        {
            "Dx_m": (("y", "x"), Dx_m),
            "Dy_m": (("y", "x"), Dy_m),
            "veloc_x": (("y", "x"), veloc_x),
            "veloc_y": (("y", "x"), veloc_y),
            "veloc_horizontal": (("y", "x"), veloc_horizontal),
        },
        coords={
            "x": x_coords,
            "y": y_coords,
        },
        attrs={
            "output_resolution_m": float(actual_output_res),
            "temporal_baseline_days": int(dt_days),
        },
    )

    # Preserve CRS if available
    try:
        out_ds = out_ds.rio.write_crs(img1_ds.rio.crs)
    except Exception:
        pass

    print("finished postprocessing")
    return out_ds

def get_parser():
    parser = argparse.ArgumentParser(description="Run autoRIFT to find pixel offsets for two Sentinel-2 images")
    parser.add_argument("img1_date", type=str, help="date of first Sentinel-2 image ('YYYY-mm-dd')")
    parser.add_argument("img2_date", type=str, help="date of second Sentinel-2 image ('YYYY-mm-dd')")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()

    # hardcoding a bbox for now
    # emmons
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
    #Juneau Icefield
    aoi = {
        "type": "Polygon",
        "coordinates": [
            [[-135.27061670682534,59.57305870964015],
            [-133.51060124884273,59.57188884388103],
            [-133.51037046328878,58.33925381751183],
            [-135.27088827541,58.33767437010192],
            [-135.27061670682534,59.57305870964015]]
        ]
    }

    #Blue Glacier
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

    #Nisqually glacier
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

    # download Sentinel-2 images
    img1_ds, img2_ds = download_s2(args.img1_date, args.img2_date, aoi)
    # grab near infrared band only
    img1 = img1_ds.B08.squeeze().values
    img2 = img2_ds.B08.squeeze().values
    
    # scale search limit with temporal baseline assuming max velocity 1000 m/yr (100 px/yr)
    t_baseline = (img2_ds.time.isel(time=0) - img1_ds.time.isel(time=0)).dt.days
    search_limit_x = search_limit_y = round(((t_baseline/365.25)*60).item())
    
    # run autoRIFT feature tracking
    obj = run_autoRIFT(img1, img2, search_limit_x=search_limit_x, search_limit_y=search_limit_y)
    # postprocess offsets
    ds = prep_outputs(obj, img1_ds, img2_ds)

    # write out velocity to tif
    ds.veloc_x.rio.to_raster(f'S2_{args.img1_date}_{args.img2_date}_veloc_x.tif')
    ds.veloc_y.rio.to_raster(f'S2_{args.img1_date}_{args.img2_date}_veloc_y.tif')
    print('finished writing velocity maps to disk')

if __name__ == "__main__":
   main()
