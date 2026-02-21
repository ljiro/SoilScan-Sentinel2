"""
Targeted Satellite Extraction: fetches a 3x3 pixel grid of Sentinel-2 bands
for each field capture via Sentinel Hub, bypassing full-tile downloads.
"""
import os
import numpy as np
import pandas as pd
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()

# Sentinel Hub config from environment (do not commit .env)
config = None

def _get_config():
    global config
    if config is not None:
        return config
    from sentinelhub import SHConfig
    c = SHConfig()
    c.instance_id = os.getenv("SENTINELHUB_INSTANCE_ID", "YOUR_INSTANCE_ID")
    c.sh_client_id = os.getenv("SENTINELHUB_CLIENT_ID", "YOUR_CLIENT_ID")
    c.sh_client_secret = os.getenv("SENTINELHUB_CLIENT_SECRET", "YOUR_CLIENT_SECRET")
    config = c
    return config


def get_sentinel_bands(lat, lon, capture_date, grid_size=30):
    """
    Fetches a 3x3 pixel grid (roughly 30x30 m) for 12 Sentinel-2 bands.
    """
    from sentinelhub import (
        SentinelHubRequest,
        DataCollection,
        BBox,
        CRS,
        bbox_to_dimensions,
    )

    cfg = _get_config()
    # ~30 m: 1 deg ≈ 111 km, so 30 m ≈ 0.00027 deg
    offset = 0.000135
    bbox = BBox(
        bbox=[lon - offset, lat - offset, lon + offset, lat + offset],
        crs=CRS.WGS84,
    )
    time_interval = (
        str((capture_date - timedelta(days=14)).date()),
        str(capture_date.date()),
    )

    evalscript = """
    //VERSION=3
    function setup() {
        return {
            input: ["B01","B02","B03","B04","B05","B06","B07","B08","B8A","B09","B11","B12", "SCL"],
            output: { bands: 13 }
        };
    }
    function evaluatePixel(sample) {
        return [sample.B01, sample.B02, sample.B03, sample.B04, sample.B05,
                sample.B06, sample.B07, sample.B08, sample.B8A, sample.B09,
                sample.B11, sample.B12, sample.SCL];
    }
    """

    request = SentinelHubRequest(
        evalscript=evalscript,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL2_L2A,
                time_interval=time_interval,
            )
        ],
        responses=[SentinelHubRequest.output_response("default", "image/tiff")],
        bbox=bbox,
        size=bbox_to_dimensions(bbox, resolution=10),
        config=cfg,
    )

    try:
        data = request.get_data()
        if not data:
            return None
        latest_image = data[-1]
        band_means = np.mean(latest_image[:, :, :12], axis=(0, 1))
        return band_means
    except Exception as e:
        print(f"Error fetching data for {lat}, {lon}: {e}")
        return None


def augment_field_data(csv_path, output_path=None):
    """Loads AgriCapture field data and appends satellite band columns."""
    df = pd.read_csv(csv_path)
    df["capture_datetime"] = pd.to_datetime(df["capture_datetime"])

    band_columns = [
        "B01", "B02", "B03", "B04", "B05", "B06",
        "B07", "B08", "B8A", "B09", "B11", "B12",
    ]
    band_data = []

    for index, row in df.iterrows():
        bands = get_sentinel_bands(
            row["latitude"],
            row["longitude"],
            row["capture_datetime"],
        )
        if bands is not None:
            band_data.append(bands)
        else:
            band_data.append([np.nan] * 12)

    band_df = pd.DataFrame(band_data, columns=band_columns)
    final_df = pd.concat([df, band_df], axis=1)
    final_df = final_df.dropna(subset=band_columns)

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        final_df.to_csv(output_path, index=False)
        print(f"Saved {len(final_df)} rows to {output_path}")
    return final_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Augment field CSV with Sentinel-2 bands.")
    parser.add_argument(
        "input_csv",
        nargs="?",
        default="data/external/combined_field_data.csv",
        help="Path to field data CSV (default: data/external/combined_field_data.csv)",
    )
    parser.add_argument(
        "-o", "--output",
        default="data/processed/field_data_with_bands.csv",
        help="Output CSV path (default: data/processed/field_data_with_bands.csv)",
    )
    args = parser.parse_args()

    if _get_config().instance_id == "YOUR_INSTANCE_ID":
        print("Set SENTINELHUB_INSTANCE_ID, SENTINELHUB_CLIENT_ID, SENTINELHUB_CLIENT_SECRET in .env")
        exit(1)

    augment_field_data(args.input_csv, args.output)
