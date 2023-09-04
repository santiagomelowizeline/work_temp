
import pdb
import pandas as pd


PERCENTILES = [0.25, 0.50, 0.75]


def get_counted_per_minute(data: pd.DataFrame):
    data["device_time"] = pd.to_datetime(data["device_time"], format="%Y-%m-%dT%H:%M:%S.%f", utc=True)

    device_time_df = data[["device_time"]]
    
    minute_resampled = device_time_df.resample("1T", on="device_time")
    
    per_minute_count = minute_resampled.mean().join(minute_resampled.size().rename('count'))

    per_minute_count = per_minute_count[per_minute_count["count"] > 0]

    return per_minute_count


def get_percentiles(per_minute: pd.DataFrame):
    percentiles = {}

    for percentile in PERCENTILES:
        percentiles[percentile] = per_minute["count"].quantile(percentile)
    

    print("Percentiles for per minute saves:", percentiles)
    print("Max amount of saves per minute:", per_minute["count"].max())
    print("Average amount of saves per minute:", per_minute["count"].mean())

    return percentiles


def run(data: pd.DataFrame):

    per_minute = get_counted_per_minute(data)

    percentiles = get_percentiles(per_minute)

    pdb.set_trace()

