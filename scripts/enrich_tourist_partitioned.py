import argparse
import io
import os
import re
from typing import Dict, Iterable, List, Tuple

import boto3
import pandas as pd

from fetch_tourist_estimates import fetch_estimates


DATE_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})/")


def extract_date_from_key(key: str) -> str:
    """
    Izvlači YYYY-MM-DD iz S3 key-a, npr:
    weather_partitioned/date=2022-04-01/part-0000.csv
    -> 2022-04-01
    """
    m = DATE_RE.search(key)
    if not m:
        raise ValueError(f"Could not extract date from key: {key}")
    return m.group(1)


def list_csv_keys_with_dates(
    s3_client, bucket: str, base_prefix: str
) -> List[Tuple[str, str]]:
    """
    Vrati listu (key, date_str) za sve fajlove ispod base_prefix.
    """
    results: List[Tuple[str, str]] = []
    continuation_token = None

    while True:
        kwargs = {
            "Bucket": bucket,
            "Prefix": base_prefix,
        }
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]

            if key.endswith("/") or key == base_prefix.rstrip("/"):
                continue

            date_str = extract_date_from_key(key)
            results.append((key, date_str))

        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break

    return results


def build_iasi_estimate_map(dates: Iterable[str]) -> Dict[str, int]:
    """
    Za sve unikatne datume poziva API JEDNOM po datumu
    i vraća mapu {date_str -> tourist_estimate_iasi}.
    """
    unique_dates = sorted(set(dates))
    est_map: Dict[str, int] = {}

    for d in unique_dates:
        print(f"[API] Fetching estimates for date={d}")
        data = fetch_estimates(d)

        iasi_entry = None
        for city_info in data.get("info", []):
            if city_info.get("name", "").lower() == "iasi":
                iasi_entry = city_info
                break

        if iasi_entry is None:
            raise RuntimeError(f"No 'Iasi' entry found in API response for date={d}")

        est_map[d] = int(iasi_entry["estimated_no_people"])

    return est_map


def enrich_single_file(
    s3_client,
    bucket: str,
    src_key: str,
    dst_prefix: str,
    date_str: str,
    est_map: Dict[str, int],
) -> None:
    """
    Skida jedan CSV sa S3, dodaje kolonu tourist_estimate i upisuje nazad
    u isti relativni put, ali ispod dst_prefix.
    """
    if date_str not in est_map:
        raise KeyError(f"Missing estimate for date {date_str} (key={src_key})")

    resp = s3_client.get_object(Bucket=bucket, Key=src_key)
    body_bytes = resp["Body"].read()
    text = body_bytes.decode("utf-8")

    df = pd.read_csv(io.StringIO(text))
    df["tourist_estimate"] = est_map[date_str]


    base_name = src_key.split("/", 1)[1]  
    dst_key = os.path.join(dst_prefix.rstrip("/"), base_name).replace("\\", "/")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=dst_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
    )
    print(f"[WRITE] {dst_key} (rows={len(df)})")


def process_dataset(
    s3_client,
    bucket: str,
    src_prefix: str,
    dst_prefix: str,
    est_map: Dict[str, int],
) -> None:
    """
    Obrada jednog dataset-a (weather ili pollution):
    prolazi kroz sve CSV fajlove u src_prefix i za svaki pravi enriched verziju u dst_prefix.
    """
    print(f"\n=== Processing dataset: {src_prefix} -> {dst_prefix} ===")
    keys_with_dates = list_csv_keys_with_dates(s3_client, bucket, src_prefix)

    if not keys_with_dates:
        print(f"No CSV files found under prefix {src_prefix}")
        return

    for key, date_str in keys_with_dates:
        enrich_single_file(s3_client, bucket, key, dst_prefix, date_str, est_map)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich partitioned weather/pollution data with tourist estimates for Iasi."
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name",
    )
    parser.add_argument(
        "--weather-prefix",
        default="weather_partitioned/",
        help="Source prefix for weather data (default: weather_partitioned/)",
    )
    parser.add_argument(
        "--pollution-prefix",
        default="pollution_partitioned/",
        help="Source prefix for pollution data (default: pollution_partitioned/)",
    )
    parser.add_argument(
        "--weather-out-prefix",
        default="weather_partitioned_enriched/",
        help="Destination prefix for enriched weather data.",
    )
    parser.add_argument(
        "--pollution-out-prefix",
        default="pollution_partitioned_enriched/",
        help="Destination prefix for enriched pollution data.",
    )

    args = parser.parse_args()

    s3_client = boto3.client("s3")
    
    weather_files = list_csv_keys_with_dates(s3_client, args.bucket, args.weather_prefix)
    pollution_files = list_csv_keys_with_dates(s3_client, args.bucket, args.pollution_prefix)

    all_dates = [d for _, d in weather_files] + [d for _, d in pollution_files]
    if not all_dates:
        print("No CSV files found in given prefixes. Nothing to do.")
        return

    est_map = build_iasi_estimate_map(all_dates)
    
    process_dataset(
        s3_client,
        args.bucket,
        args.weather_prefix,
        args.weather_out_prefix,
        est_map,
    )
    process_dataset(
        s3_client,
        args.bucket,
        args.pollution_prefix,
        args.pollution_out_prefix,
        est_map,
    )

    print("\nDone. All enriched files have been written to S3.")


if __name__ == "__main__":
    main()
