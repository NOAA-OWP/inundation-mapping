import argparse
import csv
import math
import sys

import requests


# Constants
mile_to_km = 1.60934
cfs_to_cms = 0.0283168
Earth_radius_km = 6371
NWPS_API = "https://api.water.noaa.gov/nwps/v1/reaches/{feature_id}"
NWPS_API_gage = "https://api.water.noaa.gov/nwps/v1/gauges/{gage_id}"


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great-cirle ditance between two points on the earth.
    Args:
        lat1, lon1: latitude and longitude of point 1 (in degrees).
        lat2, lon2: latitude and longitude of point 2 (in degrees).
    Returns:
        Distance in km.
    """
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = Earth_radius_km * c
    return distance


def trace_downstream(start_feature_id, flow, distance):
    """
    Trace down the NWM stream and creates a flow file.
    """
    reaches_to_write = []
    total_distance = 0.0
    current_feature_id = start_feature_id
    previous_coords = None

    print("Starting downstream trace...")
    print('======================================')

    reach_count = 0
    while current_feature_id and total_distance <= distance:
        response = requests.get(NWPS_API.format(feature_id=current_feature_id))
        response.raise_for_status()
        data = response.json()

        reach_id_str = data.get('reachId')

        reaches_to_write.append({'feature_id': int(reach_id_str), 'discharge': flow})
        reach_count += 1

        current_coords = {'lat': data.get('latitude'), 'lon': data.get('longitude')}
        if not all(current_coords.values()):
            print(
                f"Warning: Missing lat/lon for reach {reach_id_str}. Cannot calculate distance for this segment."
            )
            segment_distance = 0.0
        elif previous_coords:
            segment_distance = haversine_distance(
                previous_coords['lat'], previous_coords['lon'], current_coords['lat'], current_coords['lon']
            )
            total_distance += segment_distance
        else:
            segment_distance = 0.0
        previous_coords = current_coords

        print(
            f"Processed reach {reach_id_str}, distance: {segment_distance:.2f} km, Total: {total_distance:.2f} km"
        )

        downstream_list = data.get('route', {}).get('downstream', [])
        if downstream_list:
            current_feature_id = int(downstream_list[0]['reachId'])
        else:
            current_feature_id = None
    return reaches_to_write


def get_feature_id_from_gage(gage_id):
    try:
        response_gage = requests.get(NWPS_API_gage.format(gage_id=gage_id))
        response_gage.raise_for_status()
        data_gage = response_gage.json()
        reach_id_str = data_gage.get('reachId')
        return int(reach_id_str)
    except Exception as exc:
        raise RuntimeError(f"failed to resolve gage {gage_id} to a reach: {exc}")


def parse_list(values):
    out = []
    if not values:
        return out
    for v in values:
        for part in str(v).split(','):
            part = part.strip()
            if part:
                out.append(part)
    return out


if __name__ == "__main__":
    """
    Example usage:
    Example1:
    python3 /foss_fim/tools/generate_custom_flow_files.py
    -feature_id 23021904
    -cfs 20000
    -mile 10
    -o /output/custom_flows.csv

    Example2:
    python3 /foss_fim/tools/generate_custom_flow_files.py
    -feature_id 24228229,6129039
    -cfs 20000 25000
    -mile 10
    -o /output/custom_flows.csv

    Example3:
    python3 /foss_fim/tools/generate_custom_flow_files.py
    -gage ANAW1 13324300
    -cms 120 50
    -mile 10
    -o /output/custom_flows.csv
    """
    parser = argparse.ArgumentParser(description="Generate a FIM flow file by tracing downstream reaches.")
    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument(
        "-feature_id", nargs='+', type=str, help="Starting reach feature ID(s) (integer or comma-separated)"
    )
    id_group.add_argument(
        "-gage", nargs='+', type=str, help="The gauge, LID or USGS ID(s). Example: ANAW1 or 13334300"
    )
    flow_group = parser.add_mutually_exclusive_group(required=True)
    flow_group.add_argument("-cms", type=float, nargs='+', help="Flow value in cms")
    flow_group.add_argument("-cfs", type=float, nargs='+', help="Flow value in cfs")
    distance_group = parser.add_mutually_exclusive_group(required=True)
    distance_group.add_argument("-mile", type=float, help="Target downstream distance in mile")
    distance_group.add_argument("-km", type=float, help="Target downstream distance in km")
    parser.add_argument("-o", "--output", required=True, help="Path to output CSV file")

    args = parser.parse_args()

    # Handel feature_id and LID or USGS ID
    start_feature_id = []
    if args.feature_id:
        raw_feature_ids = parse_list(args.feature_id)
        start_feature_id = [int(x) for x in raw_feature_ids]
    else:
        raw_gages = parse_list(args.gage)
        for g in raw_gages:
            fid = get_feature_id_from_gage(g)
            print(f"Gage {g} -> start feature_id {fid}")
            start_feature_id.append(fid)

    # Handle flow and distance unit
    if args.cms is not None:
        flow = list(args.cms)
    else:
        flow = [q * cfs_to_cms for q in args.cfs]

    # If a single flow given, use it for all sites
    if len(flow) == 1 and len(start_feature_id) > 1:
        flow = flow * len(start_feature_id)
    if len(flow) != len(start_feature_id):
        print('Error: number of flows must be 1 or equal to number of start sites/feature_ids')
        sys.exit(1)

    if args.km is not None:
        distance = args.km
    else:
        distance = args.mile * mile_to_km

    # collect rows
    all_row = []
    for site, flow_val in zip(start_feature_id, flow):
        rows = trace_downstream(start_feature_id=site, flow=flow_val, distance=distance)
        all_row.extend(rows)

    # save as a csv
    with open(args.output, 'w', newline='') as csvfile:
        fieldnames = ['feature_id', 'discharge']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_row)
