import argparse
import csv
import math
import sys

import requests


# Constants
mile_to_km = 1.60934
cfs_to_cms = 0.0283168
Earth_radius_km = 6371
# API
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

    # Convert lat and long from degrees to radians
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = Earth_radius_km * c
    return distance


def trace_downstream(start_feature_id, flow, distance=None, stop_feature_id=None):
    """
    Trace down the NWM stream and creates a flow file.
    Stops when the distance is exceeded or the stop_feature_id is reached.
    """
    reaches_to_write = []
    total_distance = 0.0
    current_feature_id = start_feature_id
    previous_coords = None

    print("Starting downstream trace...")
    print('======================================')

    # Initialize a counter
    reach_count = 0

    # Define a maximum number of reaches to check. This prevents an infinite loop
    # if the stop_feature_id is not actually downstream of the start_feature_id
    max_reaches_to_check = 200

    # Loop as long as we have a valid feature_id
    while current_feature_id:
        # Check if we have exceeded the limit
        reach_count += 1
        if distance is None and reach_count > max_reaches_to_check:
            print(f"WARNING: Trace stopped after checking {max_reaches_to_check} reaches.")
            print(f"Could not find stop_feature_id: {stop_feature_id} downstream from {start_feature_id}.")
            break  # Exit the loop to prevent it from running forever

        # Check for stop condition
        # 1. Stop if the current reach is the stop reach
        if stop_feature_id and current_feature_id == stop_feature_id:
            print(f"Reached stop feature_id: {stop_feature_id}")
            break
        # 2. Stop if we have traveled the target distance
        if distance is not None and total_distance >= distance:
            print(f"Reached distance limit of {distance:.2f} km.")
            break

        # Make an API to get data
        response = requests.get(NWPS_API.format(feature_id=current_feature_id))
        response.raise_for_status()
        data = response.json()

        # Extract the reachId (feature_id)
        reach_id_str = data.get('reachId')

        # Add the current reach and its flow to our results list
        reaches_to_write.append({'feature_id': int(reach_id_str), 'discharge': flow})
        reach_count += 1

        # Calculate the distance
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

        # Store the current coordinates for the next iteration's distance calculation
        previous_coords = current_coords

        print(
            f"Processed reach {reach_id_str}, distance: {segment_distance:.2f} km, Total: {total_distance:.2f} km"
        )

        # Find the next reach downstream
        downstream_list = data.get('route', {}).get('downstream', [])
        if downstream_list:
            current_feature_id = int(downstream_list[0]['reachId'])
        else:
            current_feature_id = None
    return reaches_to_write


def get_feature_id_from_gage(gage_id):
    """
    Translate a gage ID into the internal feature_id used by NWM
    """
    try:
        response_gage = requests.get(NWPS_API_gage.format(gage_id=gage_id))
        response_gage.raise_for_status()
        data_gage = response_gage.json()
        # Extract the reachId
        reach_id_str = data_gage.get('reachId')
        return int(reach_id_str)
    except Exception as exc:
        raise RuntimeError(f"failed to resolve gage {gage_id} to a reach: {exc}")


def parse_list(values):
    """
    Parse command line arguments that might be a mix of space-separated and comma-separated values
    """
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
    -feature_id 24228229,947060081
    -cfs 20000 25000
    -mile 10
    -o /output/custom_flows.csv

    Example3:
    python3 /foss_fim/tools/generate_custom_flow_files.py
    -gage LINK1 06869500
    -cms 120 50
    -mile 10
    -o /output/custom_flows.csv

    Example4:
    python3 /foss_fim/tools/generate_custom_flow_files.py
    -gage wlsk1 link1 tsck1 ncmk1
    -cfs 2000 3000 4000 4500
    -mile 10
    -o /output/custom_flows.csv
    """
    parser = argparse.ArgumentParser(description="Generate a FIM flow file by tracing downstream reaches.")
    # Group for starting location
    id_group = parser.add_mutually_exclusive_group(required=True)
    id_group.add_argument(
        "-feature_id", nargs='+', type=str, help="Starting reach feature ID(s) (integer or comma-separated)"
    )
    id_group.add_argument(
        "-gage", nargs='+', type=str, help="The gauge, LID or USGS ID(s). Example: ANAW1 or 13334300"
    )
    # Group for flow value
    flow_group = parser.add_mutually_exclusive_group(required=True)
    flow_group.add_argument("-cms", type=float, nargs='+', help="Flow value in cms")
    flow_group.add_argument("-cfs", type=float, nargs='+', help="Flow value in cfs")
    # Group for distance
    distance_group = parser.add_mutually_exclusive_group(required=True)
    distance_group.add_argument("-mile", type=float, help="Target downstream distance in mile")
    distance_group.add_argument("-km", type=float, help="Target downstream distance in km")
    parser.add_argument("-o", "--output", required=True, help="Path to output CSV file")

    args = parser.parse_args()

    # Resolve start location into a list of feature_ids
    start_feature_id = []
    if args.feature_id:
        raw_feature_ids = parse_list(args.feature_id)
        start_feature_id = [int(x) for x in raw_feature_ids]
    else:  # If gages were given, convert them to feature_ids
        raw_gages = parse_list(args.gage)
        for g in raw_gages:
            fid = get_feature_id_from_gage(g)
            print(f"Gage {g} -> start feature_id {fid}")
            start_feature_id.append(fid)

    # Convert flow and distance unit
    if args.cms is not None:
        flow = list(args.cms)
    else:
        flow = [q * cfs_to_cms for q in args.cfs]

    # If a single flow given, use it for all sites
    if len(flow) == 1 and len(start_feature_id) > 1:
        flow = flow * len(start_feature_id)

    # Check that the number of flows matches the number of sites
    if len(flow) != len(start_feature_id):
        print('Error: number of flows must be 1 or equal to number of start sites/feature_ids')
        sys.exit(1)

    # convert distance unit
    if args.km is not None:
        distance = args.km
    else:
        distance = args.mile * mile_to_km

    # collect rows
    all_row = []
    num_sites = len(start_feature_id)
    if num_sites == 0:
        print('No start sites provided')
        sys.exit(1)

    # This loop runs from the first site to the second-to-last site
    for i in range(num_sites - 1):
        start_id = start_feature_id[i]
        stop_id = start_feature_id[i + 1]
        flow_val = flow[i]
        rows = trace_downstream(start_feature_id=start_id, flow=flow_val, stop_feature_id=stop_id)
        all_row.extend(rows)

    # The final trace from the last gage for the specified distance
    last_start_id = start_feature_id[-1]
    last_flow_val = flow[-1]
    rows = trace_downstream(start_feature_id=last_start_id, flow=last_flow_val, distance=distance)
    all_row.extend(rows)

    # save as a csv
    with open(args.output, 'w', newline='') as csvfile:
        fieldnames = ['feature_id', 'discharge']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_row)
