import argparse
import csv
import math

import requests


# Constants
mile_to_km = 1.60934
cfs_to_cms = 0.0283168
Earth_radius_km = 6371
NWPS_API = "https://api.water.noaa.gov/nwps/v1/reaches/{feature_id}"


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


def create_fim_flow_file(start_feature_id, flow, distance, output_path):
    """
    Trace down the NWM stream and creates a flow file.
    """
    reaches_to_write = []
    total_distance = 0.0
    current_feature_id = start_feature_id
    previous_coords = None

    print("Starting downstream trace...")

    while current_feature_id and total_distance < distance:
        response = requests.get(NWPS_API.format(feature_id=current_feature_id))
        response.raise_for_status()
        data = response.json()

        reach_id_str = data.get('reachId')

        reaches_to_write.append({'feature_id': int(reach_id_str), 'discharge': flow})

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

        downstream_list = data.get('route', {}).get('downstream', [])
        if downstream_list:
            current_feature_id = int(downstream_list[0]['reachId'])
        else:
            current_feature_id = None

        # Write to csv
        with open(output_path, 'w', newline='') as csvfile:
            fieldnames = ['feature_id', 'discharge']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(reaches_to_write)


if __name__ == "__main__":
    """
    Example usage:
    python3 /foss_fim/tools/generate_costume_flow_files.py
    -feature_id 23021904
    -cfs 20000
    -mile 10
    -o /output/costume_flows.csv
    """
    parser = argparse.ArgumentParser(description="Generate a FIM flow file by tracing downstream reaches.")
    parser.add_argument("-feature_id", type=int, required=True, help="Starting reach feature ID (integer)")
    flow_group = parser.add_mutually_exclusive_group(required=True)
    flow_group.add_argument("-cms", type=float, help="Flow value in cms")
    flow_group.add_argument("-cfs", type=float, help="Flow value in cfs")
    distance_group = parser.add_mutually_exclusive_group(required=True)
    distance_group.add_argument("-mile", type=float, help="Target downstream distance in mile")
    distance_group.add_argument("-km", type=float, help="Target downstream distance in km")
    parser.add_argument("-o", "--output", required=True, help="Path to output CSV file")

    args = parser.parse_args()

    # Handle flow and distance unit
    if args.cms is not None:
        flow = args.cms
    else:
        flow = args.cfs * cfs_to_cms

    if args.km is not None:
        distance = args.km
    else:
        distance = args.mile * mile_to_km

    create_fim_flow_file(
        start_feature_id=args.feature_id, flow=flow, distance=distance, output_path=args.output
    )
