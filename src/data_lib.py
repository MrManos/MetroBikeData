import redis
import json
import datetime
from typing import List
from gcd_algorithm import great_circle_distance
import logging

'''
TODO: Filter by kiosk, have option to check if either kiosk is within the radius, or just the checkout/return.
'''
 

def get_data(trips_db: redis.client.Redis, kiosk_db: redis.client.Redis) -> tuple:
    """
    Retrieve trips and kiosk data from Redis databases.

    Args:
        trips_db (redis.client.Redis): Redis connection for trips database.
        kiosk_db (redis.client.Redis): Redis connection for kiosk database.

    Returns:
        tuple: A tuple containing trips data (list) and kiosk data (list).
    """
    # Retrieve trips data
    trips_data = []
    for key in sorted(trips_db.keys()):
        trips_data.extend(json.loads(trips_db.get(key)))

    # Retrieve kiosks data
    kiosk_data = json.loads(kiosk_db.get('kiosks'))

    return trips_data, kiosk_data

def filter_by_date(trips_data: List[dict], start_datetime: datetime, end_datetime:datetime) -> List[dict]:
    '''
    Filters trip data within the interval [start_data, end_date]

    Args:
        trips_data: List of dicts, each dict is data for one trip
        start_date: start date of the interval in TBD format
        end_date: end date of the interval in TBD format

    Returns:
        List[dict]: filtered trips_data

    Example:
        trips_data, kiosk_data = get_data(trips_db, kiosk_db)
        start_date = datetime(year=2024,month=1,day=1)
        end_date = datetime(year=2024,month=2,day=10)
        filter_by_date(trips_data, start_date, end_date)
    '''

    # helper function to check if trip is within time interval
    def _in_interval(trip_dict):
        date_str = trip_dict['checkout_datetime']
        trip_datetime = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
        return start_datetime <= trip_datetime <= end_datetime
    
    return [trip for trip in trips_data if _in_interval(trip)]

def filter_by_location(trips_data: List[dict], kiosk_data: List[dict], coordinates: tuple, radius:float) -> List[dict]:
    '''
    Filters trip data based on the distance of the checkout or return kiosk to a specified geolocation

    Args:
        trips_data: Each dict is data for one trip. Must have keys 'checkout_kiosk_id' and 'return_kiosk_id'
        kiosk_data: Each dict is data for one kiosk. Must have keys 'kiosk_id', and 'location'
        coordinates: (float, float) - the specified latitude and longitude
        radius: distance in km around specified coordinates to filter by.

    Returns:
        List[dict]: the filtered data

    Example:
        trips_data = filter_by_date(trips_data, start_date, end_date)
        ut_coords  = (30.2850, -97.7335)
        filtered = filter_by_location(trips_data,
                        kiosk_data,
                        coordinates=ut_coords,
                        radius = 10)
    '''
    # precompute distance from each kiosk to the coordinates
    kiosk_distances = {} # dict mapping kiosk id to distance from coordinates
    lat1, long1 = coordinates
    for kiosk_dict in kiosk_data:
        kiosk_id = kiosk_dict['kiosk_id']
        kiosk_loc = kiosk_dict['location']
        lat2, long2 = float(kiosk_loc['latitude']), float(kiosk_loc['longitude'])
        dist = great_circle_distance(lat1, long1, lat2, long2)
        kiosk_distances[kiosk_id] = dist

    missing_ids = set()

    # helper function to determin if kiosk is within radius
    def _kiosks_in_radius(trip):
        # check if both kiosks are within radius
        for kiosk_id in (trip['checkout_kiosk_id'], trip['return_kiosk_id']):
            try:
                dist = kiosk_distances[kiosk_id]
            except KeyError:
                missing_ids.add(kiosk_id)
                return False

            if dist > radius:
                return False
            
        return True

    # filter data with list comprehension
    filtered_data = [trip for trip in trips_data if _kiosks_in_radius(trip)]

    # report missing kiosk ids
    logging.info(f"Missing kiosk IDs {missing_ids}")

    return filtered_data