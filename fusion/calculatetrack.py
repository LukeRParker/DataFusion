import json
import numpy as np
from haversine import haversine, Unit
from datetime import datetime

def calculate_distance(track):
    return [haversine(point1, point2, unit=Unit.METERS) for point1, point2 in zip(track[:-1], track[1:])]

def check_same_direction(track, direction_threshold):
    directions = [np.array(point2) - np.array(point1) for point1, point2 in zip(track[:-1], track[1:])]
    for direction1, direction2 in zip(directions[:-1], directions[1:]):
        if np.dot(direction1, direction2) < direction_threshold:
            return False
    return True

def is_same_track(data1, data2, distance_threshold, direction_threshold):
    track1 = data1['TRACK']
    track2 = data2['TRACK']

    track1_record = [(record['LATITUDE'], record['LONGITUDE']) for record in data1['TRACK']]
    track2_record = [(record['LATITUDE'], record['LONGITUDE']) for record in data2['TRACK']]

    distance1 = calculate_distance(track1_record)
    if len(distance1) == 0:
        distance1 = 0
    else:
        for i in range(len(distance1)):
            data1['TRACK'][i]['DISTANCE'] = distance1[i]
            if distance1[i] == 0:
                data1['TRACK'][i]['SPEED'] = 0.0
            else:
                data1['TRACK'][i]['SPEED'] = distance1[i]/((datetime.strptime(data1['TRACK'][i]['DETECTIONTIME'], '%Y-%m-%dT%H:%M:%S')) - (datetime.strptime(data1['TRACK'][i+1]['DETECTIONTIME'], '%Y-%m-%dT%H:%M:%S'))).total_seconds()

    distance2 = calculate_distance(track2_record)
    if len(distance2) == 0:
        distance2 = 0
    else:
        for i in range(len(distance2)):
            data2['TRACK'][i]['DISTANCE'] = distance2[i]
            if distance2[i] == 0:
                data2['TRACK'][i]['SPEED'] = 0.0
            else:
                data2['TRACK'][i]['SPEED'] = distance2[i]/((datetime.strptime(data2['TRACK'][i]['DETECTIONTIME'], '%Y-%m-%dT%H:%M:%S')) - (datetime.strptime(data2['TRACK'][i+1]['DETECTIONTIME'], '%Y-%m-%dT%H:%M:%S'))).total_seconds()

    avg_distance1 = np.mean(distance1)
    avg_distance2 = np.mean(distance2)
    
    if abs(avg_distance1 - avg_distance2) > distance_threshold:
        return False

    if not check_same_direction([(record['LATITUDE'], record['LONGITUDE']) for record in track1], direction_threshold) or not check_same_direction([(record['LATITUDE'], record['LONGITUDE']) for record in track2], direction_threshold):
        return False

    # Combine the JSON messages
    combined_data = data1.copy()
    combined_data['TRACK'].extend(data2['TRACK'])

    # Sort the combined track by DETECTIONTIME in descending order (newest to oldest)
    combined_data['TRACK'].sort(key=lambda record: datetime.strptime(record['DETECTIONTIME'], '%Y-%m-%dT%H:%M:%S'), reverse=True)
    
    combined_track_record = [(record['LATITUDE'], record['LONGITUDE']) for record in combined_data['TRACK']]
    combined_distances = calculate_distance(combined_track_record)

    if len(combined_distances) == 0:
        combined_distances = 0
    else:
        for i in range(len(combined_distances)):
            combined_data['TRACK'][i]['DISTANCE'] = combined_distances[i]
            if combined_distances[i] == 0:
                combined_data['TRACK'][i]['SPEED'] = 0.0
            else:
                combined_data['TRACK'][i]['SPEED'] = combined_distances[i]/((datetime.strptime(combined_data['TRACK'][i]['DETECTIONTIME'], '%Y-%m-%dT%H:%M:%S')) - (datetime.strptime(combined_data['TRACK'][i+1]['DETECTIONTIME'], '%Y-%m-%dT%H:%M:%S'))).total_seconds()

    return combined_data

def calculate_track(json_string, distance_threshold, direction_threshold):
# Load your JSON data
    data = json.loads(json_string)

    # Compare the data items and create a new JSON message for matching items
    i = 0
    while i < len(data):
        j = i + 1
        while j < len(data):
            combined_data = is_same_track(data[i], data[j], distance_threshold, direction_threshold)
            if combined_data:

                # Update the data list
                data[i] = combined_data
                del data[j]
            else:
                j += 1
        i += 1
    return data

# The direction_threshold is a unitless value between -1 and 1, inclusive. 
# It’s based on the dot product of the direction vectors of the track points. 
# If the dot product of the direction vectors of two consecutive points is less than the direction_threshold, 
# the track is not considered to be moving in the same direction. 
# A direction_threshold of 1 means the direction has to be exactly the same, 
# while a direction_threshold of -1 means the direction can be exactly opposite. 
# A direction_threshold of 0 allows for perpendicular directions. 
# You can adjust these thresholds based on your specific requirements. 