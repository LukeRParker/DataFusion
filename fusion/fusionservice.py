import hashlib
from datetime import datetime, timedelta
import json
import schedule
import time 
from haversine import haversine, Unit
from calculatetrack import calculate_track
from testdatagen import generate_data

stalevalue = 60            # Minutes. Ignore messages that have a DETECTIONTIME > 60 minutes ago.
distance_threshold = 100    # Meters. OBJECTIDs that are further apart than this distance are considered different.
direction_threshold = 0.9   # See note below.
poll_frequency = 1          # Minutes. How often to get new messages

# Direction Threshold:
# The direction_threshold is a unitless value between -1 and 1, inclusive. 
# It’s based on the dot product of the direction vectors of the track points. 
# If the dot product of the direction vectors of two consecutive points is less than the direction_threshold, 
# the track is not considered to be moving in the same direction. 
# A direction_threshold of 1 means the direction has to be exactly the same, 
# while a direction_threshold of -1 means the direction can be exactly opposite. 
# A direction_threshold of 0 allows for perpendicular directions. 
# You can adjust these thresholds based on your specific requirements. 

now_utc = datetime.now()
hash_list = []

data = generate_data() # This is where you'd subscribe to data on the original raw track data topic.

def send_messages(hash_list):
    # This is where you'd produce to Kafka on your 'Fusion' Topic rather than create a json.

    with open(f'./data/data-sent.json', 'w') as f:
        json.dump(hash_list, f, indent=4)
  

def track_updater(hash_list, message):

    for existing_message in hash_list:

        if message["HASH"] == existing_message["HASH"]:
            print(f"DUPLICATE (TIME): {message['HASH']} is a duplicate of {existing_message['HASH']}")
            return hash_list

        existing_datetime_value = datetime.timestamp(datetime.strptime(existing_message["DETECTIONTIME"], '%Y-%m-%dT%H:%M:%S'))
        new_datetime_value = datetime.timestamp(datetime.strptime(message["DETECTIONTIME"], '%Y-%m-%dT%H:%M:%S'))

        if message["ORIGINATORID"] == existing_message["ORIGINATORID"]:
            if existing_datetime_value < new_datetime_value:
                message["TRACK"].append({"ORIGINATORID": existing_message["ORIGINATORID"],
                                        "DETECTIONTIME": existing_message["DETECTIONTIME"],
                                        "LATITUDE": existing_message["LATITUDE"],
                                        "LONGITUDE": existing_message["LONGITUDE"]})
                hash_list.append(message)
                hash_list = calculate_track(json.dumps(hash_list),distance_threshold, direction_threshold)
 
                return hash_list
            
            else:
                existing_message["TRACK"].append({"ORIGINATORID": message["ORIGINATORID"],
                                        "DETECTIONTIME": message["DETECTIONTIME"],
                                        "LATITUDE": message["LATITUDE"],
                                        "LONGITUDE": message["LONGITUDE"]})
                hash_list = calculate_track(json.dumps(hash_list),distance_threshold, direction_threshold)

                return hash_list

    message["TRACK"].append({"ORIGINATORID": message["ORIGINATORID"],
                            "DETECTIONTIME": message["DETECTIONTIME"],
                            "LATITUDE": message["LATITUDE"],
                            "LONGITUDE": message["LONGITUDE"]})
    hash_list.append(message)
    hash_list = calculate_track(json.dumps(hash_list),distance_threshold, direction_threshold)

    return hash_list
       

def stale_message(datetime_value):

    now_utc = datetime.now()
    stale_time = (now_utc - timedelta(minutes=stalevalue)).strftime('%Y-%m-%dT%H:%M:%S') #Size of the window
    stale_time = datetime.timestamp(datetime.strptime(stale_time, '%Y-%m-%dT%H:%M:%S'))
    datetime_value = datetime.timestamp(datetime.strptime(datetime_value, '%Y-%m-%dT%H:%M:%S'))

    if datetime_value <= stale_time:
        return True
    else:
        return False
    

def get_messages(data, hash_list):

    new_hash_list = []
    # create a new list that includes only the items where stale_message is False
    for message in hash_list:
        detection_time = message["DETECTIONTIME"]
        if stale_message(detection_time) is True:
            continue
        else:
            new_hash_list.append(message)

    hash_list = new_hash_list
    
    n=0 # For demo only

    for message in data:
        id_value = message["ORIGINATORID"]
        datetime_value = message["DETECTIONTIME"]

        if stale_message(datetime_value) is True:
            continue

        else:
            unique_id = id_value + "|" + datetime_value
            unique_id_bytes = unique_id.encode()

            unique_id_hash = hashlib.sha256(unique_id_bytes)
            unique_id_hash_value = unique_id_hash.hexdigest()
            message["HASH"] = unique_id_hash_value
            message["TRACK"] = []

            hash_list = track_updater(hash_list, message)
    
            # For demo purposes only!
            n = n+1
            if n < 12:
                with open(f'./data/data-after-message-{n}.json', 'w') as f:
                    json.dump(hash_list, f, indent=4)
            # End of demo addition ^^

    send_messages(hash_list)  #Output each message in hash_out to fusion topic.


get_messages(data, hash_list)
schedule.every(1).minutes.do(get_messages,data, hash_list)

while True:
    schedule.run_pending()
