import datetime
import random

# Constants
SPEED = 30 / 60000  # 30 meters per minute in degrees (approximate)
ANGLE_CHANGE = 10  # degrees
DIRECTION = [0, 1]  # Moving north
START_TIME = datetime.datetime.now()
TIME_DELTA = datetime.timedelta(minutes=1)  # 1 minute

# Initialize the positions and directions of the cars
#positions = [[0, 0] for _ in range(3)]
#directions = [DIRECTION for _ in range(3)]

positions = [[0, 0],[0, 0],[0, 0]]
directions = [[0, 1],[0, 1],[0, 1]]

# Generate the data
data = []

def generate_data():
    for minute in range(60):
        time = START_TIME - minute * TIME_DELTA
        for car in range(3):
            if minute < 10 or (minute >= 15 and car != 0) or (minute >= 30 and car == 2):
                # Move the car
                positions[car] = [positions[car][i] + SPEED * directions[car][i] for i in range(2)]
            if minute == 30 and car == 2:
                # Change direction
                directions[car] = [directions[car][1], -directions[car][0]]
            # Add the position to the data
            data.append({
                "ORIGINATORID": str(car),
                "DETECTIONTIME": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "LATITUDE": positions[car][0],
                "LONGITUDE": positions[car][1]
            })
    return data

