import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json
import time
from kafka import KafkaProducer

# Global counter for flight numbers
flight_counter = 100

def generate_flight_batch(size=1000, start_index=None):
    global flight_counter
    airports = ["AYD", "VNS", "IXD", "HYD"]
    now = datetime.now()
    # Use provided start_index or global counter
    start = start_index if start_index is not None else flight_counter
    flights = {
        "flight_number": [f"FL{i:03d}" for i in range(start, start + size)],
        "departure_airport": np.random.choice(airports, size),
        "arrival_airport": np.random.choice(airports, size),
        "scheduled_departure": [(now + timedelta(minutes=int(m))).strftime("%Y-%m-%d %H:%M:%S") 
                               for m in np.random.randint(0, 120, size)],
        "temperature": np.random.uniform(0, 40, size),
        "wind_speed": np.random.uniform(0, 20, size),
        "precipitation": np.random.uniform(0, 10, size),
        "delay_minutes": np.where(np.random.random(size) > 0.7, np.random.randint(0, 60, size), 0)
    }
    df = pd.DataFrame(flights)
    # Ensure arrival != departure
    df["arrival_airport"] = df.apply(lambda row: np.random.choice([a for a in airports if a != row["departure_airport"]]), axis=1)
    # Update counter
    if start_index is None:
        flight_counter = start + size
    return df

# Save sample for ML (use fixed range)
sample_df = generate_flight_batch(1000, start_index=100)
sample_df.to_json("D:/flight_data_sample.json", orient="records", lines=True)

# Real-time producer
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode("utf-8"))
while True:
    flight = generate_flight_batch(1).iloc[0].to_dict()
    producer.send("flight-data", flight)
    print(f"Sent: {flight['flight_number']}")
    time.sleep(1)