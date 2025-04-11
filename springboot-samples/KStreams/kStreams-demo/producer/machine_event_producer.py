from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime

fake = Faker()

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Example industries and statuses
industries = ['Automobile', 'Textile', 'Pharmaceutical', 'Food Processing', 'Electronics']
statuses = ['ACTIVE', 'IDLE', 'MAINTENANCE']

def generate_machine_event():
    return {
        "machineId": f"MCH-{fake.unique.random_int(min=1000, max=9999)}",
        "industry": random.choice(industries),
        "location": fake.city(),
        "temperature": round(random.uniform(40.0, 120.0), 2),
        "vibration": round(random.uniform(0.5, 20.0), 2),
        "intensity": random.randint(10, 100),
        "status": random.choice(statuses),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

print("Producing 10 MachineEvent messages to Kafka...")

for _ in range(10):
    event = generate_machine_event()
    producer.send('mytopic', value=event)
    print("Produced:", event)
    time.sleep(1)

# Ensure all messages are sent
producer.flush()
producer.close()

print("Finished producing messages.")

#
# PS C:\tmp\kStreams-demo\producer> python .\machine_event_producer.py
# Producing 10 MachineEvent messages to Kafka...
# C:\tmp\kStreams-demo\producer\machine_event_producer.py:29: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
# "timestamp": datetime.utcnow().isoformat() + "Z"
# Produced: {'machineId': 'MCH-3352', 'industry': 'Textile', 'location': 'Lake Tinaton', 'temperature': 50.43, 'vibration': 9.85, 'intensity': 16, 'status': 'IDLE', 'timestamp': '2025-04-11T17:12:34.948043Z'}
# Produced: {'machineId': 'MCH-4703', 'industry': 'Food Processing', 'location': 'Wesleyfort', 'temperature': 68.61, 'vibration': 7.15, 'intensity': 38, 'status': 'IDLE', 'timestamp': '2025-04-11T17:12:35.950523Z'}
# Produced: {'machineId': 'MCH-5016', 'industry': 'Pharmaceutical', 'location': 'Katelynton', 'temperature': 114.1, 'vibration': 19.99, 'intensity': 21, 'status': 'IDLE', 'timestamp': '2025-04-11T17:12:36.952079Z'}
# Produced: {'machineId': 'MCH-2990', 'industry': 'Food Processing', 'location': 'East Brianahaven', 'temperature': 91.31, 'vibration': 7.43, 'intensity': 71, 'status': 'ACTIVE', 'timestamp': '2025-04-11T17:12:37.953308Z'}
# Produced: {'machineId': 'MCH-5955', 'industry': 'Food Processing', 'location': 'Riceborough', 'temperature': 58.45, 'vibration': 13.69, 'intensity': 95, 'status': 'IDLE', 'timestamp': '2025-04-11T17:12:38.954579Z'}
# Produced: {'machineId': 'MCH-8493', 'industry': 'Food Processing', 'location': 'North Melinda', 'temperature': 68.94, 'vibration': 5.43, 'intensity': 68, 'status': 'IDLE', 'timestamp': '2025-04-11T17:12:39.955574Z'}
# Produced: {'machineId': 'MCH-5114', 'industry': 'Pharmaceutical', 'location': 'South Kathleen', 'temperature': 40.96, 'vibration': 18.54, 'intensity': 54, 'status': 'IDLE', 'timestamp': '2025-04-11T17:12:40.956660Z'}
# Produced: {'machineId': 'MCH-1347', 'industry': 'Pharmaceutical', 'location': 'North Jimburgh', 'temperature': 118.4, 'vibration': 11.5, 'intensity': 46, 'status': 'MAINTENANCE', 'timestamp': '2025-04-11T17:12:41.957538Z'}
# Produced: {'machineId': 'MCH-1520', 'industry': 'Automobile', 'location': 'Millstown', 'temperature': 118.99, 'vibration': 17.24, 'intensity': 64, 'status': 'IDLE', 'timestamp': '2025-04-11T17:12:42.958529Z'}
# Produced: {'machineId': 'MCH-4630', 'industry': 'Textile', 'location': 'Gloriaberg', 'temperature': 72.8, 'vibration': 5.08, 'intensity': 73, 'status': 'ACTIVE', 'timestamp': '2025-04-11T17:12:43.959627Z'}
# Finished producing messages.
# PS C:\tmp\kStreams-demo\producer>
#
#
