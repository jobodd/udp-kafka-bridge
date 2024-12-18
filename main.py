import socket
from kafka import KafkaProducer
import time

# Kafka configuration
# KAFKA_BROKER = "10.1.85.195:9094"
KAFKA_BROKER = "10.1.85.195:9094,10.1.85.196:9094,10.1.85.197:9094"
# KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
KAFKA_TOPIC = 'simulator'   # Kafka topic name

# UDP configuration
UDP_IP = "0.0.0.0"  # Listen on all available interfaces
UDP_PORT = 1234      # UDP port to listen on

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

# Create a UDP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

print(f"Listening for UDP packets on {UDP_IP}:{UDP_PORT} and sending to Kafka topic '{KAFKA_TOPIC}'...")

# Loop to read UDP data and send it to Kafka
try:
    while True:
        data, addr = sock.recvfrom(1024)  # Receive data (max 1024 bytes)
        print(f"Received message from {addr}: {data.decode('utf-8', errors='ignore')}")
        
        producer.send(KAFKA_TOPIC, value=data)
        
        producer.flush()
        print(data)

        # Sleep to simulate a continuous loop with slight delay (optional)
        time.sleep(0.1)
except KeyboardInterrupt:
    print("Terminating due to KeyboardInterrupt...")
finally:
    sock.close()
    producer.close()
    print("Socket and Kafka producer closed.")

