import sys
import os

# Explicitly use kafka-python
try:
    from kafka import KafkaProducer
except ImportError:
    print("❌ Error: kafka-python not installed")
    print("Run: pip install kafka-python")
    sys.exit(1)

import json
import time

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from data_generation.ad_stream_simulator import AdStreamSimulator

class RealTimeAdProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='ad-events'):
        print("🔌 Connecting to Kafka...")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1)
            )
            self.topic = topic
            self.simulator = AdStreamSimulator()
            print("✅ Connected to Kafka successfully!")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            print("Make sure Kafka is running: docker ps | grep kafka")
            sys.exit(1)
        
    def stream_events(self, events_per_second=10):
        """Stream ad events to Kafka"""
        print(f"\n🚀 Streaming to Kafka topic: {self.topic}")
        print(f"📊 Rate: {events_per_second} events/second")
        print("Press Ctrl+C to stop\n")
        print("=" * 80)
        
        event_count = 0
        
        try:
            while True:
                # Generate event
                event = self.simulator.generate_impression()
                
                # Send to Kafka
                self.producer.send(self.topic, value=event)
                
                event_count += 1
                
                # Print status every 50 events
                if event_count % 50 == 0:
                    print(f"✓ Sent {event_count:,} events | Latest: {event['publisher_id']} | ${event['revenue']:.4f} | Device: {event['device_type']}")
                
                # Control rate
                time.sleep(1.0 / events_per_second)
                
        except KeyboardInterrupt:
            print(f"\n{'=' * 80}")
            print(f"⏹️  Stopped streaming after {event_count:,} events")
            print(f"{'=' * 80}")
            self.producer.flush()
            self.producer.close()

if __name__ == "__main__":
    producer = RealTimeAdProducer()
    producer.stream_events(events_per_second=10)