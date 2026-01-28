import sys

# Explicitly use kafka-python
try:
    from kafka import KafkaConsumer
except ImportError:
    print("❌ Error: kafka-python not installed")
    print("Run: pip install kafka-python")
    sys.exit(1)

import json
from collections import defaultdict
import time

class RealTimeAdConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='ad-events'):
        print("🔌 Connecting to Kafka...")
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='ad-analytics-group',
                api_version=(0, 10, 1)
            )
            
            # Real-time metrics
            self.metrics = {
                'total_impressions': 0,
                'total_clicks': 0,
                'total_conversions': 0,
                'total_revenue': 0.0,
                'by_publisher': defaultdict(lambda: {'impressions': 0, 'clicks': 0, 'revenue': 0}),
                'by_device': defaultdict(lambda: {'impressions': 0, 'clicks': 0, 'revenue': 0})
            }
            
            self.start_time = time.time()
            print("✅ Connected to Kafka successfully!")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            print("Make sure Kafka is running: docker ps | grep kafka")
            sys.exit(1)
        
    def consume_and_analyze(self):
        """Consume events and show real-time analytics"""
        print("\n📡 Listening for ad events...")
        print("=" * 80 + "\n")
        
        try:
            for message in self.consumer:
                event = message.value
                
                # Update metrics
                self.metrics['total_impressions'] += 1
                self.metrics['total_clicks'] += event['clicked']
                self.metrics['total_conversions'] += event['converted']
                self.metrics['total_revenue'] += event['revenue']
                
                # By publisher
                pub = event['publisher_id']
                self.metrics['by_publisher'][pub]['impressions'] += 1
                self.metrics['by_publisher'][pub]['clicks'] += event['clicked']
                self.metrics['by_publisher'][pub]['revenue'] += event['revenue']
                
                # By device
                device = event['device_type']
                self.metrics['by_device'][device]['impressions'] += 1
                self.metrics['by_device'][device]['clicks'] += event['clicked']
                self.metrics['by_device'][device]['revenue'] += event['revenue']
                
                # Print dashboard every 50 impressions
                if self.metrics['total_impressions'] % 50 == 0:
                    self.print_dashboard()
                    
        except KeyboardInterrupt:
            print("\n⏹️  Stopped consuming")
            self.print_final_report()
    
    def print_dashboard(self):
        """Print real-time dashboard"""
        elapsed = time.time() - self.start_time
        rate = self.metrics['total_impressions'] / elapsed
        ctr = (self.metrics['total_clicks'] / self.metrics['total_impressions'] * 100) if self.metrics['total_impressions'] > 0 else 0
        
        print("\n" + "=" * 80)
        print(f"📊 REAL-TIME DASHBOARD | {self.metrics['total_impressions']:,} impressions | {rate:.1f} imp/sec")
        print("=" * 80)
        print(f"Clicks:       {self.metrics['total_clicks']:,} (CTR: {ctr:.2f}%)")
        print(f"Conversions:  {self.metrics['total_conversions']:,}")
        print(f"Revenue:      ${self.metrics['total_revenue']:.2f}")
        
        # Top publishers
        print("\n🏆 Top 5 Publishers:")
        top_pubs = sorted(self.metrics['by_publisher'].items(), 
                         key=lambda x: x[1]['revenue'], reverse=True)[:5]
        for pub, stats in top_pubs:
            print(f"  {pub}: {stats['impressions']:,} imp | {stats['clicks']} clicks | ${stats['revenue']:.2f}")
        
        # Device breakdown
        print("\n📱 Device Breakdown:")
        for device, stats in sorted(self.metrics['by_device'].items()):
            print(f"  {device.capitalize():8s}: {stats['impressions']:,} imp | {stats['clicks']} clicks | ${stats['revenue']:.2f}")
        print("=" * 80)
    
    def print_final_report(self):
        """Print final summary"""
        print("\n" + "=" * 80)
        print("📄 FINAL STREAMING REPORT")
        print("=" * 80)
        self.print_dashboard()

if __name__ == "__main__":
    consumer = RealTimeAdConsumer()
    consumer.consume_and_analyze()