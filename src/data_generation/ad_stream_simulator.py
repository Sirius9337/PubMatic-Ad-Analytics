import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import csv

class AdStreamSimulator:
    def __init__(self, num_records=100000):
        self.num_records = num_records
        
        # Realistic distributions matching PubMatic's scale
        self.publishers = [f"PUB_{i:04d}" for i in range(1, 51)]  # 50 publishers
        self.advertisers = [f"ADV_{i:04d}" for i in range(1, 201)]  # 200 advertisers
        self.campaigns = [f"CAMP_{i:05d}" for i in range(1, 1001)]  # 1000 campaigns
        self.device_types = ['mobile', 'desktop', 'tablet']
        self.device_weights = [0.6, 0.3, 0.1]  # Mobile-first reality
        
        # Geographic regions for fraud detection
        self.regions = ['US', 'UK', 'IN', 'CA', 'AU', 'DE', 'FR', 'BR', 'JP', 'CN']
        
    def generate_impression(self, timestamp_base=None):
        """Generate a single ad impression event"""
        
        if timestamp_base is None:
            timestamp_base = datetime.now()
        
        # Random timestamp within last 24 hours
        timestamp = timestamp_base - timedelta(seconds=random.randint(0, 86400))
        
        publisher_id = random.choice(self.publishers)
        advertiser_id = random.choice(self.advertisers)
        campaign_id = random.choice(self.campaigns)
        device_type = random.choices(self.device_types, weights=self.device_weights)[0]
        
        # Generate user_id with some repeating users (realistic behavior)
        if random.random() < 0.3:  # 30% repeat users
            user_id = f"USER_{random.randint(100000, 150000)}"  # Smaller pool
        else:
            user_id = f"USER_{random.randint(100000, 999999)}"  # Larger pool
        
        region = random.choice(self.regions)
        
        # Realistic CTR varies by device (mobile: 1.5%, desktop: 2%, tablet: 1.2%)
        ctr_rates = {'mobile': 0.015, 'desktop': 0.020, 'tablet': 0.012}
        clicked = 1 if random.random() < ctr_rates[device_type] else 0
        
        # Conversion rate: 5-10% of clicks
        converted = 1 if clicked and random.random() < 0.08 else 0
        
        # Bid price varies by device and region
        base_price = {'mobile': 1.5, 'desktop': 3.0, 'tablet': 2.0}[device_type]
        region_multiplier = 1.5 if region in ['US', 'UK', 'CA', 'AU'] else 1.0
        bid_price = round(random.uniform(0.5, base_price) * region_multiplier, 2)
        
        # Revenue calculation (CPM model + performance bonuses)
        revenue = 0
        if clicked:
            revenue = bid_price * 0.001  # CPM to CPC conversion
        if converted:
            revenue += bid_price * random.uniform(2, 5)  # Conversion bonus
            
        # Add some fraud patterns (5% of impressions)
        is_suspicious = 0
        if random.random() < 0.05:
            # Fraudulent pattern: same user, many clicks, low conversion
            if clicked and not converted and random.random() < 0.7:
                is_suspicious = 1
                
        return {
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'publisher_id': publisher_id,
            'advertiser_id': advertiser_id,
            'campaign_id': campaign_id,
            'device_type': device_type,
            'user_id': user_id,
            'region': region,
            'clicked': clicked,
            'converted': converted,
            'bid_price': bid_price,
            'revenue': round(revenue, 4),
            'is_suspicious': is_suspicious
        }
    
    def generate_batch(self, output_file='data/generated/ad_events.csv'):
        """Generate batch of impressions"""
        print(f"🚀 Generating {self.num_records:,} ad impressions for PubMatic Analytics...")
        print("=" * 70)
        
        base_time = datetime.now()
        impressions = []
        
        # Show progress
        for i in range(self.num_records):
            impressions.append(self.generate_impression(base_time))
            
            if (i + 1) % 10000 == 0:
                print(f"Progress: {i + 1:,}/{self.num_records:,} ({(i+1)/self.num_records*100:.1f}%)")
        
        # Create DataFrame
        df = pd.DataFrame(impressions)
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        print("\n" + "=" * 70)
        print(f"✅ Data successfully saved to: {output_file}")
        print("=" * 70)
        
        # Display statistics
        print("\n📊 DATASET STATISTICS:")
        print(f"   Total Impressions:    {len(df):,}")
        print(f"   Total Clicks:         {df['clicked'].sum():,} (CTR: {df['clicked'].mean()*100:.2f}%)")
        print(f"   Total Conversions:    {df['converted'].sum():,} (CVR: {df['converted'].sum()/max(df['clicked'].sum(),1)*100:.2f}%)")
        print(f"   Total Revenue:        ${df['revenue'].sum():,.2f}")
        print(f"   Avg Revenue/Imp:      ${df['revenue'].mean():.4f}")
        print(f"   Suspicious Events:    {df['is_suspicious'].sum():,} ({df['is_suspicious'].mean()*100:.1f}%)")
        
        print(f"\n📱 DEVICE BREAKDOWN:")
        device_stats = df.groupby('device_type').agg({
            'clicked': 'sum',
            'revenue': 'sum'
        })
        for device in device_stats.index:
            clicks = device_stats.loc[device, 'clicked']
            revenue = device_stats.loc[device, 'revenue']
            print(f"   {device.capitalize():10s}: {clicks:5,} clicks | ${revenue:8,.2f} revenue")
        
        print(f"\n🌍 TOP 5 REGIONS BY REVENUE:")
        region_revenue = df.groupby('region')['revenue'].sum().sort_values(ascending=False).head(5)
        for region, revenue in region_revenue.items():
            print(f"   {region}: ${revenue:,.2f}")
        
        print("\n" + "=" * 70)
        
        return df

# Main execution
if __name__ == "__main__":
    print("\n" + "="*70)
    print(" " * 15 + "PUBMATIC AD ANALYTICS - DATA GENERATOR")
    print("="*70 + "\n")
    
    # Generate 100,000 impressions (adjust as needed)
    simulator = AdStreamSimulator(num_records=100000)
    df = simulator.generate_batch()
    
    print("\n✅ Ready to upload to HDFS!")
    print("Next step: Run the following command to upload to Hadoop:")
    print("\n   docker cp data/generated/ad_events.csv namenode:/tmp/")
    print("   docker exec -it namenode hdfs dfs -put /tmp/ad_events.csv /pubmatic/input/")
    print("\n" + "="*70 + "\n")