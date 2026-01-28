import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

print("=" * 80)
print(" " * 20 + "PUBMATIC COMPLETE ANALYTICS REPORT")
print("=" * 80)

# Load all results
revenue_df = pd.read_csv('data/processed/revenue_analysis.csv')
fraud_df = pd.read_csv('data/processed/fraud_analysis.csv')
device_df = pd.read_csv('data/processed/device_analysis.csv')
campaign_df = pd.read_csv('data/processed/campaign_analysis.csv')

# ==================== REVENUE ANALYSIS ====================
print("\n" + "=" * 80)
print("💰 REVENUE ANALYSIS BY PUBLISHER")
print("=" * 80)

print(f"\nTotal Publishers: {len(revenue_df)}")
print(f"Total Impressions: {revenue_df['impressions'].sum():,}")
print(f"Total Clicks: {revenue_df['clicks'].sum():,}")
print(f"Total Conversions: {revenue_df['conversions'].sum():,}")
print(f"Total Revenue: ${revenue_df['total_revenue'].sum():,.2f}")
print(f"Average CTR: {revenue_df['ctr_percent'].mean():.2f}%")
print(f"Average CVR: {revenue_df['cvr_percent'].mean():.2f}%")

print("\n🏆 TOP 5 PUBLISHERS BY REVENUE:")
top_pubs = revenue_df.nlargest(5, 'total_revenue')
for idx, row in top_pubs.iterrows():
    print(f"  {row['publisher_id']}: ${row['total_revenue']:.2f} | {row['ctr_percent']:.2f}% CTR | {row['conversions']} conversions")

# ==================== FRAUD DETECTION ====================
print("\n" + "=" * 80)
print("🕵️  FRAUD DETECTION ANALYSIS")
print("=" * 80)

if len(fraud_df) > 0:
    print(f"\n⚠️  Total Suspicious Users Detected: {len(fraud_df)}")
    print(f"Total Fraudulent Impressions: {fraud_df['impressions'].sum():,}")
    print(f"Total Fraudulent Clicks: {fraud_df['clicks'].sum():,}")
    
    print("\n📊 Fraud Patterns:")
    fraud_reasons = fraud_df['fraud_reason'].value_counts()
    for reason, count in fraud_reasons.items():
        print(f"  {reason}: {count} users")
    
    print("\n🚨 TOP 10 MOST SUSPICIOUS USERS:")
    top_fraud = fraud_df.nlargest(10, 'clicks')
    for idx, row in top_fraud.iterrows():
        print(f"  {row['user_id']}: {row['clicks']} clicks, {row['impressions']} impressions, {row['fraud_reason']}")
else:
    print("\n✅ No fraudulent users detected!")

# ==================== DEVICE PERFORMANCE ====================
print("\n" + "=" * 80)
print("📱 DEVICE PERFORMANCE ANALYSIS")
print("=" * 80)

print("\n📊 Performance by Device Type:")
for idx, row in device_df.iterrows():
    print(f"\n{row['device_type'].upper()}:")
    print(f"  Impressions:        {row['impressions']:,}")
    print(f"  Clicks:             {row['clicks']:,}")
    print(f"  Conversions:        {row['conversions']:,}")
    print(f"  Total Revenue:      ${row['total_revenue']:,.2f}")
    print(f"  CTR:                {row['ctr_percent']:.2f}%")
    print(f"  CVR:                {row['cvr_percent']:.2f}%")
    print(f"  Avg Bid Price:      ${row['avg_bid_price']:.2f}")
    print(f"  Revenue per Click:  ${row['revenue_per_click']:.4f}")

# Calculate device market share
device_df['impression_share'] = (device_df['impressions'] / device_df['impressions'].sum()) * 100
device_df['revenue_share'] = (device_df['total_revenue'] / device_df['total_revenue'].sum()) * 100

print("\n📈 Market Share:")
for idx, row in device_df.iterrows():
    print(f"  {row['device_type'].capitalize()}: {row['impression_share']:.1f}% impressions | {row['revenue_share']:.1f}% revenue")

# ==================== CAMPAIGN ROI ====================
print("\n" + "=" * 80)
print("💼 CAMPAIGN ROI ANALYSIS")
print("=" * 80)

print(f"\nTotal Campaigns Analyzed: {len(campaign_df)}")
print(f"Total Campaign Spend: ${campaign_df['total_spend'].sum():,.2f}")
print(f"Total Campaign Revenue: ${campaign_df['total_revenue'].sum():,.2f}")
overall_roi = ((campaign_df['total_revenue'].sum() - campaign_df['total_spend'].sum()) / campaign_df['total_spend'].sum()) * 100
print(f"Overall ROI: {overall_roi:.2f}%")

print("\n🏆 TOP 10 CAMPAIGNS BY ROI:")
top_roi = campaign_df.nlargest(10, 'roi_percent')
for idx, row in top_roi.iterrows():
    print(f"  {row['campaign_id']} ({row['advertiser_id']}): {row['roi_percent']:.2f}% ROI | ${row['total_revenue']:.2f} revenue")

print("\n💸 TOP 10 CAMPAIGNS BY REVENUE:")
top_revenue_campaigns = campaign_df.nlargest(10, 'total_revenue')
for idx, row in top_revenue_campaigns.iterrows():
    print(f"  {row['campaign_id']}: ${row['total_revenue']:.2f} | {row['conversions']} conversions | {row['roi_percent']:.2f}% ROI")

print("\n⚠️  WORST 5 CAMPAIGNS BY ROI:")
worst_roi = campaign_df.nsmallest(5, 'roi_percent')
for idx, row in worst_roi.iterrows():
    print(f"  {row['campaign_id']}: {row['roi_percent']:.2f}% ROI | ${row['total_spend']:.2f} spend | {row['conversions']} conversions")

# ==================== KEY INSIGHTS ====================
print("\n" + "=" * 80)
print("💡 KEY BUSINESS INSIGHTS")
print("=" * 80)

# Insight 1: Best performing device
best_device = device_df.loc[device_df['total_revenue'].idxmax()]
print(f"\n1. 📱 Best Device Type: {best_device['device_type'].upper()}")
print(f"   - Generates ${best_device['total_revenue']:.2f} ({best_device['revenue_share']:.1f}% of total)")
print(f"   - {best_device['ctr_percent']:.2f}% CTR, {best_device['cvr_percent']:.2f}% CVR")

# Insight 2: Fraud impact
if len(fraud_df) > 0:
    fraud_impact = (fraud_df['clicks'].sum() / revenue_df['clicks'].sum()) * 100
    print(f"\n2. 🚨 Fraud Impact: {fraud_impact:.2f}% of all clicks are potentially fraudulent")
    print(f"   - {len(fraud_df)} suspicious users detected")
    print(f"   - Primary pattern: {fraud_df['fraud_reason'].mode()[0]}")

# Insight 3: Campaign performance
profitable_campaigns = len(campaign_df[campaign_df['roi_percent'] > 0])
print(f"\n3. 💰 Campaign Performance: {profitable_campaigns}/{len(campaign_df)} campaigns are profitable")
avg_roi = campaign_df['roi_percent'].mean()
print(f"   - Average ROI: {avg_roi:.2f}%")
print(f"   - Best campaign ROI: {campaign_df['roi_percent'].max():.2f}%")

# Insight 4: Publisher effectiveness
high_performers = len(revenue_df[revenue_df['total_revenue'] > revenue_df['total_revenue'].median()])
print(f"\n4. 📊 Publisher Distribution: {high_performers}/{len(revenue_df)} publishers above median revenue")
print(f"   - Top 20% publishers generate {revenue_df.nlargest(10, 'total_revenue')['total_revenue'].sum() / revenue_df['total_revenue'].sum() * 100:.1f}% of revenue")

print("\n" + "=" * 80)
print("✅ ANALYSIS COMPLETE - All metrics calculated successfully!")
print("=" * 80 + "\n")

# Save summary
summary = {
    'total_impressions': revenue_df['impressions'].sum(),
    'total_clicks': revenue_df['clicks'].sum(),
    'total_conversions': revenue_df['conversions'].sum(),
    'total_revenue': revenue_df['total_revenue'].sum(),
    'avg_ctr': revenue_df['ctr_percent'].mean(),
    'suspicious_users': len(fraud_df),
    'campaigns_analyzed': len(campaign_df),
    'overall_roi': overall_roi
}

summary_df = pd.DataFrame([summary])
summary_df.to_csv('data/processed/executive_summary.csv', index=False)
print("📄 Executive summary saved to: data/processed/executive_summary.csv")