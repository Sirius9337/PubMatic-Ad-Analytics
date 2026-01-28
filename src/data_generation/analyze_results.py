import pandas as pd
import numpy as np

# Load the results
df = pd.read_csv('data/processed/revenue_analysis.csv')

print("=" * 80)
print(" " * 20 + "PUBMATIC AD ANALYTICS - SUMMARY REPORT")
print("=" * 80)

print("\n📊 OVERALL STATISTICS:")
print(f"   Total Publishers Analyzed:     {len(df)}")
print(f"   Total Impressions:             {df['impressions'].sum():,}")
print(f"   Total Clicks:                  {df['clicks'].sum():,}")
print(f"   Total Conversions:             {df['conversions'].sum():,}")
print(f"   Total Revenue:                 ${df['total_revenue'].sum():,.2f}")
print(f"   Average CTR:                   {df['ctr_percent'].mean():.2f}%")
print(f"   Average Conversion Rate:       {df['cvr_percent'].mean():.2f}%")
print(f"   Average Revenue per Publisher: ${df['total_revenue'].mean():,.2f}")

print("\n" + "=" * 80)
print("🏆 TOP 5 PUBLISHERS BY REVENUE:")
print("=" * 80)
top_revenue = df.nlargest(5, 'total_revenue')
for idx, row in top_revenue.iterrows():
    print(f"\n{row['publisher_id']}:")
    print(f"   Impressions:     {row['impressions']:,}")
    print(f"   Clicks:          {row['clicks']:,}")
    print(f"   Conversions:     {row['conversions']:,}")
    print(f"   Total Revenue:   ${row['total_revenue']:,.2f}")
    print(f"   CTR:             {row['ctr_percent']:.2f}%")
    print(f"   CVR:             {row['cvr_percent']:.2f}%")

print("\n" + "=" * 80)
print("🎯 TOP 5 PUBLISHERS BY CTR (Click-Through Rate):")
print("=" * 80)
top_ctr = df.nlargest(5, 'ctr_percent')
for idx, row in top_ctr.iterrows():
    print(f"{row['publisher_id']}: {row['ctr_percent']:.2f}% CTR | {row['clicks']} clicks | ${row['total_revenue']:.2f} revenue")

print("\n" + "=" * 80)
print("💰 TOP 5 PUBLISHERS BY CONVERSION RATE:")
print("=" * 80)
top_cvr = df.nlargest(5, 'cvr_percent')
for idx, row in top_cvr.iterrows():
    print(f"{row['publisher_id']}: {row['cvr_percent']:.2f}% CVR | {row['conversions']} conversions | ${row['total_revenue']:.2f} revenue")

print("\n" + "=" * 80)
print("⚠️  UNDERPERFORMING PUBLISHERS (Lowest Revenue):")
print("=" * 80)
bottom_revenue = df.nsmallest(5, 'total_revenue')
for idx, row in bottom_revenue.iterrows():
    print(f"{row['publisher_id']}: ${row['total_revenue']:.2f} revenue | {row['ctr_percent']:.2f}% CTR | {row['conversions']} conversions")

print("\n" + "=" * 80)
print("💡 KEY INSIGHTS:")
print("=" * 80)

# Calculate insights
high_ctr_low_cvr = df[(df['ctr_percent'] > df['ctr_percent'].median()) & 
                       (df['cvr_percent'] < df['cvr_percent'].median())]
print(f"\n📌 Publishers with HIGH CTR but LOW Conversion: {len(high_ctr_low_cvr)}")
print("   → These publishers get clicks but don't convert - may need better ad quality")

low_ctr_high_cvr = df[(df['ctr_percent'] < df['ctr_percent'].median()) & 
                       (df['cvr_percent'] > df['cvr_percent'].median())]
print(f"\n📌 Publishers with LOW CTR but HIGH Conversion: {len(low_ctr_high_cvr)}")
print("   → These publishers have quality traffic - should increase ad volume")

print("\n" + "=" * 80)