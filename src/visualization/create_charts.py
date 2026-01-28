import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Create output directory
os.makedirs('data/processed/charts', exist_ok=True)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

print("=" * 80)
print(" " * 20 + "CREATING PUBMATIC ANALYTICS VISUALIZATIONS")
print("=" * 80)

# Load data
print("\n📊 Loading data...")
revenue_df = pd.read_csv('data/processed/revenue_analysis.csv')
device_df = pd.read_csv('data/processed/device_analysis.csv')
campaign_df = pd.read_csv('data/processed/campaign_analysis.csv')

try:
    fraud_df = pd.read_csv('data/processed/fraud_analysis.csv')
    has_fraud = len(fraud_df) > 0
except:
    has_fraud = False
    print("⚠️  No fraud data found, skipping fraud visualization")

print(f"✅ Loaded: {len(revenue_df)} publishers, {len(campaign_df)} campaigns")

# ============= CHART 1: Top Publishers =============
print("\n📈 Creating Chart 1: Top Publishers by Revenue...")
fig, ax = plt.subplots(figsize=(14, 8))
top_publishers = revenue_df.nlargest(15, 'total_revenue')

bars = ax.barh(top_publishers['publisher_id'], top_publishers['total_revenue'], 
               color=sns.color_palette("viridis", len(top_publishers)))
ax.set_xlabel('Total Revenue ($)', fontsize=12, fontweight='bold')
ax.set_ylabel('Publisher ID', fontsize=12, fontweight='bold')
ax.set_title('Top 15 Publishers by Revenue\nPubMatic Ad Analytics Platform', 
             fontsize=16, fontweight='bold', pad=20)

for i, (idx, row) in enumerate(top_publishers.iterrows()):
    ax.text(row['total_revenue'], i, f" ${row['total_revenue']:.2f}", 
            va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('data/processed/charts/1_top_publishers_revenue.png', dpi=300, bbox_inches='tight')
print("✅ Saved: 1_top_publishers_revenue.png")
plt.close()

# ============= CHART 2: Device Performance =============
print("📈 Creating Chart 2: Device Performance...")
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Device Performance Analysis - PubMatic Analytics', 
             fontsize=18, fontweight='bold', y=0.995)

colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']

# Impressions
ax1 = axes[0, 0]
ax1.pie(device_df['impressions'], labels=device_df['device_type'].str.capitalize(), 
        autopct='%1.1f%%', startangle=90, colors=colors, 
        textprops={'fontsize': 11, 'fontweight': 'bold'})
ax1.set_title('Impressions Distribution', fontsize=14, fontweight='bold', pad=15)

# Revenue
ax2 = axes[0, 1]
bars = ax2.bar(device_df['device_type'].str.capitalize(), device_df['total_revenue'], 
               color=colors, edgecolor='black', linewidth=1.5)
ax2.set_ylabel('Total Revenue ($)', fontsize=12, fontweight='bold')
ax2.set_title('Revenue by Device Type', fontsize=14, fontweight='bold', pad=15)
for bar in bars:
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height,
            f'${height:.2f}', ha='center', va='bottom', fontsize=11, fontweight='bold')

# CTR
ax3 = axes[1, 0]
bars = ax3.bar(device_df['device_type'].str.capitalize(), device_df['ctr_percent'], 
               color=colors, edgecolor='black', linewidth=1.5)
ax3.set_ylabel('Click-Through Rate (%)', fontsize=12, fontweight='bold')
ax3.set_title('CTR by Device Type', fontsize=14, fontweight='bold', pad=15)
for bar in bars:
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.2f}%', ha='center', va='bottom', fontsize=11, fontweight='bold')

# Conversion Rate
ax4 = axes[1, 1]
bars = ax4.bar(device_df['device_type'].str.capitalize(), device_df['cvr_percent'], 
               color=colors, edgecolor='black', linewidth=1.5)
ax4.set_ylabel('Conversion Rate (%)', fontsize=12, fontweight='bold')
ax4.set_title('Conversion Rate by Device Type', fontsize=14, fontweight='bold', pad=15)
for bar in bars:
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.2f}%', ha='center', va='bottom', fontsize=11, fontweight='bold')

plt.tight_layout()
plt.savefig('data/processed/charts/2_device_performance.png', dpi=300, bbox_inches='tight')
print("✅ Saved: 2_device_performance.png")
plt.close()

# ============= CHART 3: Campaign ROI =============
print("📈 Creating Chart 3: Campaign ROI Analysis...")
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle('Campaign ROI Analysis - PubMatic Analytics', fontsize=18, fontweight='bold')

# ROI histogram
ax1 = axes[0]
ax1.hist(campaign_df['roi_percent'], bins=30, color='#4ECDC4', edgecolor='black', alpha=0.7)
ax1.axvline(campaign_df['roi_percent'].mean(), color='red', linestyle='--', linewidth=2, 
            label=f'Mean: {campaign_df["roi_percent"].mean():.1f}%')
ax1.axvline(0, color='gray', linestyle='-', linewidth=1.5, label='Break-even')
ax1.set_xlabel('ROI (%)', fontsize=12, fontweight='bold')
ax1.set_ylabel('Number of Campaigns', fontsize=12, fontweight='bold')
ax1.set_title('ROI Distribution', fontsize=14, fontweight='bold')
ax1.legend(fontsize=11)
ax1.grid(alpha=0.3)

# Top campaigns
ax2 = axes[1]
top_campaigns = campaign_df.nlargest(10, 'roi_percent')
bars = ax2.barh(range(len(top_campaigns)), top_campaigns['roi_percent'], 
                color=sns.color_palette("RdYlGn", len(top_campaigns)))
ax2.set_yticks(range(len(top_campaigns)))
ax2.set_yticklabels(top_campaigns['campaign_id'], fontsize=10)
ax2.set_xlabel('ROI (%)', fontsize=12, fontweight='bold')
ax2.set_title('Top 10 Campaigns by ROI', fontsize=14, fontweight='bold')
for i, roi in enumerate(top_campaigns['roi_percent']):
    ax2.text(roi, i, f' {roi:.1f}%', va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('data/processed/charts/3_campaign_roi.png', dpi=300, bbox_inches='tight')
print("✅ Saved: 3_campaign_roi.png")
plt.close()

# ============= CHART 4: Executive Dashboard =============
print("📈 Creating Chart 4: Executive Dashboard...")
fig = plt.figure(figsize=(18, 10))
gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)

fig.suptitle('PubMatic Analytics - Executive Dashboard', fontsize=20, fontweight='bold', y=0.98)

# KPIs
total_impressions = revenue_df['impressions'].sum()
total_clicks = revenue_df['clicks'].sum()
total_conversions = revenue_df['conversions'].sum()
total_revenue = revenue_df['total_revenue'].sum()
avg_ctr = revenue_df['ctr_percent'].mean()

kpis = [
    ('Total Impressions', f'{total_impressions:,}', '#FF6B6B'),
    ('Total Clicks', f'{total_clicks:,}', '#4ECDC4'),
    ('Total Revenue', f'${total_revenue:,.2f}', '#96CEB4'),
]

for idx, (label, value, color) in enumerate(kpis):
    ax = fig.add_subplot(gs[0, idx])
    ax.text(0.5, 0.6, value, ha='center', va='center', fontsize=24, fontweight='bold')
    ax.text(0.5, 0.2, label, ha='center', va='center', fontsize=14, fontweight='bold')
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    ax.add_patch(plt.Rectangle((0.05, 0.05), 0.9, 0.9, fill=True, 
                                facecolor=color, alpha=0.3, edgecolor='black', linewidth=2))

# Device pie
ax_device = fig.add_subplot(gs[1, 0])
ax_device.pie(device_df['total_revenue'], labels=device_df['device_type'].str.capitalize(), 
              autopct='%1.1f%%', colors=colors, textprops={'fontsize': 10, 'fontweight': 'bold'})
ax_device.set_title('Revenue by Device', fontsize=12, fontweight='bold')

# Top publishers
ax_top = fig.add_subplot(gs[1, 1:])
top_5 = revenue_df.nlargest(5, 'total_revenue')
bars = ax_top.barh(top_5['publisher_id'], top_5['total_revenue'], 
                   color=sns.color_palette("viridis", 5))
ax_top.set_xlabel('Revenue ($)', fontsize=11, fontweight='bold')
ax_top.set_title('Top 5 Publishers', fontsize=12, fontweight='bold')
for i, (idx, row) in enumerate(top_5.iterrows()):
    ax_top.text(row['total_revenue'], i, f" ${row['total_revenue']:.2f}", 
               va='center', fontsize=9, fontweight='bold')

# Campaign profitability
ax_roi = fig.add_subplot(gs[2, :])
profitable = len(campaign_df[campaign_df['roi_percent'] > 0])
unprofitable = len(campaign_df[campaign_df['roi_percent'] <= 0])
ax_roi.bar(['Profitable', 'Unprofitable'], [profitable, unprofitable], 
           color=['#96CEB4', '#FF6B6B'], edgecolor='black', linewidth=1.5)
ax_roi.set_ylabel('Number of Campaigns', fontsize=11, fontweight='bold')
ax_roi.set_title(f'Campaign Profitability ({profitable}/{len(campaign_df)} profitable)', 
                fontsize=12, fontweight='bold')
for i, v in enumerate([profitable, unprofitable]):
    ax_roi.text(i, v, str(v), ha='center', va='bottom', fontsize=14, fontweight='bold')

plt.savefig('data/processed/charts/4_executive_dashboard.png', dpi=300, bbox_inches='tight')
print("✅ Saved: 4_executive_dashboard.png")
plt.close()

print("\n" + "=" * 80)
print("🎉 ALL VISUALIZATIONS CREATED SUCCESSFULLY!")
print("=" * 80)
print(f"\n📁 Charts saved in: data/processed/charts/")
print("\nGenerated files:")
print("  1. 1_top_publishers_revenue.png")
print("  2. 2_device_performance.png")
print("  3. 3_campaign_roi.png")
print("  4. 4_executive_dashboard.png")
print("\n" + "=" * 80)