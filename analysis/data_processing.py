"""
Data processing utilities for MATS advertising analysis.
Loads CSVs, fixes data issues, and provides helper functions.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "Advertising data"
MAPPING_FILE = BASE_DIR / "analysis" / "handle_source_mapping.json"


def load_posthog_data():
    """Load aggregated PostHog data."""
    df = pd.read_csv(DATA_DIR / "PostHog data-Grid view.csv", encoding='utf-8-sig')
    # Convert numeric columns
    numeric_cols = ['Events', 'Unique visitors', 'Apply page views', 'Program page views', 'Pageviews']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df


def load_posthog_daily():
    """Load daily PostHog data."""
    df = pd.read_csv(DATA_DIR / "PostHog daily-Grid view.csv", encoding='utf-8-sig')
    # Convert date
    df['Date'] = pd.to_datetime(df['Date'])
    # Convert numeric columns
    numeric_cols = ['Events', 'Pageviews', 'Unique visitors', 'Apply page views', 'Program page views']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df


def load_referral_sources():
    """Load aggregated referral sources data."""
    df = pd.read_csv(DATA_DIR / "10.0 referral sources-Grid view.csv", encoding='utf-8-sig')
    # Convert numeric columns
    numeric_cols = ['Count', 'Total applications', 'Stage 2 advanced', 'Stage 2 rejected', 'Stage 2 pending']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df


def load_referral_daily():
    """Load daily referral sources data."""
    df = pd.read_csv(DATA_DIR / "10.0 referral sources daily-Grid view.csv", encoding='utf-8-sig')
    # Convert date
    df['Date'] = pd.to_datetime(df['Date'])
    # Convert numeric columns
    numeric_cols = ['Cumulative count', 'Cumulative stage 2 advanced',
                    'Cumulative stage 2 rejected', 'Cumulative stage 2 pending']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df


def load_handle_mapping():
    """Load handle-to-source mapping."""
    with open(MAPPING_FILE, 'r') as f:
        mapping = json.load(f)
    # Remove comment keys
    return {k: v for k, v in mapping.items() if not k.startswith('_')}


def calculate_correct_totals(referral_df):
    """
    Recalculate correct (all) totals for referral data.
    The original (all) row sums per-source counts, which double-counts
    applications with multiple sources selected.

    Returns dict with correct totals.
    """
    # Filter out special rows
    sources_only = referral_df[~referral_df['Source'].isin(['(all)', '(no response)'])]

    # The correct total applications is already in the data
    total_apps = referral_df['Total applications'].iloc[0] if 'Total applications' in referral_df.columns else 0

    # For the (all) row, we need the actual unique counts
    # Since we don't have per-application data, we'll estimate from the (all) row's Count
    all_row = referral_df[referral_df['Source'] == '(all)']

    if len(all_row) > 0:
        # The Count in (all) should be total unique applications
        total_count = int(all_row['Count'].iloc[0])
        # Advanced/rejected/pending are overcounted in (all), need to derive from rate
        # For now, estimate based on average rate across sources weighted by volume

        # Actually, let's recalculate from scratch
        # We know total_count = total unique applications
        # We need to find the true advanced/rejected/pending counts

        # The sum of per-source advanced includes duplicates
        # Let's use the fact that total_count is correct and estimate the split
        sum_advanced = sources_only['Stage 2 advanced'].sum()
        sum_rejected = sources_only['Stage 2 rejected'].sum()
        sum_pending = sources_only['Stage 2 pending'].sum()
        sum_total = sources_only['Count'].sum()

        # Estimate true counts by scaling down by the duplication factor
        if sum_total > 0:
            scale = total_count / sum_total
            est_advanced = int(sum_advanced * scale)
            est_rejected = int(sum_rejected * scale)
            est_pending = int(sum_pending * scale)
        else:
            est_advanced = est_rejected = est_pending = 0

        return {
            'count': total_count,
            'advanced': est_advanced,
            'rejected': est_rejected,
            'pending': est_pending,
            'advancement_rate': (est_advanced / total_count * 100) if total_count > 0 else 0
        }

    return None


def get_daily_new_applications(referral_daily_df):
    """
    Calculate daily NEW applications from cumulative data.
    Returns DataFrame with Date, Source, and new application counts.
    """
    # Sort by source and date
    df = referral_daily_df.sort_values(['Source', 'Date'])

    # Calculate daily new counts (difference from previous day)
    result = []
    for source in df['Source'].unique():
        source_df = df[df['Source'] == source].copy()
        source_df = source_df.sort_values('Date')

        # Calculate differences
        source_df['New count'] = source_df['Cumulative count'].diff().fillna(source_df['Cumulative count'])
        source_df['New advanced'] = source_df['Cumulative stage 2 advanced'].diff().fillna(source_df['Cumulative stage 2 advanced'])
        source_df['New rejected'] = source_df['Cumulative stage 2 rejected'].diff().fillna(source_df['Cumulative stage 2 rejected'])
        source_df['New pending'] = source_df['Cumulative stage 2 pending'].diff().fillna(source_df['Cumulative stage 2 pending'])

        result.append(source_df)

    return pd.concat(result, ignore_index=True)


def get_total_daily_traffic(posthog_daily_df):
    """
    Get total daily traffic (summing all handles except (all) if present).
    """
    # Filter out (all) if it exists
    df = posthog_daily_df[posthog_daily_df['Handle'] != '(all)']

    # Group by date and sum
    daily_totals = df.groupby('Date').agg({
        'Events': 'sum',
        'Pageviews': 'sum',
        'Unique visitors': 'sum',
        'Apply page views': 'sum',
        'Program page views': 'sum'
    }).reset_index()

    return daily_totals


def get_top_handles(posthog_df, n=10, metric='Events'):
    """Get top N handles by specified metric."""
    df = posthog_df[~posthog_df['Handle'].isin(['(all)', '(direct)'])]
    return df.nlargest(n, metric)


def get_top_sources(referral_df, n=10, metric='Count', min_count=0):
    """Get top N sources by specified metric."""
    df = referral_df[~referral_df['Source'].isin(['(all)', '(no response)'])]
    if min_count > 0:
        df = df[df['Count'] >= min_count]
    return df.nlargest(n, metric)


def get_sources_by_quality(referral_df, n=10, min_count=20):
    """
    Get top N sources by advancement rate.
    Only includes sources with at least min_count applications for statistical significance.
    """
    df = referral_df[~referral_df['Source'].isin(['(all)', '(no response)'])]
    df = df[df['Count'] >= min_count].copy()

    # Calculate advancement rate
    df['Advancement Rate'] = df['Stage 2 advanced'] / df['Count'] * 100

    return df.nlargest(n, 'Advancement Rate')


def load_all_data():
    """Load all data sources and return as a dict."""
    return {
        'posthog': load_posthog_data(),
        'posthog_daily': load_posthog_daily(),
        'referral': load_referral_sources(),
        'referral_daily': load_referral_daily(),
        'handle_mapping': load_handle_mapping()
    }


if __name__ == '__main__':
    # Test loading
    print("Testing data loading...")
    data = load_all_data()

    print(f"\nPostHog data: {len(data['posthog'])} rows")
    print(f"PostHog daily: {len(data['posthog_daily'])} rows")
    print(f"Referral sources: {len(data['referral'])} rows")
    print(f"Referral daily: {len(data['referral_daily'])} rows")
    print(f"Handle mappings: {len(data['handle_mapping'])} entries")

    print("\nCorrected totals:")
    totals = calculate_correct_totals(data['referral'])
    if totals:
        print(f"  Total applications: {totals['count']}")
        print(f"  Advanced: {totals['advanced']}")
        print(f"  Rejected: {totals['rejected']}")
        print(f"  Pending: {totals['pending']}")
        print(f"  Advancement rate: {totals['advancement_rate']:.1f}%")
