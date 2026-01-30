"""
MATS Advertising Analysis Report Generator

Generates two HTML reports:
1. Executive Summary - High-level insights for leadership
2. Detailed Analysis - Comprehensive data exploration

Usage:
    python generate_reports.py
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from datetime import datetime
import json

from data_processing import (
    load_all_data,
    calculate_correct_totals,
    get_daily_new_applications,
    get_total_daily_traffic,
    get_top_handles,
    get_top_sources,
    get_sources_by_quality
)

# Paths
BASE_DIR = Path(__file__).parent.parent
REPORTS_DIR = BASE_DIR / "reports"

# Color scheme
COLORS = {
    'primary': '#2563eb',      # Blue
    'secondary': '#64748b',    # Gray
    'success': '#22c55e',      # Green
    'warning': '#f59e0b',      # Amber
    'danger': '#ef4444',       # Red
    'advanced': '#22c55e',
    'rejected': '#ef4444',
    'pending': '#f59e0b',
    'background': '#ffffff',
    'text': '#1e293b'
}

# Plotly template
TEMPLATE = 'plotly_white'


def create_metric_card(title, value, subtitle=None, color=COLORS['primary']):
    """Create HTML for a metric card."""
    subtitle_html = f'<div style="color: #64748b; font-size: 14px;">{subtitle}</div>' if subtitle else ''
    return f'''
    <div style="background: linear-gradient(135deg, {color}15, {color}05);
                border: 1px solid {color}30;
                border-radius: 12px;
                padding: 24px;
                text-align: center;
                flex: 1;
                min-width: 200px;">
        <div style="color: #64748b; font-size: 14px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">
            {title}
        </div>
        <div style="color: {color}; font-size: 36px; font-weight: 700; margin-bottom: 4px;">
            {value}
        </div>
        {subtitle_html}
    </div>
    '''


def create_insight_box(title, content, icon="💡"):
    """Create HTML for an insight box."""
    return f'''
    <div style="background: #f8fafc; border-left: 4px solid {COLORS['primary']}; padding: 16px 20px; margin: 16px 0; border-radius: 0 8px 8px 0;">
        <div style="font-weight: 600; color: {COLORS['text']}; margin-bottom: 8px;">
            {icon} {title}
        </div>
        <div style="color: #475569; line-height: 1.6;">
            {content}
        </div>
    </div>
    '''


def generate_executive_summary(data):
    """Generate the executive summary HTML report."""

    posthog = data['posthog']
    referral = data['referral']
    referral_daily = data['referral_daily']

    # Calculate corrected totals
    totals = calculate_correct_totals(referral)

    # Get key metrics from PostHog (excluding special rows)
    posthog_filtered = posthog[~posthog['Handle'].isin(['(all)', '(direct)'])]
    posthog_all = posthog[posthog['Handle'] == '(all)'].iloc[0] if len(posthog[posthog['Handle'] == '(all)']) > 0 else None

    total_visitors = int(posthog_all['Unique visitors']) if posthog_all is not None else posthog_filtered['Unique visitors'].sum()
    total_apply_views = int(posthog_all['Apply page views']) if posthog_all is not None else posthog_filtered['Apply page views'].sum()

    # Get top sources
    top_by_volume = get_top_sources(referral, n=10, metric='Count')
    top_by_quality = get_sources_by_quality(referral, n=10, min_count=20)

    # Best overall source (high quality + decent volume)
    referral_filtered = referral[~referral['Source'].isin(['(all)', '(no response)'])]
    referral_filtered = referral_filtered[referral_filtered['Count'] >= 30].copy()
    referral_filtered['Quality Score'] = (referral_filtered['Stage 2 advanced'] / referral_filtered['Count']) * 100
    if len(referral_filtered) > 0:
        best_overall = referral_filtered.nlargest(1, 'Quality Score').iloc[0]
        best_source_name = best_overall['Source']
        best_source_rate = best_overall['Quality Score']
        best_source_count = int(best_overall['Count'])
    else:
        best_source_name = "N/A"
        best_source_rate = 0
        best_source_count = 0

    # ===== CREATE CHARTS =====

    # 1. Funnel Chart
    funnel_fig = go.Figure(go.Funnel(
        y=['Website Visitors', 'Apply Page Views', 'Applications', 'Stage 2 Advanced'],
        x=[total_visitors, total_apply_views, totals['count'], totals['advanced']],
        textposition="inside",
        textinfo="value+percent initial",
        marker=dict(color=[COLORS['primary'], '#3b82f6', '#60a5fa', COLORS['success']])
    ))
    funnel_fig.update_layout(
        title=dict(text="Application Funnel", font=dict(size=20)),
        height=400,
        margin=dict(t=60, b=40, l=40, r=40),
        template=TEMPLATE
    )

    # 2. Top Sources by Volume (horizontal bar)
    volume_fig = go.Figure(go.Bar(
        y=top_by_volume['Source'],
        x=top_by_volume['Count'],
        orientation='h',
        marker_color=COLORS['primary'],
        text=top_by_volume['Count'],
        textposition='outside'
    ))
    volume_fig.update_layout(
        title=dict(text="Top 10 Sources by Application Volume", font=dict(size=20)),
        height=400,
        margin=dict(t=60, b=40, l=200, r=60),
        xaxis_title="Applications",
        yaxis=dict(autorange="reversed"),
        template=TEMPLATE
    )

    # 3. Top Sources by Quality (with min count filter)
    quality_fig = go.Figure(go.Bar(
        y=top_by_quality['Source'],
        x=top_by_quality['Stage 2 advanced'] / top_by_quality['Count'] * 100,
        orientation='h',
        marker_color=COLORS['success'],
        text=[f"{r:.1f}%" for r in top_by_quality['Stage 2 advanced'] / top_by_quality['Count'] * 100],
        textposition='outside',
        customdata=top_by_quality['Count'],
        hovertemplate='%{y}<br>Advancement Rate: %{x:.1f}%<br>Applications: %{customdata}<extra></extra>'
    ))
    quality_fig.update_layout(
        title=dict(text="Top 10 Sources by Stage 2 Advancement Rate (min 20 apps)", font=dict(size=20)),
        height=400,
        margin=dict(t=60, b=40, l=200, r=80),
        xaxis_title="Advancement Rate (%)",
        xaxis=dict(range=[0, 100]),
        yaxis=dict(autorange="reversed"),
        template=TEMPLATE
    )

    # 4. Application Growth Over Time
    all_daily = referral_daily[referral_daily['Source'] == '(all)'].copy()
    all_daily = all_daily.sort_values('Date')

    growth_fig = go.Figure()
    growth_fig.add_trace(go.Scatter(
        x=all_daily['Date'],
        y=all_daily['Cumulative count'],
        mode='lines+markers',
        name='Total Applications',
        line=dict(color=COLORS['primary'], width=3),
        fill='tozeroy',
        fillcolor='rgba(37, 99, 235, 0.1)'
    ))
    growth_fig.update_layout(
        title=dict(text="Cumulative Applications Over Time", font=dict(size=20)),
        height=400,
        margin=dict(t=60, b=40, l=60, r=40),
        xaxis_title="Date",
        yaxis_title="Total Applications",
        template=TEMPLATE,
        hovermode='x unified'
    )

    # 5. Stage 2 Outcomes Pie
    outcomes_fig = go.Figure(go.Pie(
        labels=['Advanced', 'Rejected', 'Pending'],
        values=[totals['advanced'], totals['rejected'], totals['pending']],
        marker=dict(colors=[COLORS['advanced'], COLORS['rejected'], COLORS['pending']]),
        hole=0.4,
        textinfo='label+percent',
        textposition='outside'
    ))
    outcomes_fig.update_layout(
        title=dict(text="Stage 2 Outcomes", font=dict(size=20)),
        height=350,
        margin=dict(t=60, b=40, l=40, r=40),
        template=TEMPLATE,
        annotations=[dict(text=f"{totals['count']}<br>Total", x=0.5, y=0.5, font_size=16, showarrow=False)]
    )

    # ===== BUILD HTML =====

    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MATS Advertising Analysis - Executive Summary</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f8fafc;
            color: {COLORS['text']};
            line-height: 1.6;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 40px 20px;
        }}
        .header {{
            text-align: center;
            margin-bottom: 40px;
        }}
        .header h1 {{
            font-size: 32px;
            font-weight: 700;
            color: {COLORS['text']};
            margin-bottom: 8px;
        }}
        .header .subtitle {{
            color: #64748b;
            font-size: 16px;
        }}
        .metrics-grid {{
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            margin-bottom: 40px;
        }}
        .section {{
            background: white;
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 24px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .section h2 {{
            font-size: 20px;
            font-weight: 600;
            color: {COLORS['text']};
            margin-bottom: 20px;
            padding-bottom: 12px;
            border-bottom: 2px solid #e2e8f0;
        }}
        .chart-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 24px;
        }}
        .chart-container {{
            background: white;
            border-radius: 16px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .insights-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
        }}
        .footer {{
            text-align: center;
            color: #94a3b8;
            font-size: 14px;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #e2e8f0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>MATS Advertising Analysis</h1>
            <div class="subtitle">Executive Summary | Generated {datetime.now().strftime('%B %d, %Y')}</div>
        </div>

        <!-- Key Metrics -->
        <div class="metrics-grid">
            {create_metric_card("Total Visitors", f"{total_visitors:,}", "Unique website visitors", COLORS['primary'])}
            {create_metric_card("Applications", f"{totals['count']:,}", f"{total_apply_views:,} apply page views", COLORS['primary'])}
            {create_metric_card("Stage 2 Advanced", f"{totals['advanced']:,}", f"{totals['advancement_rate']:.1f}% advancement rate", COLORS['success'])}
            {create_metric_card("Top Quality Source", best_source_name, f"{best_source_rate:.1f}% rate ({best_source_count} apps)", COLORS['success'])}
        </div>

        <!-- Funnel & Outcomes -->
        <div class="chart-grid">
            <div class="chart-container">
                <div id="funnel-chart"></div>
            </div>
            <div class="chart-container">
                <div id="outcomes-chart"></div>
            </div>
        </div>

        <!-- Volume & Quality -->
        <div class="chart-grid" style="margin-top: 24px;">
            <div class="chart-container">
                <div id="volume-chart"></div>
            </div>
            <div class="chart-container">
                <div id="quality-chart"></div>
            </div>
        </div>

        <!-- Growth Over Time -->
        <div class="chart-container" style="margin-top: 24px;">
            <div id="growth-chart"></div>
        </div>

        <!-- Key Insights -->
        <div class="section" style="margin-top: 24px;">
            <h2>Key Insights</h2>
            <div class="insights-grid">
                {create_insight_box(
                    "Top Volume Sources",
                    f"<strong>LinkedIn</strong> leads with {int(top_by_volume.iloc[0]['Count'])} applications ({top_by_volume.iloc[0]['Count']/totals['count']*100:.1f}% of total). "
                    f"<strong>Personal recommendations</strong> and <strong>X (Twitter)</strong> are #2 and #3.",
                    "📊"
                )}
                {create_insight_box(
                    "Highest Quality Sources",
                    f"Among sources with 20+ applications, <strong>{top_by_quality.iloc[0]['Source']}</strong> has the highest advancement rate at "
                    f"{top_by_quality.iloc[0]['Stage 2 advanced']/top_by_quality.iloc[0]['Count']*100:.1f}%. "
                    f"<strong>LessWrong</strong> and <strong>AI safety/EA student groups</strong> also show excellent quality.",
                    "⭐"
                )}
                {create_insight_box(
                    "Best ROI Sources",
                    f"Sources with both high volume AND high quality: <strong>Personal recommendations</strong> (78.7% rate, 362 apps), "
                    f"<strong>80,000 Hours</strong> (77.6% rate, 294 apps), and <strong>AI safety/EA student groups</strong> (80.9% rate, 272 apps).",
                    "💰"
                )}
                {create_insight_box(
                    "Overall Funnel Health",
                    f"Of {total_visitors:,} unique visitors, {totals['count']:,} applied ({totals['count']/total_visitors*100:.2f}% conversion). "
                    f"Stage 2 advancement rate is <strong>{totals['advancement_rate']:.1f}%</strong>, with {totals['pending']} applications still pending.",
                    "📈"
                )}
            </div>
        </div>

        <div class="footer">
            Generated by MATS Advertising Analysis Tool | Data as of {datetime.now().strftime('%Y-%m-%d')}
        </div>
    </div>

    <script>
        Plotly.newPlot('funnel-chart', {funnel_fig.to_json()}.data, {funnel_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('outcomes-chart', {outcomes_fig.to_json()}.data, {outcomes_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('volume-chart', {volume_fig.to_json()}.data, {volume_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('quality-chart', {quality_fig.to_json()}.data, {quality_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('growth-chart', {growth_fig.to_json()}.data, {growth_fig.to_json()}.layout, {{responsive: true}});
    </script>
</body>
</html>
'''

    # Write to file
    output_path = REPORTS_DIR / "executive_summary.html"
    with open(output_path, 'w') as f:
        f.write(html_content)

    print(f"Executive summary generated: {output_path}")
    return output_path


def generate_detailed_report(data):
    """Generate the detailed analysis HTML report."""

    posthog = data['posthog']
    posthog_daily = data['posthog_daily']
    referral = data['referral']
    referral_daily = data['referral_daily']

    # Calculate corrected totals
    totals = calculate_correct_totals(referral)

    # Get daily traffic totals
    daily_traffic = get_total_daily_traffic(posthog_daily)

    # Get daily new applications
    daily_apps = get_daily_new_applications(referral_daily)

    # ===== SECTION 1: TRAFFIC OVERVIEW =====

    # 1.1 Daily Traffic Trend
    traffic_trend_fig = go.Figure()
    traffic_trend_fig.add_trace(go.Scatter(
        x=daily_traffic['Date'],
        y=daily_traffic['Unique visitors'],
        mode='lines',
        name='Unique Visitors',
        line=dict(color=COLORS['primary'], width=2)
    ))
    traffic_trend_fig.add_trace(go.Scatter(
        x=daily_traffic['Date'],
        y=daily_traffic['Apply page views'],
        mode='lines',
        name='Apply Page Views',
        line=dict(color=COLORS['success'], width=2)
    ))
    traffic_trend_fig.update_layout(
        title="Daily Traffic Trend",
        height=400,
        template=TEMPLATE,
        hovermode='x unified',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )

    # 1.2 Traffic by Source Over Time (Top 10 handles)
    top_handles = get_top_handles(posthog, n=10, metric='Events')['Handle'].tolist()
    top_handles_daily = posthog_daily[posthog_daily['Handle'].isin(top_handles)]

    source_traffic_fig = px.area(
        top_handles_daily,
        x='Date',
        y='Events',
        color='Handle',
        title="Daily Traffic by Top 10 Sources"
    )
    source_traffic_fig.update_layout(height=450, template=TEMPLATE)

    # 1.3 Handle Performance Comparison
    top_20_handles = get_top_handles(posthog, n=20, metric='Events')

    handle_comparison_fig = go.Figure()
    metrics = ['Events', 'Unique visitors', 'Apply page views', 'Program page views']
    colors = [COLORS['primary'], '#3b82f6', COLORS['success'], COLORS['warning']]

    for i, metric in enumerate(metrics):
        handle_comparison_fig.add_trace(go.Bar(
            name=metric,
            x=top_20_handles['Handle'],
            y=top_20_handles[metric],
            marker_color=colors[i]
        ))

    handle_comparison_fig.update_layout(
        title="Top 20 Traffic Sources - Metric Comparison",
        barmode='group',
        height=500,
        template=TEMPLATE,
        xaxis_tickangle=-45
    )

    # ===== SECTION 2: APPLICATION FUNNEL =====

    # 2.1 Applications Over Time
    all_daily = referral_daily[referral_daily['Source'] == '(all)'].copy().sort_values('Date')

    apps_time_fig = make_subplots(specs=[[{"secondary_y": True}]])
    apps_time_fig.add_trace(
        go.Scatter(x=all_daily['Date'], y=all_daily['Cumulative count'],
                   mode='lines', name='Cumulative Applications',
                   line=dict(color=COLORS['primary'], width=3)),
        secondary_y=False
    )

    # Calculate daily new applications
    all_daily['New Apps'] = all_daily['Cumulative count'].diff().fillna(all_daily['Cumulative count'])
    apps_time_fig.add_trace(
        go.Bar(x=all_daily['Date'], y=all_daily['New Apps'],
               name='New Applications', marker_color='rgba(37, 99, 235, 0.4)'),
        secondary_y=True
    )
    apps_time_fig.update_layout(
        title="Applications Over Time",
        height=400,
        template=TEMPLATE,
        legend=dict(orientation='h', yanchor='bottom', y=1.02)
    )
    apps_time_fig.update_yaxes(title_text="Cumulative", secondary_y=False)
    apps_time_fig.update_yaxes(title_text="Daily New", secondary_y=True)

    # 2.2 Applications by Source Treemap
    referral_filtered = referral[~referral['Source'].isin(['(all)', '(no response)'])].copy()
    referral_filtered['Advancement Rate'] = referral_filtered['Stage 2 advanced'] / referral_filtered['Count'] * 100

    treemap_fig = px.treemap(
        referral_filtered,
        path=['Source'],
        values='Count',
        color='Advancement Rate',
        color_continuous_scale='RdYlGn',
        title="Applications by Source (size=count, color=advancement rate)"
    )
    treemap_fig.update_layout(height=500, template=TEMPLATE)

    # 2.3 Stage 2 Outcomes by Source
    top_sources = get_top_sources(referral, n=15, metric='Count')

    outcomes_by_source_fig = go.Figure()
    outcomes_by_source_fig.add_trace(go.Bar(
        name='Advanced',
        x=top_sources['Source'],
        y=top_sources['Stage 2 advanced'],
        marker_color=COLORS['advanced']
    ))
    outcomes_by_source_fig.add_trace(go.Bar(
        name='Rejected',
        x=top_sources['Source'],
        y=top_sources['Stage 2 rejected'],
        marker_color=COLORS['rejected']
    ))
    outcomes_by_source_fig.add_trace(go.Bar(
        name='Pending',
        x=top_sources['Source'],
        y=top_sources['Stage 2 pending'],
        marker_color=COLORS['pending']
    ))
    outcomes_by_source_fig.update_layout(
        title="Stage 2 Outcomes by Source (Top 15)",
        barmode='stack',
        height=450,
        template=TEMPLATE,
        xaxis_tickangle=-45
    )

    # 2.4 Advancement Rate Distribution
    rate_dist_fig = px.histogram(
        referral_filtered,
        x='Advancement Rate',
        nbins=20,
        title="Distribution of Advancement Rates Across Sources"
    )
    rate_dist_fig.update_layout(height=350, template=TEMPLATE)
    rate_dist_fig.add_vline(x=totals['advancement_rate'], line_dash="dash",
                           annotation_text=f"Overall: {totals['advancement_rate']:.1f}%")

    # ===== SECTION 3: CONVERSION ANALYSIS =====

    # 3.1 Quality vs Volume Scatter
    scatter_fig = px.scatter(
        referral_filtered,
        x='Count',
        y='Advancement Rate',
        size='Stage 2 advanced',
        color='Advancement Rate',
        color_continuous_scale='RdYlGn',
        hover_name='Source',
        title="Quality vs Volume Analysis",
        labels={'Count': 'Applications', 'Advancement Rate': 'Advancement Rate (%)'}
    )
    scatter_fig.update_layout(height=500, template=TEMPLATE)

    # Add quadrant lines
    median_count = referral_filtered['Count'].median()
    median_rate = referral_filtered['Advancement Rate'].median()
    scatter_fig.add_hline(y=median_rate, line_dash="dot", line_color="gray")
    scatter_fig.add_vline(x=median_count, line_dash="dot", line_color="gray")

    # ===== SECTION 4: TIME TRENDS =====

    # 4.1 Day of Week Pattern
    daily_traffic['DayOfWeek'] = daily_traffic['Date'].dt.day_name()
    dow_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dow_traffic = daily_traffic.groupby('DayOfWeek').agg({
        'Unique visitors': 'mean',
        'Apply page views': 'mean'
    }).reindex(dow_order)

    dow_fig = go.Figure()
    dow_fig.add_trace(go.Bar(
        x=dow_order,
        y=dow_traffic['Unique visitors'],
        name='Avg Visitors',
        marker_color=COLORS['primary']
    ))
    dow_fig.add_trace(go.Bar(
        x=dow_order,
        y=dow_traffic['Apply page views'],
        name='Avg Apply Views',
        marker_color=COLORS['success']
    ))
    dow_fig.update_layout(
        title="Average Traffic by Day of Week",
        barmode='group',
        height=350,
        template=TEMPLATE
    )

    # 4.2 Weekly Application Growth
    all_daily['Week'] = all_daily['Date'].dt.isocalendar().week
    weekly_apps = all_daily.groupby('Week').agg({
        'New Apps': 'sum',
        'Date': 'first'
    }).reset_index()

    weekly_fig = go.Figure(go.Bar(
        x=weekly_apps['Date'],
        y=weekly_apps['New Apps'],
        marker_color=COLORS['primary'],
        text=weekly_apps['New Apps'].astype(int),
        textposition='outside'
    ))
    weekly_fig.update_layout(
        title="Weekly New Applications",
        height=350,
        template=TEMPLATE,
        xaxis_title="Week Starting"
    )

    # ===== SECTION 5: SOURCE DEEP DIVES =====

    # 5.1 Source Comparison Heatmap
    # Normalize metrics for comparison
    comparison_df = referral_filtered[['Source', 'Count', 'Stage 2 advanced', 'Advancement Rate']].copy()
    comparison_df = comparison_df.nlargest(15, 'Count')

    # Normalize to 0-100 scale
    for col in ['Count', 'Stage 2 advanced', 'Advancement Rate']:
        comparison_df[f'{col}_norm'] = (comparison_df[col] - comparison_df[col].min()) / (comparison_df[col].max() - comparison_df[col].min()) * 100

    heatmap_fig = go.Figure(go.Heatmap(
        z=[comparison_df['Count_norm'], comparison_df['Stage 2 advanced_norm'], comparison_df['Advancement Rate_norm']],
        x=comparison_df['Source'],
        y=['Volume', 'Advanced', 'Rate'],
        colorscale='Blues',
        showscale=True
    ))
    heatmap_fig.update_layout(
        title="Source Comparison Matrix (Normalized)",
        height=300,
        template=TEMPLATE,
        xaxis_tickangle=-45
    )

    # ===== BUILD HTML =====

    html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MATS Advertising Analysis - Detailed Report</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f8fafc;
            color: {COLORS['text']};
            line-height: 1.6;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 40px 20px; }}
        .header {{ text-align: center; margin-bottom: 40px; }}
        .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 8px; }}
        .header .subtitle {{ color: #64748b; font-size: 16px; }}
        .section {{
            background: white;
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 32px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .section h2 {{
            font-size: 24px;
            font-weight: 600;
            color: {COLORS['text']};
            margin-bottom: 24px;
            padding-bottom: 12px;
            border-bottom: 2px solid {COLORS['primary']};
        }}
        .section h3 {{
            font-size: 18px;
            font-weight: 500;
            color: #475569;
            margin: 24px 0 16px 0;
        }}
        .chart-container {{ margin-bottom: 32px; }}
        .chart-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 24px;
        }}
        .nav {{
            position: sticky;
            top: 0;
            background: white;
            padding: 12px 20px;
            border-bottom: 1px solid #e2e8f0;
            z-index: 100;
            margin-bottom: 20px;
        }}
        .nav a {{
            color: {COLORS['primary']};
            text-decoration: none;
            margin-right: 20px;
            font-weight: 500;
        }}
        .nav a:hover {{ text-decoration: underline; }}
        .data-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 16px;
        }}
        .data-table th, .data-table td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e2e8f0;
        }}
        .data-table th {{
            background: #f8fafc;
            font-weight: 600;
            color: #475569;
        }}
        .data-table tr:hover {{ background: #f8fafc; }}
        .footer {{
            text-align: center;
            color: #94a3b8;
            font-size: 14px;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #e2e8f0;
        }}
    </style>
</head>
<body>
    <nav class="nav">
        <a href="#traffic">Traffic Overview</a>
        <a href="#funnel">Application Funnel</a>
        <a href="#conversion">Conversion Analysis</a>
        <a href="#trends">Time Trends</a>
        <a href="#sources">Source Deep Dives</a>
    </nav>

    <div class="container">
        <div class="header">
            <h1>MATS Advertising Analysis</h1>
            <div class="subtitle">Detailed Report | Generated {datetime.now().strftime('%B %d, %Y')}</div>
        </div>

        <!-- Section 1: Traffic Overview -->
        <div class="section" id="traffic">
            <h2>1. Traffic Overview</h2>

            <h3>1.1 Daily Traffic Trend</h3>
            <div class="chart-container">
                <div id="traffic-trend-chart"></div>
            </div>

            <h3>1.2 Traffic by Source Over Time</h3>
            <div class="chart-container">
                <div id="source-traffic-chart"></div>
            </div>

            <h3>1.3 Handle Performance Comparison</h3>
            <div class="chart-container">
                <div id="handle-comparison-chart"></div>
            </div>
        </div>

        <!-- Section 2: Application Funnel -->
        <div class="section" id="funnel">
            <h2>2. Application Funnel</h2>

            <h3>2.1 Applications Over Time</h3>
            <div class="chart-container">
                <div id="apps-time-chart"></div>
            </div>

            <h3>2.2 Applications by Source</h3>
            <div class="chart-container">
                <div id="treemap-chart"></div>
            </div>

            <h3>2.3 Stage 2 Outcomes by Source</h3>
            <div class="chart-container">
                <div id="outcomes-source-chart"></div>
            </div>

            <h3>2.4 Advancement Rate Distribution</h3>
            <div class="chart-container">
                <div id="rate-dist-chart"></div>
            </div>
        </div>

        <!-- Section 3: Conversion Analysis -->
        <div class="section" id="conversion">
            <h2>3. Conversion Analysis</h2>

            <h3>3.1 Quality vs Volume</h3>
            <p style="color: #64748b; margin-bottom: 16px;">
                Quadrants: Upper-right = high volume + high quality (best). Size indicates number of Stage 2 advances.
            </p>
            <div class="chart-container">
                <div id="scatter-chart"></div>
            </div>
        </div>

        <!-- Section 4: Time Trends -->
        <div class="section" id="trends">
            <h2>4. Time Trends</h2>

            <div class="chart-grid">
                <div>
                    <h3>4.1 Traffic by Day of Week</h3>
                    <div class="chart-container">
                        <div id="dow-chart"></div>
                    </div>
                </div>
                <div>
                    <h3>4.2 Weekly Application Volume</h3>
                    <div class="chart-container">
                        <div id="weekly-chart"></div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Section 5: Source Deep Dives -->
        <div class="section" id="sources">
            <h2>5. Source Deep Dives</h2>

            <h3>5.1 Source Comparison Matrix</h3>
            <div class="chart-container">
                <div id="heatmap-chart"></div>
            </div>

            <h3>5.2 All Sources Data Table</h3>
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Source</th>
                        <th>Applications</th>
                        <th>Advanced</th>
                        <th>Rejected</th>
                        <th>Pending</th>
                        <th>Advancement Rate</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(f"""
                    <tr>
                        <td>{row['Source']}</td>
                        <td>{int(row['Count'])}</td>
                        <td>{int(row['Stage 2 advanced'])}</td>
                        <td>{int(row['Stage 2 rejected'])}</td>
                        <td>{int(row['Stage 2 pending'])}</td>
                        <td>{row['Stage 2 advanced']/row['Count']*100:.1f}%</td>
                    </tr>
                    """ for _, row in referral_filtered.sort_values('Count', ascending=False).iterrows())}
                </tbody>
            </table>
        </div>

        <div class="footer">
            Generated by MATS Advertising Analysis Tool | Data as of {datetime.now().strftime('%Y-%m-%d')}
        </div>
    </div>

    <script>
        Plotly.newPlot('traffic-trend-chart', {traffic_trend_fig.to_json()}.data, {traffic_trend_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('source-traffic-chart', {source_traffic_fig.to_json()}.data, {source_traffic_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('handle-comparison-chart', {handle_comparison_fig.to_json()}.data, {handle_comparison_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('apps-time-chart', {apps_time_fig.to_json()}.data, {apps_time_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('treemap-chart', {treemap_fig.to_json()}.data, {treemap_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('outcomes-source-chart', {outcomes_by_source_fig.to_json()}.data, {outcomes_by_source_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('rate-dist-chart', {rate_dist_fig.to_json()}.data, {rate_dist_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('scatter-chart', {scatter_fig.to_json()}.data, {scatter_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('dow-chart', {dow_fig.to_json()}.data, {dow_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('weekly-chart', {weekly_fig.to_json()}.data, {weekly_fig.to_json()}.layout, {{responsive: true}});
        Plotly.newPlot('heatmap-chart', {heatmap_fig.to_json()}.data, {heatmap_fig.to_json()}.layout, {{responsive: true}});
    </script>
</body>
</html>
'''

    # Write to file
    output_path = REPORTS_DIR / "detailed_analysis.html"
    with open(output_path, 'w') as f:
        f.write(html_content)

    print(f"Detailed analysis generated: {output_path}")
    return output_path


def main():
    """Generate all reports."""
    print("=" * 60)
    print("MATS Advertising Analysis Report Generator")
    print("=" * 60)

    # Ensure output directory exists
    REPORTS_DIR.mkdir(exist_ok=True)

    # Load all data
    print("\nLoading data...")
    data = load_all_data()
    print(f"  PostHog: {len(data['posthog'])} sources, {len(data['posthog_daily'])} daily records")
    print(f"  Referral: {len(data['referral'])} sources, {len(data['referral_daily'])} daily records")

    # Generate reports
    print("\nGenerating reports...")

    exec_path = generate_executive_summary(data)
    detailed_path = generate_detailed_report(data)

    print("\n" + "=" * 60)
    print("Reports generated successfully!")
    print(f"  Executive Summary: {exec_path}")
    print(f"  Detailed Analysis: {detailed_path}")
    print("\nOpen in browser to view.")


if __name__ == '__main__':
    main()
