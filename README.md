# MATS ad analysis

Syncs PostHog analytics to Airtable, creating aggregated records per handle (utm_source) for each application round.

## Architecture

```
┌─────────────┐     ┌────────────────────┐     ┌─────────────────────────┐
│  PostHog    │────▶│ Airtable Automation│────▶│      Airtable           │
│  (HogQL)    │     │     Script         │     │                         │
└─────────────┘     └────────────────────┘     │  ┌───────────────────┐  │
                                               │  │  PostHog data     │  │
                                               │  │  (aggregated)     │  │
                                               │  └─────────┬─────────┘  │
                                               │            │ links      │
                                               │  ┌─────────▼─────────┐  │
                                               │  │  Handles tables   │  │
                                               │  │  (10.0 targets,   │  │
                                               │  │   contacts, etc)  │  │
                                               │  └───────────────────┘  │
                                               └─────────────────────────┘
```

## What it does

1. Runs a HogQL query against PostHog for a date range
2. Aggregates metrics per handle (utm_source)
3. Creates a `(direct)` handle for traffic without utm_source
4. Creates an `(all)` handle with total aggregation (sanity check)
5. Upserts records into the PostHog data table (updates existing, creates new)
6. Handles tables can link to these records via a separate automation

## PostHog data table structure

| Field | Type | Description |
|-------|------|-------------|
| Key | Formula | `{Handle}-{Round start M/D/YY}-{Round end M/D/YY}` (primary key) |
| Handle | Text | The utm_source value, or `(direct)` for no utm_source, or `(all)` for total |
| Round start | Date | Start of date range (from input) |
| Round end | Date | End of date range (from input) |
| Events | Number | Total events |
| Pageviews | Number | Total $pageview events |
| Unique visitors | Number | Distinct users |
| Apply page views | Number | `/apply` page visits |
| Program page views | Number | `/program/` page visits |
| First active | Date | First observed event in range |
| Last active | Date | Last observed event in range |
| Campaigns | Long text | Comma-separated utm_campaign values |

## Setup

### 1. Create the PostHog data table

1. Create a new table called **PostHog data**
2. Add these fields manually:

| Field Name | Type |
|------------|------|
| Handle | Single line text |
| Round start | Date |
| Round end | Date |
| Events | Number (integer) |
| Pageviews | Number (integer) |
| Unique visitors | Number (integer) |
| Apply page views | Number (integer) |
| Program page views | Number (integer) |
| First active | Date |
| Last active | Date |
| Campaigns | Long text |
| Key | Formula (see below) |

3. For the **Key** formula field, use:
```
IF(AND({Handle}, {Round start}, {Round end}), {Handle} & "-" & DATETIME_FORMAT({Round start}, 'M/D/YY') & "-" & DATETIME_FORMAT({Round end}, 'M/D/YY'), "")
```

### 2. Get your PostHog credentials

- **API key**: PostHog → Project settings → Project API key
- **Project ID**: Number from your PostHog URL (e.g., `app.posthog.com/project/12345` → `12345`)

### 3. Create the sync automation

1. Go to **Automations** → **Create automation**
2. Trigger: **At scheduled time** (e.g., daily)
3. Action: **Run script**
4. Paste the contents of `airtable-script.js`

### 4. Configure input variables

In the script action's left panel, add these input variables:

| Variable | Required | Example |
|----------|----------|---------|
| `posthog_api_key` | Yes | `phc_xxx...` |
| `posthog_project_id` | Yes | `12345` |
| `posthog_data_table` | Yes | `PostHog data` |
| `round_start` | Yes | `1/1/25` (M/D/YY format) |
| `round_end` | Yes | `3/15/25` (M/D/YY format) |

### 5. Test and enable

1. Click **Test** to run once
2. Check output for errors
3. Verify PostHog data table has records
4. Toggle automation **On**

### 6. Set up linking (optional)

Use Airtable's native automation features to link records:
1. Create a new automation
2. Trigger: When record matches conditions (in PostHog data)
3. Action: Update record (in your handles table)
4. Link based on matching Handle field

## How upsert works

The script uses a formula-based key (`Handle-RoundStart-RoundEnd`) to identify records:

- If a record with matching Key exists → **update** it
- If no matching record exists → **create** new one

This means you can re-run the script and it will update existing records rather than creating duplicates.

## For each new round

1. Update the automation's `round_start` and `round_end` input variables
2. Run the automation
3. New records will be created for the new date range
4. Old records (different date range) remain untouched

## Files

| File | Purpose |
|------|---------|
| `airtable-script.js` | Main sync script (paste into Airtable Automation) |
| `create-table-script.js` | One-time setup script to create table fields |
| `README.md` | This file |

## Troubleshooting

**"PostHog HogQL error: 401"**
- Check your API key is correct and not expired
- Make sure it's a Project API key, not Personal API key

**"Cannot find table"**
- Verify `posthog_data_table` matches your table name exactly (case-sensitive)

**Fields not updating**
- Field names are case-sensitive
- Run `create-table-script.js` to ensure all fields exist with correct names

**No data found**
- Verify your PostHog project has events with `$utm_source` property
- Check the date range is correct
