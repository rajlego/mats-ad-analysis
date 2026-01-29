// @ts-nocheck
/**
 * PostHog Daily Metrics Script
 *
 * Fetches daily metrics from PostHog and upserts into PostHog daily table.
 * Run once per day to capture that day's data.
 *
 * Input variables (set in Automation):
 *   - posthog_api_key: Your PostHog personal API key (not project API key)
 *   - posthog_project_id: Your PostHog project ID
 *   - posthog_daily_table: Name of the daily table (e.g., "PostHog daily")
 *   - round_start: Start date for this round (M/D/YY or M/D/YYYY)
 *   - round_end: End date for this round (M/D/YY or M/D/YYYY)
 */

const inputConfig = input.config();

// Required inputs
const POSTHOG_API_KEY = inputConfig.posthog_api_key;
const POSTHOG_PROJECT_ID = inputConfig.posthog_project_id;
const OUTPUT_TABLE = inputConfig.posthog_daily_table;
const ROUND_START = inputConfig.round_start;
const ROUND_END = inputConfig.round_end;

const POSTHOG_HOST = 'https://app.posthog.com';

// Special handles
const DIRECT_HANDLE = '(direct)';

// URL patterns
const APPLY_PAGE_PATTERN = '%/apply%';
const PROGRAM_PAGE_PATTERN = '%/program/%';

// ============ DATE UTILITIES ============

function toISODate(dateStr) {
    const [month, day, year] = dateStr.split('/');
    let fullYear;
    if (year.length === 4) {
        fullYear = year;
    } else {
        fullYear = parseInt(year) < 50 ? `20${year.padStart(2, '0')}` : `19${year.padStart(2, '0')}`;
    }
    return `${fullYear}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
}

// ============ VALIDATION ============

function validateInputs() {
    const missing = [];
    if (!POSTHOG_API_KEY) missing.push('posthog_api_key');
    if (!POSTHOG_PROJECT_ID) missing.push('posthog_project_id');
    if (!OUTPUT_TABLE) missing.push('posthog_daily_table');
    if (!ROUND_START) missing.push('round_start');
    if (!ROUND_END) missing.push('round_end');

    if (missing.length > 0) {
        throw new Error(`Missing required input variables: ${missing.join(', ')}`);
    }

    if (!/^\d{1,2}\/\d{1,2}\/(\d{2}|\d{4})$/.test(ROUND_START)) {
        throw new Error(`round_start must be M/D/YY or M/D/YYYY format, got: ${ROUND_START}`);
    }
    if (!/^\d{1,2}\/\d{1,2}\/(\d{2}|\d{4})$/.test(ROUND_END)) {
        throw new Error(`round_end must be M/D/YY or M/D/YYYY format, got: ${ROUND_END}`);
    }
}

// ============ POSTHOG API ============

async function runHogQLQuery(query) {
    const url = `${POSTHOG_HOST}/api/projects/${POSTHOG_PROJECT_ID}/query`;

    const response = await fetch(url, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${POSTHOG_API_KEY}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            query: {
                kind: 'HogQLQuery',
                query: query
            }
        })
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`PostHog HogQL error: ${response.status} ${errorText}`);
    }

    const data = await response.json();
    return data.results || [];
}

async function fetchDailyMetrics() {
    const startISO = toISODate(ROUND_START);
    const endISO = toISODate(ROUND_END);

    const query = `
        SELECT
            toDate(timestamp) as event_date,
            lower(coalesce(nullIf(trim(properties.utm_source), ''), '${DIRECT_HANDLE}')) as handle,
            count() as events,
            countIf(event = '$pageview') as pageviews,
            count(DISTINCT distinct_id) as unique_visitors,
            countIf(event = '$pageview' AND properties.$current_url LIKE '${APPLY_PAGE_PATTERN}') as apply_page_views,
            countIf(event = '$pageview' AND properties.$current_url LIKE '${PROGRAM_PAGE_PATTERN}') as program_page_views
        FROM events
        WHERE timestamp >= toDateTime('${startISO} 00:00:00')
          AND timestamp <= toDateTime('${endISO} 23:59:59')
        GROUP BY event_date, handle
        ORDER BY event_date DESC, events DESC
    `;

    console.log('Running HogQL query for daily metrics...');
    const results = await runHogQLQuery(query);

    const metrics = [];
    for (const row of results) {
        const [eventDate, handle, events, pageviews, uniqueVisitors, applyViews, programViews] = row;

        if (!handle || !eventDate) continue;

        metrics.push({
            date: eventDate.slice(0, 10), // YYYY-MM-DD
            handle: handle,
            events: events,
            pageviews: pageviews,
            uniqueVisitors: uniqueVisitors,
            applyPageViews: applyViews,
            programPageViews: programViews
        });
    }

    return metrics;
}

// ============ AIRTABLE UPSERT ============

function computeKey(handle, date) {
    return `${handle}-${date}`;
}

async function upsertDailyData(metrics) {
    const table = base.getTable(OUTPUT_TABLE);

    console.log(`Fetching existing records from "${OUTPUT_TABLE}"...`);
    const query = await table.selectRecordsAsync({
        fields: ['Key', 'Handle', 'Date']
    });

    // Build key -> record mapping
    const keyToRecord = {};
    for (const record of query.records) {
        const key = record.getCellValueAsString('Key');
        if (key) {
            keyToRecord[key] = record;
        }
    }

    console.log(`Found ${Object.keys(keyToRecord).length} existing records`);

    const updates = [];
    const creates = [];

    for (const m of metrics) {
        const key = computeKey(m.handle, m.date);
        const existingRecord = keyToRecord[key];

        const fields = {
            'Handle': m.handle,
            'Date': m.date,
            'Events': m.events,
            'Pageviews': m.pageviews,
            'Unique visitors': m.uniqueVisitors,
            'Apply page views': m.applyPageViews,
            'Program page views': m.programPageViews
        };

        if (existingRecord) {
            updates.push({ id: existingRecord.id, fields });
        } else {
            creates.push({ fields });
        }
    }

    console.log(`\nTo update: ${updates.length}`);
    console.log(`To create: ${creates.length}`);

    // Perform updates in batches of 50
    if (updates.length > 0) {
        console.log(`\nUpdating ${updates.length} records...`);
        for (let i = 0; i < updates.length; i += 50) {
            const batch = updates.slice(i, i + 50);
            await table.updateRecordsAsync(batch);
        }
    }

    // Perform creates in batches of 50
    if (creates.length > 0) {
        console.log(`\nCreating ${creates.length} records...`);
        for (let i = 0; i < creates.length; i += 50) {
            const batch = creates.slice(i, i + 50);
            await table.createRecordsAsync(batch);
        }
    }

    return { updated: updates.length, created: creates.length };
}

// ============ MAIN ============

console.log('PostHog Daily Metrics');
console.log('='.repeat(50));

validateInputs();

console.log(`\nConfiguration:`);
console.log(`  Table: ${OUTPUT_TABLE}`);
console.log(`  Range: ${ROUND_START} to ${ROUND_END}`);

console.log('\n1. Fetching daily metrics from PostHog...');
const metrics = await fetchDailyMetrics();

// Count unique dates and handles
const uniqueDates = [...new Set(metrics.map(m => m.date))];
const uniqueHandles = [...new Set(metrics.map(m => m.handle))];
console.log(`   Found ${metrics.length} rows (${uniqueDates.length} days, ${uniqueHandles.length} handles)`);

if (metrics.length === 0) {
    console.log('\nNo data found for this date range.');
} else {
    console.log('\n2. Upserting into Airtable...');
    const result = await upsertDailyData(metrics);

    console.log('\n' + '='.repeat(50));
    console.log(`Done! Updated ${result.updated}, created ${result.created} records.`);
}
