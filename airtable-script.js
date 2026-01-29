// @ts-nocheck
/**
 * PostHog → Airtable sync script (HogQL version)
 *
 * Fetches metrics from PostHog and upserts into PostHog data table.
 * Uses HogQL for efficient server-side aggregation.
 *
 * Input variables (set in Automation):
 *   - posthog_api_key: Your PostHog project API key
 *   - posthog_project_id: Your PostHog project ID
 *   - posthog_data_table: Name of the PostHog data table (e.g., "PostHog data")
 *   - round_start: Start date for this round (YYYY-MM-DD)
 *   - round_end: End date for this round (YYYY-MM-DD)
 */

const inputConfig = input.config();

// Required inputs
const POSTHOG_API_KEY = inputConfig.posthog_api_key;
const POSTHOG_PROJECT_ID = inputConfig.posthog_project_id;
const POSTHOG_DATA_TABLE = inputConfig.posthog_data_table;
const ROUND_START = inputConfig.round_start;
const ROUND_END = inputConfig.round_end;

const POSTHOG_HOST = 'https://app.posthog.com';

// URL patterns to track
const APPLY_PAGE_PATTERN = '%/apply%';
const PROGRAM_PAGE_PATTERN = '%/program/%';

// ============ VALIDATION ============

function validateInputs() {
    const missing = [];
    if (!POSTHOG_API_KEY) missing.push('posthog_api_key');
    if (!POSTHOG_PROJECT_ID) missing.push('posthog_project_id');
    if (!POSTHOG_DATA_TABLE) missing.push('posthog_data_table');
    if (!ROUND_START) missing.push('round_start');
    if (!ROUND_END) missing.push('round_end');

    if (missing.length > 0) {
        throw new Error(`Missing required input variables: ${missing.join(', ')}`);
    }

    if (!/^\d{4}-\d{2}-\d{2}$/.test(ROUND_START)) {
        throw new Error(`round_start must be YYYY-MM-DD format, got: ${ROUND_START}`);
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(ROUND_END)) {
        throw new Error(`round_end must be YYYY-MM-DD format, got: ${ROUND_END}`);
    }
}

// ============ POSTHOG HOGQL API ============

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

async function fetchMetrics() {
    const dateFilter = `timestamp >= toDateTime('${ROUND_START}T00:00:00Z') AND timestamp <= toDateTime('${ROUND_END}T23:59:59Z')`;

    const query = `
        SELECT
            lower(trim(properties.$utm_source)) as handle,
            count() as visits,
            count(DISTINCT distinct_id) as unique_visitors,
            countIf(properties.$current_url LIKE '${APPLY_PAGE_PATTERN}') as apply_page_views,
            countIf(properties.$current_url LIKE '${PROGRAM_PAGE_PATTERN}') as program_page_views,
            min(timestamp) as first_active,
            max(timestamp) as last_active,
            groupUniqArray(properties.$utm_campaign) as campaigns
        FROM events
        WHERE properties.$utm_source IS NOT NULL
            AND properties.$utm_source != ''
            AND ${dateFilter}
        GROUP BY handle
        ORDER BY visits DESC
    `;

    console.log('Running HogQL query...');
    const results = await runHogQLQuery(query);

    // Convert to array of metrics objects
    const metrics = [];
    for (const row of results) {
        const [handle, visits, uniqueVisitors, applyViews, programViews, firstActive, lastActive, campaigns] = row;

        if (!handle) continue;

        // Filter out null/empty campaigns and join
        const campaignList = (campaigns || []).filter(c => c && c.trim());

        metrics.push({
            handle: handle,
            visits: visits,
            uniqueVisitors: uniqueVisitors,
            applyPageViews: applyViews,
            programPageViews: programViews,
            firstActive: firstActive ? firstActive.slice(0, 10) : null,
            lastActive: lastActive ? lastActive.slice(0, 10) : null,
            campaigns: campaignList.length > 0 ? campaignList.sort().join(', ') : null
        });
    }

    return metrics;
}

// ============ AIRTABLE UPSERT ============

function computeKey(handle) {
    return `${handle}-${ROUND_START}-${ROUND_END}`;
}

async function upsertPostHogData(metrics) {
    const table = base.getTable(POSTHOG_DATA_TABLE);

    // Fetch existing records to find matches by Key
    console.log(`Fetching existing records from "${POSTHOG_DATA_TABLE}"...`);
    const query = await table.selectRecordsAsync({
        fields: ['Key', 'Handle']
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

    // Prepare updates and creates
    const updates = [];
    const creates = [];

    for (const m of metrics) {
        const key = computeKey(m.handle);
        const existingRecord = keyToRecord[key];

        const fields = {
            'Handle': m.handle,
            'Round start': m.firstActive ? m.firstActive : ROUND_START, // Use first active or round start
            'Round end': m.lastActive ? m.lastActive : ROUND_END, // Use last active or round end
            'Visits': m.visits,
            'Unique visitors': m.uniqueVisitors,
            'Apply page views': m.applyPageViews,
            'Program page views': m.programPageViews,
            'First active': m.firstActive,
            'Last active': m.lastActive,
            'Campaigns': m.campaigns,
        };

        // Remove null values
        for (const k of Object.keys(fields)) {
            if (fields[k] === null || fields[k] === undefined) {
                delete fields[k];
            }
        }

        if (existingRecord) {
            updates.push({ id: existingRecord.id, fields });
        } else {
            // For creates, we need to set the date fields to actual dates for the Key formula to work
            fields['Round start'] = ROUND_START;
            fields['Round end'] = ROUND_END;
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
            if ((i + 50) % 200 === 0 || i + 50 >= updates.length) {
                console.log(`  Updated ${Math.min(i + 50, updates.length)}/${updates.length}`);
            }
        }
    }

    // Perform creates in batches of 50
    if (creates.length > 0) {
        console.log(`\nCreating ${creates.length} records...`);
        for (let i = 0; i < creates.length; i += 50) {
            const batch = creates.slice(i, i + 50);
            await table.createRecordsAsync(batch);
            if ((i + 50) % 200 === 0 || i + 50 >= creates.length) {
                console.log(`  Created ${Math.min(i + 50, creates.length)}/${creates.length}`);
            }
        }
    }

    return { updated: updates.length, created: creates.length };
}

// ============ MAIN ============

console.log('PostHog → Airtable sync (HogQL)');
console.log('='.repeat(50));

validateInputs();

console.log(`\nConfiguration:`);
console.log(`  Table: ${POSTHOG_DATA_TABLE}`);
console.log(`  Round: ${ROUND_START} to ${ROUND_END}`);

// Fetch metrics from PostHog
console.log('\n1. Fetching metrics from PostHog...');
const metrics = await fetchMetrics();
console.log(`   Found ${metrics.length} unique handles`);

if (metrics.length === 0) {
    console.log('\nNo data found for this date range.');
} else {
    // Upsert into PostHog data table
    console.log('\n2. Upserting into Airtable...');
    const result = await upsertPostHogData(metrics);

    console.log('\n' + '='.repeat(50));
    console.log(`Done! Updated ${result.updated}, created ${result.created} records.`);
}
