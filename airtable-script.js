// @ts-nocheck
/**
 * PostHog → Airtable sync script (HogQL version)
 *
 * Fetches metrics from PostHog and upserts into PostHog data table.
 * Uses HogQL for efficient server-side aggregation.
 *
 * Input variables (set in Automation):
 *   - posthog_api_key: Your PostHog personal API key (not project API key)
 *   - posthog_project_id: Your PostHog project ID
 *   - posthog_data_table: Name of the PostHog data table (e.g., "PostHog data")
 *   - round_start: Start date for this round (M/D/YY or M/D/YYYY, e.g., 1/1/25 or 1/1/2025)
 *   - round_end: End date for this round (M/D/YY or M/D/YYYY, e.g., 3/15/25 or 3/15/2025)
 */

const inputConfig = input.config();

// Required inputs
const POSTHOG_API_KEY = inputConfig.posthog_api_key;
const POSTHOG_PROJECT_ID = inputConfig.posthog_project_id;
const POSTHOG_DATA_TABLE = inputConfig.posthog_data_table;
const ROUND_START = inputConfig.round_start; // M/D/YY or M/D/YYYY format
const ROUND_END = inputConfig.round_end; // M/D/YY or M/D/YYYY format

const POSTHOG_HOST = 'https://app.posthog.com';

// Special handles
const DIRECT_HANDLE = '(direct)'; // For traffic without utm_source
const TOTAL_HANDLE = '(all)'; // For total aggregation

// URL patterns to track
const APPLY_PAGE_PATTERN = '%/apply%';
const PROGRAM_PAGE_PATTERN = '%/program/%';

// ============ DATE UTILITIES ============

/**
 * Convert M/D/YY or M/D/YYYY to YYYY-MM-DD for HogQL queries
 * @param {string} dateStr - Date in M/D/YY or M/D/YYYY format (e.g., "1/1/25" or "1/1/2025")
 * @returns {string} Date in YYYY-MM-DD format (e.g., "2025-01-01")
 */
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

/**
 * Convert M/D/YY or M/D/YYYY to Airtable-compatible date (YYYY-MM-DD)
 * Airtable accepts ISO dates and stores them properly
 */
function toAirtableDate(dateStr) {
    return toISODate(dateStr);
}

/**
 * Normalize date to M/D/YY format for Key matching
 * Airtable's DATETIME_FORMAT uses M/D/YY, so we must match that
 * @param {string} dateStr - Date in M/D/YY or M/D/YYYY format
 * @returns {string} Date in M/D/YY format (e.g., "1/1/25")
 */
function toKeyFormat(dateStr) {
    const [month, day, year] = dateStr.split('/');
    const shortYear = year.length === 4 ? year.slice(-2) : year;
    // Remove leading zeros from month/day to match Airtable's M/D/YY format
    return `${parseInt(month)}/${parseInt(day)}/${shortYear}`;
}

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

    if (!/^\d{1,2}\/\d{1,2}\/(\d{2}|\d{4})$/.test(ROUND_START)) {
        throw new Error(`round_start must be M/D/YY or M/D/YYYY format (e.g., 1/1/25 or 1/1/2025), got: ${ROUND_START}`);
    }
    if (!/^\d{1,2}\/\d{1,2}\/(\d{2}|\d{4})$/.test(ROUND_END)) {
        throw new Error(`round_end must be M/D/YY or M/D/YYYY format (e.g., 3/15/25 or 3/15/2025), got: ${ROUND_END}`);
    }
}

// ============ POSTHOG HOGQL API ============

/**
 * List available event properties from PostHog's property definitions API
 * Filters for UTM-related properties
 */
async function listAvailableProperties() {
    const url = `${POSTHOG_HOST}/api/projects/${POSTHOG_PROJECT_ID}/property_definitions/?type=event&limit=500`;

    const response = await fetch(url, {
        method: 'GET',
        headers: {
            'Authorization': `Bearer ${POSTHOG_API_KEY}`,
            'Content-Type': 'application/json'
        }
    });

    if (!response.ok) {
        console.log('   (Could not fetch property definitions)');
        return [];
    }

    const data = await response.json();
    const allProps = (data.results || []).map(p => p.name);

    // Filter for UTM-related and other potentially useful properties
    const utmProps = allProps.filter(p =>
        p.toLowerCase().includes('utm') ||
        p.toLowerCase().includes('referr') ||
        p.toLowerCase().includes('source') ||
        p.toLowerCase().includes('campaign') ||
        p.toLowerCase().includes('medium')
    );

    return utmProps.sort();
}

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
    // Convert M/D/YY to ISO for HogQL
    const startISO = toISODate(ROUND_START);
    const endISO = toISODate(ROUND_END);
    const dateFilter = `timestamp >= toDateTime('${startISO} 00:00:00') AND timestamp <= toDateTime('${endISO} 23:59:59')`;

    // Query with (direct) for missing utm_source
    // Note: Property names are without $ prefix (utm_source, not $utm_source)
    const query = `
        SELECT
            lower(coalesce(nullIf(trim(properties.utm_source), ''), '${DIRECT_HANDLE}')) as handle,
            count() as events,
            countIf(event = '$pageview') as pageviews,
            count(DISTINCT distinct_id) as unique_visitors,
            countIf(event = '$pageview' AND properties.$current_url LIKE '${APPLY_PAGE_PATTERN}') as apply_page_views,
            countIf(event = '$pageview' AND properties.$current_url LIKE '${PROGRAM_PAGE_PATTERN}') as program_page_views,
            min(timestamp) as first_active,
            max(timestamp) as last_active,
            groupUniqArray(properties.utm_campaign) as campaigns
        FROM events
        WHERE ${dateFilter}
        GROUP BY handle
        ORDER BY events DESC
    `;

    console.log('Running HogQL query for per-handle metrics...');
    const results = await runHogQLQuery(query);

    // Query for total aggregation
    const totalQuery = `
        SELECT
            '${TOTAL_HANDLE}' as handle,
            count() as events,
            countIf(event = '$pageview') as pageviews,
            count(DISTINCT distinct_id) as unique_visitors,
            countIf(event = '$pageview' AND properties.$current_url LIKE '${APPLY_PAGE_PATTERN}') as apply_page_views,
            countIf(event = '$pageview' AND properties.$current_url LIKE '${PROGRAM_PAGE_PATTERN}') as program_page_views,
            min(timestamp) as first_active,
            max(timestamp) as last_active,
            groupUniqArray(properties.utm_campaign) as campaigns
        FROM events
        WHERE ${dateFilter}
    `;

    console.log('Running HogQL query for total aggregation...');
    const totalResults = await runHogQLQuery(totalQuery);

    // Combine results
    const allResults = [...results, ...totalResults];

    // Convert to array of metrics objects
    const metrics = [];
    for (const row of allResults) {
        const [handle, events, pageviews, uniqueVisitors, applyViews, programViews, firstActive, lastActive, campaigns] = row;

        if (!handle) continue;

        // Filter out null/empty campaigns, trim each, sort, and join
        const campaignList = (campaigns || [])
            .map(c => (c || '').trim())
            .filter(c => c.length > 0);

        metrics.push({
            handle: handle,
            events: events,
            pageviews: pageviews,
            uniqueVisitors: uniqueVisitors,
            applyPageViews: applyViews,
            programPageViews: programViews,
            firstActive: firstActive ? firstActive.slice(0, 10) : null,
            lastActive: lastActive ? lastActive.slice(0, 10) : null,
            campaigns: campaignList.length > 0 ? [...new Set(campaignList)].sort().join(', ') : null
        });
    }

    return metrics;
}

// ============ AIRTABLE UPSERT ============

function computeKey(handle) {
    // Normalize to M/D/YY to match Airtable's DATETIME_FORMAT
    return `${handle}-${toKeyFormat(ROUND_START)}-${toKeyFormat(ROUND_END)}`;
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

    // Convert round dates to ISO for Airtable
    const roundStartDate = toAirtableDate(ROUND_START);
    const roundEndDate = toAirtableDate(ROUND_END);

    // Prepare updates and creates
    const updates = [];
    const creates = [];

    for (const m of metrics) {
        const key = computeKey(m.handle);
        const existingRecord = keyToRecord[key];

        const fields = {
            'Handle': m.handle,
            'Round start': roundStartDate, // Always use input round dates
            'Round end': roundEndDate, // Always use input round dates
            'Events': m.events,
            'Pageviews': m.pageviews,
            'Unique visitors': m.uniqueVisitors,
            'Apply page views': m.applyPageViews,
            'Program page views': m.programPageViews,
            'First active': m.firstActive, // Observed timestamp
            'Last active': m.lastActive, // Observed timestamp
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
console.log(`  Round (ISO): ${toISODate(ROUND_START)} to ${toISODate(ROUND_END)}`);

// List available UTM properties (via REST API)
console.log('\n1. Discovering available properties...');
const availableProps = await listAvailableProperties();
if (availableProps.length > 0) {
    console.log(`   UTM-related properties: ${availableProps.join(', ')}`);
}

// Fetch metrics from PostHog
console.log('\n2. Fetching metrics from PostHog...');
const metrics = await fetchMetrics();

// Log summary
const directRow = metrics.find(m => m.handle === DIRECT_HANDLE);
const totalRow = metrics.find(m => m.handle === TOTAL_HANDLE);
const handleCount = metrics.filter(m => m.handle !== TOTAL_HANDLE).length;

console.log(`   Found ${handleCount} handles (including ${DIRECT_HANDLE})`);
if (directRow) {
    console.log(`   ${DIRECT_HANDLE}: ${directRow.events} events, ${directRow.pageviews} pageviews`);
}
if (totalRow) {
    console.log(`   ${TOTAL_HANDLE}: ${totalRow.events} events, ${totalRow.pageviews} pageviews, ${totalRow.uniqueVisitors} unique visitors`);
}

if (metrics.length === 0) {
    console.log('\nNo data found for this date range.');
} else {
    // Upsert into PostHog data table
    console.log('\n3. Upserting into Airtable...');
    const result = await upsertPostHogData(metrics);

    console.log('\n' + '='.repeat(50));
    console.log(`Done! Updated ${result.updated}, created ${result.created} records.`);
}
