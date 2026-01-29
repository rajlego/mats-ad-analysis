// @ts-nocheck
/**
 * Referral Sources Daily Script
 *
 * Calculates cumulative application counts per referral source by creation date.
 * Creates one row per source per day showing how many apps existed by that date.
 *
 * Run in: Airtable Automation (or Scripting extension)
 *
 * Tables:
 *   - Source: 10.0 application
 *   - Output: 10.0 referral sources daily
 */

const APPLICATIONS_TABLE = '10.0 application';
const REFERRAL_FIELD = '[stage-1-logistics] How did you hear about us?';
const STAGE_2_FIELD = '[stage-1-infra] Advance to stage 2';
const CREATED_TIME_FIELD = 'Created'; // Add "Created time" field to table if missing
const OUTPUT_TABLE = '10.0 referral sources daily';

// Special source names
const BLANK_SOURCE = '(no response)';
const TOTAL_SOURCE = '(all)';

// Stage 2 status values
const STATUS_ADVANCED = 'Advanced to stage 2';
const STATUS_REJECTED = 'Reject';

function normalizeSource(name) {
    return (name || '').trim();
}

// ============ MAIN ============

console.log('Referral Sources Daily');
console.log('='.repeat(50));

const applicationsTable = base.getTable(APPLICATIONS_TABLE);
const outputTable = base.getTable(OUTPUT_TABLE);

// Fetch all applications with created time
console.log(`\n1. Fetching records from "${APPLICATIONS_TABLE}"...`);
const applicationsQuery = await applicationsTable.selectRecordsAsync({
    fields: [REFERRAL_FIELD, STAGE_2_FIELD, CREATED_TIME_FIELD]
});

const totalApplications = applicationsQuery.records.length;
console.log(`   Found ${totalApplications} applications`);

// Build list of applications with their data
console.log('\n2. Processing applications by creation date...');
const applications = [];

for (const record of applicationsQuery.records) {
    const createdTime = record.getCellValue(CREATED_TIME_FIELD);
    if (!createdTime) {
        console.log(`   Warning: record ${record.id} has no created time, skipping`);
        continue;
    }
    // Handle both string and Date object
    const createdDate = typeof createdTime === 'string'
        ? createdTime.slice(0, 10)
        : createdTime.toISOString().slice(0, 10); // YYYY-MM-DD
    const sources = record.getCellValue(REFERRAL_FIELD);
    const stage2Status = record.getCellValueAsString(STAGE_2_FIELD);

    const isAdvanced = stage2Status === STATUS_ADVANCED;
    const isRejected = stage2Status === STATUS_REJECTED;

    // Get source names (or blank)
    let sourceNames = [];
    if (!sources || !Array.isArray(sources) || sources.length === 0) {
        sourceNames = [BLANK_SOURCE];
    } else {
        sourceNames = sources.map(s => normalizeSource(s.name)).filter(n => n);
    }

    applications.push({
        createdDate,
        sources: sourceNames,
        isAdvanced,
        isRejected
    });
}

// Get all unique dates and sources
const allDates = [...new Set(applications.map(a => a.createdDate))].sort();
const allSources = new Set();
for (const app of applications) {
    for (const source of app.sources) {
        allSources.add(source);
    }
}
allSources.add(TOTAL_SOURCE);

console.log(`   Date range: ${allDates[0]} to ${allDates[allDates.length - 1]}`);
console.log(`   Unique sources: ${allSources.size}`);

// Calculate cumulative counts for each date and source
// Optimized O(n) approach: sort apps by date, accumulate as we go
console.log('\n3. Calculating cumulative counts...');

// Sort applications by creation date
applications.sort((a, b) => a.createdDate.localeCompare(b.createdDate));

// Initialize running totals per source
const runningTotals = {};
for (const source of allSources) {
    runningTotals[source] = { count: 0, advanced: 0, rejected: 0, pending: 0 };
}
let totalCount = 0;
let totalAdvanced = 0;
let totalRejected = 0;
let totalPending = 0;

// Group applications by date
const appsByDate = {};
for (const app of applications) {
    if (!appsByDate[app.createdDate]) {
        appsByDate[app.createdDate] = [];
    }
    appsByDate[app.createdDate].push(app);
}

const dailyData = []; // { date, source, count, advanced, rejected, pending }

// Process each date in order, accumulating totals
for (const targetDate of allDates) {
    const appsOnDate = appsByDate[targetDate] || [];

    // Add this date's apps to running totals
    for (const app of appsOnDate) {
        totalCount++;
        if (app.isAdvanced) totalAdvanced++;
        else if (app.isRejected) totalRejected++;
        else totalPending++;

        for (const source of app.sources) {
            if (!runningTotals[source]) {
                runningTotals[source] = { count: 0, advanced: 0, rejected: 0, pending: 0 };
            }
            runningTotals[source].count++;
            if (app.isAdvanced) runningTotals[source].advanced++;
            else if (app.isRejected) runningTotals[source].rejected++;
            else runningTotals[source].pending++;
        }
    }

    // Snapshot current totals for this date (excluding TOTAL_SOURCE which we handle separately)
    for (const [source, data] of Object.entries(runningTotals)) {
        if (source !== TOTAL_SOURCE && data.count > 0) {
            dailyData.push({
                date: targetDate,
                source: source,
                count: data.count,
                advanced: data.advanced,
                rejected: data.rejected,
                pending: data.pending
            });
        }
    }

    // Add total row for this date
    dailyData.push({
        date: targetDate,
        source: TOTAL_SOURCE,
        count: totalCount,
        advanced: totalAdvanced,
        rejected: totalRejected,
        pending: totalPending
    });
}

console.log(`   Generated ${dailyData.length} daily records`);

// Fetch existing records from output table
console.log(`\n4. Fetching existing records from "${OUTPUT_TABLE}"...`);
const outputQuery = await outputTable.selectRecordsAsync({
    fields: ['Key', 'Source', 'Date']
});

// Build key -> record mapping
const keyToRecord = {};
for (const record of outputQuery.records) {
    const key = record.getCellValueAsString('Key');
    if (key) {
        keyToRecord[key] = record;
    }
}

console.log(`   Found ${Object.keys(keyToRecord).length} existing records`);

// Prepare updates and creates
const updates = [];
const creates = [];

for (const d of dailyData) {
    const key = `${d.source}-${d.date}`;
    const existingRecord = keyToRecord[key];

    const fields = {
        'Source': d.source,
        'Date': d.date,
        'Cumulative count': d.count,
        'Cumulative stage 2 advanced': d.advanced,
        'Cumulative stage 2 rejected': d.rejected,
        'Cumulative stage 2 pending': d.pending
    };

    if (existingRecord) {
        updates.push({ id: existingRecord.id, fields });
    } else {
        creates.push({ fields });
    }
}

console.log(`\n   To update: ${updates.length}`);
console.log(`   To create: ${creates.length}`);

// Perform updates in batches of 50
if (updates.length > 0) {
    console.log(`\n5. Updating ${updates.length} records...`);
    for (let i = 0; i < updates.length; i += 50) {
        const batch = updates.slice(i, i + 50);
        await outputTable.updateRecordsAsync(batch);
    }
}

// Perform creates in batches of 50
if (creates.length > 0) {
    console.log(`\n6. Creating ${creates.length} records...`);
    for (let i = 0; i < creates.length; i += 50) {
        const batch = creates.slice(i, i + 50);
        await outputTable.createRecordsAsync(batch);
    }
}

console.log('\n' + '='.repeat(50));
console.log(`Done! Updated ${updates.length}, created ${creates.length} records.`);

// Show latest totals
const latestDate = allDates[allDates.length - 1];
const latestTotal = dailyData.find(d => d.date === latestDate && d.source === TOTAL_SOURCE);
if (latestTotal) {
    const advRate = latestTotal.count > 0
        ? ((latestTotal.advanced / latestTotal.count) * 100).toFixed(1)
        : '0';
    console.log(`\nLatest (${latestDate}):`);
    console.log(`  Total: ${latestTotal.count} applications`);
    console.log(`  Advanced: ${latestTotal.advanced} (${advRate}%)`);
    console.log(`  Rejected: ${latestTotal.rejected}`);
    console.log(`  Pending: ${latestTotal.pending}`);
}
