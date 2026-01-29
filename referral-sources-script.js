// @ts-nocheck
/**
 * Referral sources aggregation script
 *
 * Counts "How did you hear about us?" multi-select responses
 * and upserts into the referral sources table.
 *
 * Run in: Airtable Automation (or Scripting extension)
 *
 * Tables:
 *   - Source: 10.0 applications
 *   - Output: 10.0 referral sources
 */

const APPLICATIONS_TABLE = '10.0 applications';
const REFERRAL_FIELD = '[stage-1-logistics] How did you hear about us?';
const OUTPUT_TABLE = '10.0 referral sources';

// Special source for blank responses
const BLANK_SOURCE = '(no response)';

/**
 * Normalize source name for consistent matching
 */
function normalizeSource(name) {
    return (name || '').trim();
}

// ============ MAIN ============

console.log('Referral sources aggregation');
console.log('='.repeat(50));

// Get tables
const applicationsTable = base.getTable(APPLICATIONS_TABLE);
const outputTable = base.getTable(OUTPUT_TABLE);

// Fetch all applications
console.log(`\n1. Fetching records from "${APPLICATIONS_TABLE}"...`);
const applicationsQuery = await applicationsTable.selectRecordsAsync({
    fields: [REFERRAL_FIELD]
});

const totalApplications = applicationsQuery.records.length;
console.log(`   Found ${totalApplications} applications`);

// Count each referral source
console.log('\n2. Counting referral sources...');
const sourceCounts = {};
let blankCount = 0;

for (const record of applicationsQuery.records) {
    const sources = record.getCellValue(REFERRAL_FIELD);

    if (!sources || !Array.isArray(sources) || sources.length === 0) {
        // No response
        blankCount++;
    } else {
        for (const source of sources) {
            const name = normalizeSource(source.name);
            if (name) {
                sourceCounts[name] = (sourceCounts[name] || 0) + 1;
            }
        }
    }
}

// Add blank responses
if (blankCount > 0) {
    sourceCounts[BLANK_SOURCE] = blankCount;
}

const sourceNames = Object.keys(sourceCounts).sort((a, b) => sourceCounts[b] - sourceCounts[a]);
console.log(`   Found ${sourceNames.length} unique sources (including ${BLANK_SOURCE} if any)`);
console.log(`   Blank responses: ${blankCount}`);

// Show top 5
console.log('\n   Top 5 sources:');
for (const name of sourceNames.slice(0, 5)) {
    const count = sourceCounts[name];
    const pct = ((count / totalApplications) * 100).toFixed(1);
    console.log(`   - ${name}: ${count} (${pct}%)`);
}

// Fetch existing records from output table
console.log(`\n3. Fetching existing records from "${OUTPUT_TABLE}"...`);
const outputQuery = await outputTable.selectRecordsAsync({
    fields: ['Source', 'Count']
});

// Build source -> record mapping and detect duplicates
const sourceToRecord = {};
const duplicates = [];

for (const record of outputQuery.records) {
    const source = normalizeSource(record.getCellValueAsString('Source'));
    if (source) {
        if (sourceToRecord[source]) {
            duplicates.push(source);
        }
        sourceToRecord[source] = record;
    }
}

console.log(`   Found ${Object.keys(sourceToRecord).length} existing records`);

// Warn about duplicates
if (duplicates.length > 0) {
    console.log(`\n   WARNING: Found ${duplicates.length} duplicate source(s) in output table:`);
    for (const dup of duplicates) {
        console.log(`   - "${dup}"`);
    }
    console.log('   Consider removing duplicates manually.');
}

// Prepare updates and creates
const updates = [];
const creates = [];

// Track which existing sources we've seen (to zero out missing ones)
const seenSources = new Set();

for (const name of sourceNames) {
    const count = sourceCounts[name];
    const normalizedName = normalizeSource(name);
    const existingRecord = sourceToRecord[normalizedName];

    const fields = {
        'Source': name,
        'Count': count,
        'Total applications': totalApplications
    };

    if (existingRecord) {
        seenSources.add(normalizedName);
        updates.push({ id: existingRecord.id, fields });
    } else {
        creates.push({ fields });
    }
}

// Zero out sources that weren't in this run
const zeroUpdates = [];
for (const [source, record] of Object.entries(sourceToRecord)) {
    if (!seenSources.has(source)) {
        zeroUpdates.push({
            id: record.id,
            fields: {
                'Count': 0,
                'Total applications': totalApplications
            }
        });
    }
}

if (zeroUpdates.length > 0) {
    console.log(`\n   Sources no longer present (will set to 0): ${zeroUpdates.length}`);
    for (const u of zeroUpdates) {
        const record = outputQuery.records.find(r => r.id === u.id);
        if (record) {
            console.log(`   - "${record.getCellValueAsString('Source')}"`);
        }
    }
}

console.log(`\n   To update: ${updates.length}`);
console.log(`   To create: ${creates.length}`);
console.log(`   To zero: ${zeroUpdates.length}`);

// Perform updates in batches of 50
if (updates.length > 0) {
    console.log(`\n4. Updating ${updates.length} records...`);
    for (let i = 0; i < updates.length; i += 50) {
        const batch = updates.slice(i, i + 50);
        await outputTable.updateRecordsAsync(batch);
    }
}

// Perform creates in batches of 50
if (creates.length > 0) {
    console.log(`\n5. Creating ${creates.length} records...`);
    for (let i = 0; i < creates.length; i += 50) {
        const batch = creates.slice(i, i + 50);
        await outputTable.createRecordsAsync(batch);
    }
}

// Zero out missing sources in batches of 50
if (zeroUpdates.length > 0) {
    console.log(`\n6. Zeroing ${zeroUpdates.length} missing sources...`);
    for (let i = 0; i < zeroUpdates.length; i += 50) {
        const batch = zeroUpdates.slice(i, i + 50);
        await outputTable.updateRecordsAsync(batch);
    }
}

console.log('\n' + '='.repeat(50));
console.log(`Done! Updated ${updates.length}, created ${creates.length}, zeroed ${zeroUpdates.length} records.`);
