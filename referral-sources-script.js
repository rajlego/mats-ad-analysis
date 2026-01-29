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

for (const record of applicationsQuery.records) {
    const sources = record.getCellValue(REFERRAL_FIELD);

    if (sources && Array.isArray(sources)) {
        for (const source of sources) {
            const name = source.name;
            sourceCounts[name] = (sourceCounts[name] || 0) + 1;
        }
    }
}

const sourceNames = Object.keys(sourceCounts).sort((a, b) => sourceCounts[b] - sourceCounts[a]);
console.log(`   Found ${sourceNames.length} unique sources`);

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
    fields: ['Source']
});

// Build source -> record mapping
const sourceToRecord = {};
for (const record of outputQuery.records) {
    const source = record.getCellValueAsString('Source');
    if (source) {
        sourceToRecord[source] = record;
    }
}

console.log(`   Found ${Object.keys(sourceToRecord).length} existing records`);

// Prepare updates and creates
const updates = [];
const creates = [];

for (const name of sourceNames) {
    const count = sourceCounts[name];
    const existingRecord = sourceToRecord[name];

    const fields = {
        'Source': name,
        'Count': count,
        'Total applications': totalApplications
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

console.log('\n' + '='.repeat(50));
console.log(`Done! Updated ${updates.length}, created ${creates.length} records.`);
