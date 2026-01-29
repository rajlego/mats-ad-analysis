// @ts-nocheck
/**
 * PostHog data table setup script
 *
 * IMPORTANT: Run this in the Scripting EXTENSION, not Automations.
 * Automations cannot create fields.
 *
 * Setup:
 * 1. Create an empty table called "PostHog data" in your base
 * 2. Go to Extensions → Add an extension → Scripting
 * 3. Paste this script and click Run
 */

const TABLE_NAME = 'PostHog data';

// Get the table
let table;
try {
    table = base.getTable(TABLE_NAME);
} catch (e) {
    output.text(`Error: Table "${TABLE_NAME}" not found.`);
    output.text('Please create an empty table with that name first, then run this script again.');
    throw e;
}

output.text(`Setting up fields for "${TABLE_NAME}"...`);
output.text('');

// Define fields to create
const fields = [
    {
        name: 'Handle',
        type: 'singleLineText',
        description: 'The utm_source value (lowercased, trimmed)'
    },
    {
        name: 'Round start',
        type: 'date',
        options: { dateFormat: { name: 'iso' } },
        description: 'Start of date range'
    },
    {
        name: 'Round end',
        type: 'date',
        options: { dateFormat: { name: 'iso' } },
        description: 'End of date range'
    },
    {
        name: 'Events',
        type: 'number',
        options: { precision: 0 },
        description: 'Total events'
    },
    {
        name: 'Pageviews',
        type: 'number',
        options: { precision: 0 },
        description: 'Total $pageview events'
    },
    {
        name: 'Unique visitors',
        type: 'number',
        options: { precision: 0 },
        description: 'Distinct users'
    },
    {
        name: 'Apply page views',
        type: 'number',
        options: { precision: 0 },
        description: '/apply page visits'
    },
    {
        name: 'Program page views',
        type: 'number',
        options: { precision: 0 },
        description: '/program/ page visits'
    },
    {
        name: 'First active',
        type: 'date',
        options: { dateFormat: { name: 'iso' } },
        description: 'First event in range'
    },
    {
        name: 'Last active',
        type: 'date',
        options: { dateFormat: { name: 'iso' } },
        description: 'Last event in range'
    },
    {
        name: 'Campaigns',
        type: 'multilineText',
        description: 'Comma-separated utm_campaign values'
    }
];

// Get existing field names
const existingFields = table.fields.map(f => f.name);

// Create fields that don't exist
for (const field of fields) {
    if (existingFields.includes(field.name)) {
        output.text(`✓ "${field.name}" already exists`);
    } else {
        try {
            await table.createFieldAsync(field.name, field.type, field.options || null, field.description);
            output.text(`✓ Created "${field.name}"`);
        } catch (e) {
            output.text(`✗ Error creating "${field.name}": ${e.message}`);
        }
    }
}

// Create the Key formula field
const keyFieldName = 'Key';
if (existingFields.includes(keyFieldName)) {
    output.text(`✓ "${keyFieldName}" already exists`);
} else {
    try {
        const formula = 'IF(AND({Handle}, {Round start}, {Round end}), {Handle} & "-" & DATETIME_FORMAT({Round start}, \'M/D/YY\') & "-" & DATETIME_FORMAT({Round end}, \'M/D/YY\'), "")';
        await table.createFieldAsync(keyFieldName, 'formula', { formula }, 'Primary key: Handle-StartDate-EndDate');
        output.text(`✓ Created "${keyFieldName}" formula field`);
    } catch (e) {
        output.text(`✗ Error creating "${keyFieldName}": ${e.message}`);
    }
}

output.text('');
output.text('Done! Your PostHog data table is ready.');
output.text('');
output.text('Next steps:');
output.text('1. Create an Automation with "Run script" action');
output.text('2. Paste the main airtable-script.js');
output.text('3. Configure the input variables');
