import assert from 'node:assert/strict';
import test from 'node:test';
import { parseArgs } from '../src/cli.js';
import { prepareOrder, processSourceIds } from '../src/uptrack.js';

function order(overrides = {}) {
    return {
        source_id: '305-0000000-0000000',
        source_name: 'Amazon',
        order_id: '4973000000',
        folder_id: 567433,
        tracking_number: 'YT1234567890',
        mark_shipped: false,
        scheduled: false,
        status: 'Paid',
        ...overrides
    };
}

function fakeSupabase(rowsBySourceId) {
    const lookups = [];

    return {
        lookups,
        from() {
            const filters = {};
            const builder = {
                select() { return builder; },
                eq(column, value) { filters[column] = value; return builder; },
                limit(value) { assert.equal(value, 1); return builder; },
                async maybeSingle() {
                    lookups.push(filters.source_id);
                    return { data: rowsBySourceId[filters.source_id] || null, error: null };
                }
            };
            return builder;
        }
    };
}

const findStore = (folderId) =>
    String(folderId) === '567433' ? { folderId: '567433', storeId: 'store', apiKey: 'secret' } : null;

test('parses and deduplicates repeated source IDs', () => {
    const parsed = parseArgs([
        '--source-id', '305-1', '--source-id', '305-2', '--source-id', '305-1', '--dry-run'
    ]);
    assert.deepEqual(parsed.sourceIds, ['305-1', '305-2']);
    assert.equal(parsed.dryRun, true);
});

test('prepares YT tracking with fixed Yunexpress carrier', () => {
    const prepared = prepareOrder(order(), findStore);
    assert.equal(prepared.ok, true);
    assert.deepEqual(prepared.payload, {
        order_id: 4973000000,
        tracking_number: 'YT1234567890',
        carrier_code: 'Yunexpress',
        shipment_method: ''
    });
});

test('prepares UL tracking with Yanwen carrier', () => {
    const prepared = prepareOrder(order({ tracking_number: 'UL1234567890' }), findStore);
    assert.equal(prepared.ok, true);
    assert.deepEqual(prepared.payload, {
        order_id: 4973000000,
        tracking_number: 'UL1234567890',
        carrier_code: 'Yanwen',
        shipment_method: ''
    });
});

test('rejects tracking without a YT or UL prefix for review', () => {
    const prepared = prepareOrder(order({ tracking_number: '920123456789' }), findStore);
    assert.equal(prepared.ok, false);
    assert.equal(prepared.status, 'NEEDS_REVIEW');
    assert.match(prepared.message, /YT or UL/);
});

test('multiple dry-run orders perform one lookup per source ID and no writes', async () => {
    const supabase = fakeSupabase({
        '305-1': order({ source_id: '305-1' }),
        '305-2': order({ source_id: '305-2', tracking_number: 'ABC123' })
    });
    const results = await processSourceIds({
        supabase,
        sourceIds: ['305-1', '305-2'],
        dryRun: true,
        findStore
    });

    assert.deepEqual(supabase.lookups, ['305-1', '305-2']);
    assert.deepEqual(results.map((result) => result.status), ['READY', 'NEEDS_REVIEW']);
});

test('mark_shipped=true remains ready for normal submission', async () => {
    const supabase = fakeSupabase({ '305-1': order({ source_id: '305-1', mark_shipped: true }) });
    const [result] = await processSourceIds({
        supabase,
        sourceIds: ['305-1'],
        dryRun: true,
        findStore
    });
    assert.equal(result.status, 'READY');
    assert.match(result.message, /will still submit normally/);
});

test('mark_shipped=true submits normally in live mode', async () => {
    const supabase = fakeSupabase({ '305-1': order({ source_id: '305-1', mark_shipped: true }) });
    const requests = [];
    const fetchImpl = async (url, init) => {
        requests.push({ url, init });
        return {
            ok: true,
            status: 200,
            async text() { return JSON.stringify({ results: [{ status: 'success' }] }); }
        };
    };
    const [result] = await processSourceIds({
        supabase,
        sourceIds: ['305-1'],
        dryRun: false,
        findStore,
        fetchImpl
    });

    assert.equal(result.status, 'SUCCESS');
    assert.equal(requests.length, 1);
    assert.deepEqual(JSON.parse(requests[0].init.body), [{
        order_id: 4973000000,
        tracking_number: 'YT1234567890',
        carrier_code: 'Yunexpress',
        shipment_method: ''
    }]);
});

test('new live shipment updates only mark_shipped with exact identifiers', async () => {
    const updates = [];
    const supabase = {
        from() {
            const filters = {};
            let updatePayload = null;
            const builder = {
                select() { return builder; },
                update(value) { updatePayload = value; return builder; },
                eq(column, value) { filters[column] = value; return builder; },
                limit(value) { assert.equal(value, 1); return builder; },
                async maybeSingle() {
                    if (updatePayload) {
                        updates.push({ updatePayload, filters: { ...filters } });
                        return {
                            data: { source_id: filters.source_id, order_id: filters.order_id, mark_shipped: true },
                            error: null
                        };
                    }
                    return { data: order({ source_id: filters.source_id }), error: null };
                }
            };
            return builder;
        }
    };
    const fetchImpl = async () => ({
        ok: true,
        status: 200,
        async text() { return JSON.stringify({ results: [{ status: 'success' }] }); }
    });

    const [result] = await processSourceIds({
        supabase,
        sourceIds: ['305-1'],
        dryRun: false,
        findStore,
        fetchImpl
    });

    assert.equal(result.status, 'SUCCESS');
    assert.deepEqual(updates, [{
        updatePayload: { mark_shipped: true },
        filters: { source_name: 'Amazon', source_id: '305-1', order_id: '4973000000' }
    }]);
});
