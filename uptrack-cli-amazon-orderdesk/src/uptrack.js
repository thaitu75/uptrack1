const SOURCE_NAME = 'Amazon';
const ORDERS_TABLE = 'orders';
const ORDERDESK_BATCH_SHIPMENTS_URL = 'https://app.orderdesk.me/api/v2/batch-shipments';

function normalize(value) {
    return value == null ? '' : String(value).trim();
}

export function inferCarrier(trackingNumber) {
    const normalizedTracking = normalize(trackingNumber).toUpperCase();
    if (normalizedTracking.startsWith('YT')) {
        return 'Yunexpress';
    }
    if (normalizedTracking.startsWith('UL')) {
        return 'Yanwen';
    }
    return null;
}

export async function fetchOneOrder(supabase, sourceId) {
    const { data, error } = await supabase
        .from(ORDERS_TABLE)
        .select('source_id,source_name,order_id,folder_id,tracking_number,mark_shipped,scheduled,status')
        .eq('source_name', SOURCE_NAME)
        .eq('source_id', sourceId)
        .limit(1)
        .maybeSingle();

    if (error) {
        throw new Error(`Supabase lookup failed: ${error.message}`);
    }

    return data;
}

export function prepareOrder(order, findStore) {
    if (!order) {
        return { ok: false, status: 'NOT_FOUND', message: 'Amazon order not found' };
    }

    const sourceId = normalize(order.source_id);
    const orderId = normalize(order.order_id);
    const folderId = normalize(order.folder_id);
    const trackingNumber = normalize(order.tracking_number);

    if (!orderId || !folderId || !trackingNumber) {
        return {
            ok: false,
            status: 'NEEDS_REVIEW',
            message: 'Missing order_id, folder_id, or tracking_number'
        };
    }

    const carrier = inferCarrier(trackingNumber);
    if (!carrier) {
        return {
            ok: false,
            status: 'NEEDS_REVIEW',
            message: `tracking_number must start with YT or UL: ${trackingNumber}`
        };
    }

    const store = findStore(folderId);
    if (!store) {
        return {
            ok: false,
            status: 'NEEDS_REVIEW',
            message: `No complete Order Desk store configuration for folder_id=${folderId}`
        };
    }

    return {
        ok: true,
        sourceId,
        orderId,
        folderId,
        trackingNumber,
        carrier,
        alreadyShipped: order.mark_shipped === true,
        scheduled: order.scheduled,
        currentStatus: order.status,
        store,
        payload: {
            order_id: /^\d+$/.test(orderId) && Number.isSafeInteger(Number(orderId)) ? Number(orderId) : orderId,
            tracking_number: trackingNumber,
            carrier_code: carrier,
            shipment_method: ''
        }
    };
}

export async function submitShipment(store, payload, fetchImpl = fetch) {
    const response = await fetchImpl(ORDERDESK_BATCH_SHIPMENTS_URL, {
        method: 'POST',
        headers: {
            'ORDERDESK-STORE-ID': store.storeId,
            'ORDERDESK-API-KEY': store.apiKey,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify([payload])
    });
    const responseText = await response.text();
    let body = null;

    try {
        body = responseText ? JSON.parse(responseText) : null;
    } catch {
        body = null;
    }

    if (!response.ok) {
        throw new Error(`Order Desk HTTP ${response.status}: ${responseText || 'empty response'}`);
    }

    const itemResult = Array.isArray(body?.results) ? body.results[0] : null;
    const success = itemResult ? itemResult.status === 'success' : body?.status === 'success';
    if (!success) {
        throw new Error(itemResult?.message || body?.message || responseText || 'Order Desk returned no success status');
    }

    return body;
}

export async function markOrderShipped(supabase, sourceId, orderId) {
    const { data, error } = await supabase
        .from(ORDERS_TABLE)
        .update({ mark_shipped: true })
        .eq('source_name', SOURCE_NAME)
        .eq('source_id', sourceId)
        .eq('order_id', orderId)
        .select('source_id,order_id,mark_shipped')
        .limit(1)
        .maybeSingle();

    if (error) {
        throw new Error(`Order Desk succeeded but Supabase mark_shipped update failed: ${error.message}`);
    }
    if (!data || data.mark_shipped !== true) {
        throw new Error('Order Desk succeeded but Supabase mark_shipped update was not verified');
    }

    return data;
}

export async function processSourceId({
    supabase,
    sourceId,
    dryRun,
    findStore,
    fetchImpl = fetch
}) {
    const order = await fetchOneOrder(supabase, sourceId);
    const prepared = prepareOrder(order, findStore);
    if (!prepared.ok) {
        return { source_id: sourceId, status: prepared.status, message: prepared.message };
    }

    const publicDetails = {
        source_id: prepared.sourceId,
        orderdesk_order_id: prepared.orderId,
        folder_id: prepared.folderId,
        tracking_number: prepared.trackingNumber,
        carrier: prepared.carrier,
        mark_shipped: prepared.alreadyShipped,
        scheduled: prepared.scheduled,
        current_status: prepared.currentStatus
    };

    if (dryRun) {
        return {
            ...publicDetails,
            status: 'READY',
            message: prepared.alreadyShipped ? 'mark_shipped is already true; live mode will still submit normally' : null,
            planned_payload: prepared.payload
        };
    }

    await submitShipment(prepared.store, prepared.payload, fetchImpl);
    if (!prepared.alreadyShipped) {
        await markOrderShipped(supabase, prepared.sourceId, prepared.orderId);
    }

    return {
        ...publicDetails,
        status: 'SUCCESS',
        message: prepared.alreadyShipped
            ? 'Shipment uploaded; mark_shipped was already true'
            : 'Shipment uploaded and mark_shipped verified'
    };
}

export async function processSourceIds(options) {
    const results = [];

    for (const sourceId of options.sourceIds) {
        try {
            results.push(await processSourceId({ ...options, sourceId }));
        } catch (error) {
            results.push({ source_id: sourceId, status: 'FAILED', message: error.message });
        }
    }

    return results;
}
