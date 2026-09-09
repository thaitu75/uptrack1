# Amazon OrderDesk Uptrack CLI

Local-only CLI for uploading Amazon tracking to Order Desk independently from the existing Cloudflare Worker.

## Safety rules

- Each `source_id` is queried separately with a one-row limit. Batch `.in(...)` lookups are not used.
- Tracking comes only from Supabase `orders.tracking_number`.
- Tracking beginning with `YT` uses `carrier_code=Yunexpress`.
- Tracking beginning with `UL` uses `carrier_code=Yanwen`.
- Any other prefix is reported as `NEEDS_REVIEW` and is not submitted.
- Dry run performs no Order Desk request and no Supabase update.
- `mark_shipped=true` does not block submission; a valid `YT` or `UL` order is still submitted normally.
- A successful new shipment updates only `mark_shipped=true`, scoped by `source_name`, `source_id`, and `order_id`.

## Configuration

The CLI loads configuration in this order:

1. Process environment variables.
2. `--env-file <path>`.
3. This project's `.env`.
4. As a local compatibility fallback, the sibling `uptrack-db-amazon-orderdesk/.env`.

Required values are `SUPABASE_URL`, `SUPABASE_SECRET_KEY` (or `SUPABASE_SERVICE_ROLE_KEY`), and the matching Order Desk `STORE_n_FOLDER_ID`, `STORE_n_STORE_ID`, and `STORE_n_API_KEY` triplet.

## Commands

```powershell
npm install
npm run uptrack -- --source-id 305-6103702-4127526 --dry-run
npm run uptrack -- --source-id 305-6103702-4127526
```

Multiple orders are processed independently and sequentially:

```powershell
npm run uptrack -- `
  --source-id 305-6103702-4127526 `
  --source-id 305-6254958-6733104 `
  --dry-run
```
