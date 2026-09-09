#!/usr/bin/env node

import { createClient } from '@supabase/supabase-js';
import { pathToFileURL } from 'node:url';
import { findOrderDeskStore, loadConfig } from './config.js';
import { processSourceIds } from './uptrack.js';

function usage() {
    return [
        'Usage:',
        '  npm run uptrack -- --source-id <Amazon source_id> [--source-id <id> ...] [--dry-run]',
        '',
        'Options:',
        '  --source-id <id>   Process one exact source_id; repeat for multiple orders',
        '  --dry-run          Validate and show payload without Order Desk or Supabase writes',
        '  --env-file <path>   Use a specific local env file',
        '  --help              Show this help'
    ].join('\n');
}

export function parseArgs(argv) {
    const options = { sourceIds: [], dryRun: false, envFile: null };

    for (let index = 0; index < argv.length; index += 1) {
        const arg = argv[index];
        if (arg === '--source-id') {
            const value = argv[++index]?.trim();
            if (!value) throw new Error('--source-id requires a value');
            options.sourceIds.push(value);
        } else if (arg === '--dry-run') {
            options.dryRun = true;
        } else if (arg === '--env-file') {
            const value = argv[++index]?.trim();
            if (!value) throw new Error('--env-file requires a path');
            options.envFile = value;
        } else if (arg === '--help' || arg === '-h') {
            options.help = true;
        } else {
            throw new Error(`Unknown argument: ${arg}`);
        }
    }

    options.sourceIds = [...new Set(options.sourceIds)];
    if (!options.help && options.sourceIds.length === 0) {
        throw new Error('At least one --source-id is required');
    }
    return options;
}

async function main() {
    let options;
    try {
        options = parseArgs(process.argv.slice(2));
    } catch (error) {
        console.error(`ERROR: ${error.message}\n\n${usage()}`);
        process.exitCode = 2;
        return;
    }

    if (options.help) {
        console.log(usage());
        return;
    }

    try {
        const config = loadConfig(options.envFile);
        const supabase = createClient(config.supabaseUrl, config.supabaseKey, {
            auth: { persistSession: false, autoRefreshToken: false }
        });
        const results = await processSourceIds({
            supabase,
            sourceIds: options.sourceIds,
            dryRun: options.dryRun,
            findStore: (folderId) => findOrderDeskStore(config.values, folderId)
        });

        console.log(JSON.stringify({ mode: options.dryRun ? 'dry-run' : 'live', results }, null, 2));
        const accepted = new Set(options.dryRun ? ['READY'] : ['SUCCESS']);
        if (results.some((result) => !accepted.has(result.status))) {
            process.exitCode = 1;
        }
    } catch (error) {
        console.error(`ERROR: ${error.message}`);
        process.exitCode = 1;
    }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    await main();
}
