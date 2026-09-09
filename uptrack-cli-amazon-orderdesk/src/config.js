import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const PROJECT_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const FALLBACK_ENV_FILE = path.resolve(PROJECT_DIR, '..', 'uptrack-db-amazon-orderdesk', '.env');

function parseEnvFile(filePath) {
    const values = {};

    for (const rawLine of fs.readFileSync(filePath, 'utf8').split(/\r?\n/)) {
        const line = rawLine.trim();
        if (!line || line.startsWith('#')) {
            continue;
        }

        const separator = line.indexOf('=');
        if (separator < 1) {
            continue;
        }

        const key = line.slice(0, separator).trim();
        let value = line.slice(separator + 1).trim();
        if (
            (value.startsWith('"') && value.endsWith('"')) ||
            (value.startsWith("'") && value.endsWith("'"))
        ) {
            value = value.slice(1, -1);
        }
        values[key] = value;
    }

    return values;
}

function resolveEnvFile(explicitPath) {
    if (explicitPath) {
        const resolved = path.resolve(explicitPath);
        if (!fs.existsSync(resolved)) {
            throw new Error(`Env file not found: ${resolved}`);
        }
        return resolved;
    }

    const localEnv = path.join(PROJECT_DIR, '.env');
    if (fs.existsSync(localEnv)) {
        return localEnv;
    }

    return fs.existsSync(FALLBACK_ENV_FILE) ? FALLBACK_ENV_FILE : null;
}

export function loadConfig(explicitEnvPath) {
    const envFile = resolveEnvFile(explicitEnvPath);
    const fileValues = envFile ? parseEnvFile(envFile) : {};
    const values = { ...fileValues, ...process.env };
    const supabaseUrl = values.SUPABASE_URL?.trim();
    const supabaseKey = (values.SUPABASE_SECRET_KEY || values.SUPABASE_SERVICE_ROLE_KEY)?.trim();

    if (!supabaseUrl || !supabaseKey) {
        throw new Error('Missing SUPABASE_URL and SUPABASE_SECRET_KEY (or SUPABASE_SERVICE_ROLE_KEY)');
    }

    return {
        values,
        supabaseUrl,
        supabaseKey,
        envFile
    };
}

export function findOrderDeskStore(values, folderId) {
    const normalizedFolderId = String(folderId ?? '').trim();

    for (const [key, value] of Object.entries(values)) {
        const match = key.match(/^STORE_(\d+)_FOLDER_ID$/);
        if (!match || String(value).trim() !== normalizedFolderId) {
            continue;
        }

        const prefix = `STORE_${match[1]}`;
        const storeId = String(values[`${prefix}_STORE_ID`] || '').trim();
        const apiKey = String(values[`${prefix}_API_KEY`] || '').trim();
        if (storeId && apiKey) {
            return { folderId: normalizedFolderId, storeId, apiKey };
        }
    }

    return null;
}

export { PROJECT_DIR };
