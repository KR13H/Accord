import { open } from "@op-engineering/op-sqlite";

const CACHE_DB_NAME = "accord_offline_cache.db";
const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

const db = open({ name: CACHE_DB_NAME });
let schemaReady = false;

async function ensureSchema(): Promise<void> {
  if (schemaReady) {
    return;
  }

  await db.execute(`
    CREATE TABLE IF NOT EXISTS api_cache (
      cache_key TEXT PRIMARY KEY,
      payload_json TEXT NOT NULL,
      created_at_ms INTEGER NOT NULL,
      expires_at_ms INTEGER NOT NULL
    )
  `);

  schemaReady = true;
}

export async function cacheJsonResponse(cacheKey: string, payload: unknown, ttlMs: number = DEFAULT_TTL_MS): Promise<void> {
  if (!cacheKey.trim()) {
    return;
  }

  await ensureSchema();
  const now = Date.now();
  const expiresAt = now + Math.max(10_000, ttlMs);

  await db.execute(
    `
      INSERT OR REPLACE INTO api_cache(cache_key, payload_json, created_at_ms, expires_at_ms)
      VALUES (?, ?, ?, ?)
    `,
    [cacheKey, JSON.stringify(payload ?? null), now, expiresAt]
  );
}

export async function readJsonResponse<T>(cacheKey: string): Promise<T | null> {
  if (!cacheKey.trim()) {
    return null;
  }

  await ensureSchema();
  const result = await db.execute(
    `
      SELECT payload_json
      FROM api_cache
      WHERE cache_key = ?
        AND expires_at_ms >= ?
      LIMIT 1
    `,
    [cacheKey, Date.now()]
  );

  const first = result.rows?.[0];
  if (!first || typeof first.payload_json !== "string") {
    return null;
  }

  try {
    return JSON.parse(first.payload_json) as T;
  } catch {
    return null;
  }
}
