import { openDB } from "idb";

const DB_NAME = "accord-offline-pos";
const DB_VERSION = 1;
const STORE_NAME = "pending-sales";

function getSmeRole() {
  const role = window.localStorage.getItem("smeRole");
  return role && role.trim() ? role.trim().toLowerCase() : "owner";
}

async function getDb() {
  return openDB(DB_NAME, DB_VERSION, {
    upgrade(db) {
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        const store = db.createObjectStore(STORE_NAME, { keyPath: "id", autoIncrement: true });
        store.createIndex("createdAt", "createdAt");
      }
    },
  });
}

export async function queueSale(payload) {
  const db = await getDb();
  await db.add(STORE_NAME, {
    payload,
    createdAt: Date.now(),
  });
}

export async function getQueuedSales() {
  const db = await getDb();
  return db.getAll(STORE_NAME);
}

function queueSignature(entry) {
  return JSON.stringify({
    payload: entry?.payload || {},
    createdAt: Number(entry?.createdAt || 0),
  });
}

export async function removeQueuedSale(id) {
  const db = await getDb();
  await db.delete(STORE_NAME, id);
}

export async function flushQueuedSales() {
  const queued = await getQueuedSales();
  if (queued.length === 0) {
    return { synced: 0, remaining: 0 };
  }

  let synced = 0;
  const roleHeader = getSmeRole();

  for (const entry of queued) {
    try {
      const res = await fetch("/api/v1/sme/transactions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-SME-Role": roleHeader,
        },
        body: JSON.stringify(entry.payload),
      });

      if (!res.ok) {
        continue;
      }

      await removeQueuedSale(entry.id);
      synced += 1;
    } catch {
      // Leave item in queue and retry on the next online event.
    }
  }

  const remaining = (await getQueuedSales()).length;
  return { synced, remaining };
}

export async function mergeSharedQueueEntries(entries) {
  if (!Array.isArray(entries) || entries.length === 0) {
    return { merged: 0 };
  }

  const db = await getDb();
  const existing = await db.getAll(STORE_NAME);
  const known = new Set(existing.map(queueSignature));

  let merged = 0;
  for (const entry of entries) {
    const normalized = {
      payload: entry?.payload || {},
      createdAt: Number(entry?.createdAt || Date.now()),
    };
    const signature = queueSignature(normalized);
    if (known.has(signature)) {
      continue;
    }
    await db.add(STORE_NAME, normalized);
    known.add(signature);
    merged += 1;
  }

  return { merged };
}
