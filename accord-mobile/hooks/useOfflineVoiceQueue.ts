import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import AsyncStorage from "@react-native-async-storage/async-storage";

const STORAGE_KEY = "accord.mobile.voice.offlineQueue.v1";

export type OfflineVoiceCommitStatus = "PENDING" | "SYNCED" | "FAILED";

export type OfflineVoiceCommitItem = {
  idempotencyKey: string;
  transcript: string;
  currencyCode?: string;
  exchangeRate?: string;
  sessionId?: string;
  createdAt: string;
  status: OfflineVoiceCommitStatus;
  retryCount: number;
  lastError?: string;
  syncedAt?: string;
  ledgerReference?: string;
  entryId?: string;
};

type SyncResult = {
  ledgerResult?: {
    reference?: string;
    entry_id?: string | number;
  };
};

const newKey = () =>
  `offline_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;

export function useOfflineVoiceQueue() {
  const [queue, setQueue] = useState<OfflineVoiceCommitItem[]>([]);
  const [loaded, setLoaded] = useState(false);
  const queueRef = useRef<OfflineVoiceCommitItem[]>([]);

  const persistQueue = useCallback(async (next: OfflineVoiceCommitItem[]) => {
    queueRef.current = next;
    setQueue(next);
    await AsyncStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  }, []);

  const hydrateQueue = useCallback(async () => {
    try {
      const raw = await AsyncStorage.getItem(STORAGE_KEY);
      if (!raw) {
        queueRef.current = [];
        setQueue([]);
      } else {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          queueRef.current = parsed as OfflineVoiceCommitItem[];
          setQueue(parsed as OfflineVoiceCommitItem[]);
        } else {
          queueRef.current = [];
          setQueue([]);
        }
      }
    } catch {
      queueRef.current = [];
      setQueue([]);
    } finally {
      setLoaded(true);
    }
  }, []);

  useEffect(() => {
    void hydrateQueue();
  }, [hydrateQueue]);

  const queueCommit = useCallback(
    async (payload: {
      transcript: string;
      currencyCode?: string;
      exchangeRate?: string;
      sessionId?: string;
      idempotencyKey?: string;
    }) => {
      const transcript = payload.transcript.trim();
      if (!transcript) {
        return "";
      }
      const idempotencyKey = payload.idempotencyKey || newKey();
      const now = new Date().toISOString();
      const item: OfflineVoiceCommitItem = {
        idempotencyKey,
        transcript,
        currencyCode: payload.currencyCode,
        exchangeRate: payload.exchangeRate,
        sessionId: payload.sessionId,
        createdAt: now,
        status: "PENDING",
        retryCount: 0,
      };
      await persistQueue([item, ...queueRef.current]);
      return idempotencyKey;
    },
    [persistQueue]
  );

  const markFailed = useCallback(
    async (idempotencyKey: string, error: string) => {
      const next = queueRef.current.map((item) => {
        if (item.idempotencyKey !== idempotencyKey) {
          return item;
        }
        return {
          ...item,
          status: "FAILED" as const,
          retryCount: item.retryCount + 1,
          lastError: error,
        };
      });
      await persistQueue(next);
    },
    [persistQueue]
  );

  const markSynced = useCallback(
    async (idempotencyKey: string, result?: SyncResult) => {
      const next = queueRef.current.map((item) => {
        if (item.idempotencyKey !== idempotencyKey) {
          return item;
        }
        return {
          ...item,
          status: "SYNCED" as const,
          syncedAt: new Date().toISOString(),
          lastError: undefined,
          ledgerReference: result?.ledgerResult?.reference || item.ledgerReference,
          entryId: result?.ledgerResult?.entry_id ? String(result.ledgerResult.entry_id) : item.entryId,
        };
      });
      await persistQueue(next);
    },
    [persistQueue]
  );

  const resetToPending = useCallback(
    async (idempotencyKey: string) => {
      const next = queueRef.current.map((item) => {
        if (item.idempotencyKey !== idempotencyKey) {
          return item;
        }
        return {
          ...item,
          status: "PENDING" as const,
          lastError: undefined,
        };
      });
      await persistQueue(next);
    },
    [persistQueue]
  );

  const clearSynced = useCallback(async () => {
    const next = queueRef.current.filter((item) => item.status !== "SYNCED");
    await persistQueue(next);
  }, [persistQueue]);

  const pendingItems = useMemo(
    () => queue.filter((item) => item.status === "PENDING" || item.status === "FAILED"),
    [queue]
  );

  return {
    loaded,
    queue,
    pendingItems,
    pendingCount: pendingItems.length,
    queueCommit,
    markFailed,
    markSynced,
    resetToPending,
    clearSynced,
    hydrateQueue,
  };
}

export default useOfflineVoiceQueue;
