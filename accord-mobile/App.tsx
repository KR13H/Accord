import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  AppState,
  AppStateStatus,
  SafeAreaView,
  ScrollView,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  useWindowDimensions,
} from "react-native";
import Voice from "@react-native-voice/voice";
import * as SecureStore from "expo-secure-store";
import NetInfo from "@react-native-community/netinfo";

type Txn = {
  payment_id: string;
  status: string;
  amount_inr: string;
  challan_reference: string;
  updated_at: string;
};

type RealtimeEvent = {
  id: string;
  type: string;
  summary: string;
  occurred_at: string;
};

const API_BASE = "http://127.0.0.1:8000";
const RETRY_DELAYS_MS = [800, 1600, 3200];
const REQUEST_TIMEOUT_MS = 12000;
const SSE_MAX_BACKOFF_MS = 20000;
const SESSION_TOKEN_KEY = "accord.mobile.sessionToken";
const SESSION_CREATED_KEY = "accord.mobile.sessionCreatedAt";

export default function App() {
  const { width } = useWindowDimensions();
  const isTablet = width >= 900;
  const gridColumns = isTablet ? 2 : 1;

  const [sessionToken, setSessionToken] = useState("");
  const [sessionExpired, setSessionExpired] = useState(false);
  const [smeId, setSmeId] = useState("101");
  const [caId, setCaId] = useState("201");
  const [filingId, setFilingId] = useState("1001");
  const [amount, setAmount] = useState("45000");
  const [challanRef, setChallanRef] = useState("GST-MAR-2026-001");
  const [paymentId, setPaymentId] = useState("");
  const [transactions, setTransactions] = useState<Txn[]>([]);
  const [log, setLog] = useState("Ready");
  const [voiceSessionId, setVoiceSessionId] = useState("");
  const [isListening, setIsListening] = useState(false);
  const [liveTranscript, setLiveTranscript] = useState("");
  const [voiceCurrency, setVoiceCurrency] = useState("INR");
  const [lastVoiceEntry, setLastVoiceEntry] = useState("");
  const [networkOnline, setNetworkOnline] = useState(true);
  const [retryingRequest, setRetryingRequest] = useState(false);
  const [queuedChunks, setQueuedChunks] = useState(0);
  const [pausedBySystem, setPausedBySystem] = useState(false);
  const [sseStatus, setSseStatus] = useState("idle");
  const [lastSyncAt, setLastSyncAt] = useState("");
  const [liveEvents, setLiveEvents] = useState<RealtimeEvent[]>([]);

  const chunkQueueRef = useRef<string[]>([]);
  const flushInFlightRef = useRef(false);
  const eventSourceRef = useRef<any>(null);
  const sseReconnectRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const sseBackoffRef = useRef(1200);
  const appStateRef = useRef<AppStateStatus>(AppState.currentState);

  const headers = useMemo(
    () => ({
      "Content-Type": "application/json",
      "X-Role": "admin",
      "X-Admin-Id": "1001",
    }),
    []
  );

  const buildHeaders = useMemo(
    () => ({
      ...headers,
      ...(sessionToken ? { "X-Mobile-Session": sessionToken } : {}),
    }),
    [headers, sessionToken]
  );

  const appendLog = (line: string) => {
    setLog((prev: string) => `${new Date().toLocaleTimeString()} ${line}\n${prev}`);
  };

  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

  const clearSession = async (reason: string) => {
    setSessionToken("");
    setSessionExpired(true);
    await SecureStore.deleteItemAsync(SESSION_TOKEN_KEY);
    await SecureStore.deleteItemAsync(SESSION_CREATED_KEY);
    appendLog(reason);
  };

  const fetchWithTimeout = async (url: string, options: RequestInit): Promise<Response> => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      return await fetch(url, { ...options, signal: controller.signal });
    } finally {
      clearTimeout(timer);
    }
  };

  const parseErrorMessage = (data: unknown, status: number): string => {
    if (data && typeof data === "object" && "detail" in data) {
      const detail = (data as { detail?: unknown }).detail;
      if (typeof detail === "string") {
        return detail;
      }
    }
    return `HTTP ${status}`;
  };

  const requestJson = async (
    path: string,
    options: RequestInit,
    cfg: { requireSession?: boolean; retries?: number } = {}
  ): Promise<unknown> => {
    const retries = cfg.retries ?? 2;
    if (!networkOnline) {
      throw new Error("You are offline. Check mobile data or Wi-Fi.");
    }

    let attempt = 0;
    while (true) {
      try {
        const res = await fetchWithTimeout(`${API_BASE}${path}`, options);
        const data = await res.json().catch(() => ({}));

        if ((res.status === 401 || res.status === 403) && cfg.requireSession !== false) {
          await clearSession("Session expired. Please start mobile session again.");
          throw new Error("Session expired");
        }

        if (!res.ok) {
          const message = parseErrorMessage(data, res.status);
          const retryable = res.status >= 500 || res.status === 429;
          if (retryable && attempt < retries) {
            setRetryingRequest(true);
            await sleep(RETRY_DELAYS_MS[Math.min(attempt, RETRY_DELAYS_MS.length - 1)]);
            attempt += 1;
            continue;
          }
          throw new Error(message);
        }

        setRetryingRequest(false);
        return data;
      } catch (error) {
        if (attempt < retries) {
          setRetryingRequest(true);
          await sleep(RETRY_DELAYS_MS[Math.min(attempt, RETRY_DELAYS_MS.length - 1)]);
          attempt += 1;
          continue;
        }
        setRetryingRequest(false);
        throw error;
      }
    }
  };

  const flushChunkQueue = async () => {
    if (flushInFlightRef.current || !voiceSessionId) {
      return;
    }
    flushInFlightRef.current = true;
    try {
      while (chunkQueueRef.current.length > 0) {
        const chunk = chunkQueueRef.current[0];
        await requestJson(
          "/api/v2/mobile/voice/session/chunk",
          {
            method: "POST",
            headers: buildHeaders,
            body: JSON.stringify({
              session_id: voiceSessionId,
              chunk_text: chunk,
            }),
          },
          { retries: 3 }
        );
        chunkQueueRef.current.shift();
        setQueuedChunks(chunkQueueRef.current.length);
      }
    } catch (error) {
      appendLog(`Chunk sync paused: ${String(error)}`);
    } finally {
      flushInFlightRef.current = false;
    }
  };

  const appendLiveEvent = (event: RealtimeEvent) => {
    setLiveEvents((prev) => [event, ...prev].slice(0, 8));
    setLastSyncAt(new Date().toLocaleTimeString());
  };

  const closeSse = () => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    if (sseReconnectRef.current) {
      clearTimeout(sseReconnectRef.current);
      sseReconnectRef.current = null;
    }
  };

  const connectSse = async () => {
    const ca = Number(caId);
    if (!Number.isFinite(ca) || ca <= 0) {
      return;
    }

    closeSse();

    const EventSourceImpl = (globalThis as { EventSource?: any }).EventSource;
    if (!EventSourceImpl) {
      setSseStatus("fallback-polling");
      appendLog("EventSource unavailable on this runtime. Using manual refresh fallback.");
      return;
    }

    try {
      const tokenData = (await requestJson(
        `/api/v1/ca/events/token?ca_id=${ca}`,
        {
          method: "GET",
          headers: buildHeaders,
        },
        { requireSession: false }
      )) as { token?: string };

      if (!tokenData.token) {
        setSseStatus("token-missing");
        appendLog("Realtime token missing. SSE not started.");
        return;
      }

      const streamUrl = `${API_BASE}/api/v1/ca/events/stream?ca_id=${ca}&token=${encodeURIComponent(tokenData.token)}`;
      const source = new EventSourceImpl(streamUrl);
      eventSourceRef.current = source;
      setSseStatus("connecting");

      source.addEventListener("connected", () => {
        sseBackoffRef.current = 1200;
        setSseStatus("live");
        setLastSyncAt(new Date().toLocaleTimeString());
      });

      source.addEventListener("new_transaction", (event: { data?: string }) => {
        let summary = "New transaction received";
        let occurredAt = new Date().toISOString();
        try {
          const payload = JSON.parse(event?.data || "{}");
          summary = String(payload.summary || payload.reference || "New transaction received");
          occurredAt = String(payload.occurred_at || occurredAt);
        } catch {
          // Ignore parse failures and show generic event summary.
        }
        appendLiveEvent({
          id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
          type: "new_transaction",
          summary,
          occurred_at: occurredAt,
        });
      });

      source.addEventListener("heartbeat", () => {
        setLastSyncAt(new Date().toLocaleTimeString());
      });

      source.onerror = () => {
        setSseStatus("reconnecting");
        source.close();
        const nextBackoff = Math.min(Math.floor(sseBackoffRef.current * 1.8), SSE_MAX_BACKOFF_MS);
        const jitter = Math.floor(Math.random() * 400);
        sseBackoffRef.current = nextBackoff;
        sseReconnectRef.current = setTimeout(() => {
          void connectSse();
        }, nextBackoff + jitter);
      };
    } catch (error) {
      setSseStatus("error");
      appendLog(`SSE setup failed: ${String(error)}`);
    }
  };

  useEffect(() => {
    const unsubscribeNetInfo = NetInfo.addEventListener((state) => {
      const online = Boolean(state.isConnected && state.isInternetReachable !== false);
      setNetworkOnline(online);
      if (!online) {
        appendLog("Network offline. Actions will retry when connection returns.");
      } else {
        appendLog("Network restored.");
        if (chunkQueueRef.current.length > 0) {
          void flushChunkQueue();
        }
      }
    });

    const appStateSubscription = AppState.addEventListener("change", (nextState) => {
      const prevState = appStateRef.current;
      appStateRef.current = nextState;
      if (prevState === "active" && nextState.match(/inactive|background/)) {
        if (isListening) {
          void Voice.stop();
          setIsListening(false);
          setPausedBySystem(true);
          appendLog("Microphone paused by OS transition. Resume when active.");
        }
      }
      if (prevState.match(/inactive|background/) && nextState === "active") {
        if (pausedBySystem) {
          appendLog("App resumed. You can restart microphone safely.");
        }
        if (chunkQueueRef.current.length > 0) {
          void flushChunkQueue();
        }
        void connectSse();
      }
    });

    void (async () => {
      const storedToken = await SecureStore.getItemAsync(SESSION_TOKEN_KEY);
      if (storedToken) {
        setSessionToken(storedToken);
        appendLog("Recovered secure mobile session token.");
      }
    })();

    Voice.onSpeechResults = async (event: { value?: string[] }) => {
      const spoken = (event.value || []).join(" ").trim();
      if (!spoken) return;
      setLiveTranscript(spoken);
      if (!voiceSessionId) return;
      chunkQueueRef.current.push(spoken);
      setQueuedChunks(chunkQueueRef.current.length);
      appendLog(`Voice chunk captured (${chunkQueueRef.current.length} queued)`);
      await flushChunkQueue();
    };

    Voice.onSpeechError = (event: { error?: { message?: string } }) => {
      setIsListening(false);
      const message = event?.error?.message || "Unknown speech error";
      if (/interrupted|audio|busy|aborted/i.test(message)) {
        setPausedBySystem(true);
      }
      appendLog(`Voice error: ${message}`);
    };

    Voice.onSpeechEnd = () => {
      setIsListening(false);
      void flushChunkQueue();
    };

    return () => {
      unsubscribeNetInfo();
      appStateSubscription.remove();
      closeSse();
      void Voice.destroy();
      Voice.removeAllListeners();
    };
  }, [buildHeaders, isListening, pausedBySystem, voiceSessionId]);

  useEffect(() => {
    if (sessionToken) {
      void connectSse();
    } else {
      closeSse();
      setSseStatus("idle");
    }
    return () => closeSse();
  }, [sessionToken, caId]);

  const startSession = async () => {
    try {
      const data = (await requestJson(
        "/api/v2/mobile/auth/session",
        {
          method: "POST",
          headers,
          body: JSON.stringify({ device_id: "ios-android-tablet-01", platform: "mobile" }),
        },
        { requireSession: false }
      )) as { session_token?: string };
      const token = data.session_token || "";
      setSessionToken(token);
      setSessionExpired(false);
      await SecureStore.setItemAsync(SESSION_TOKEN_KEY, token);
      await SecureStore.setItemAsync(SESSION_CREATED_KEY, new Date().toISOString());
      appendLog(`Session active: ${token}`);
    } catch (error) {
      appendLog(`Session error: ${String(error)}`);
    }
  };

  const connectCA = async () => {
    try {
      await requestJson("/api/v2/mobile/connect-ca", {
        method: "POST",
        headers: buildHeaders,
        body: JSON.stringify({
          sme_id: Number(smeId),
          ca_id: Number(caId),
          sme_name: "Sunrise Traders",
          ca_firm_name: "Accord CA Network",
        }),
      });
      appendLog(`Linked SME ${smeId} with CA ${caId}`);
    } catch (error) {
      appendLog(`Link error: ${String(error)}`);
    }
  };

  const requestApproval = async () => {
    try {
      const data = (await requestJson("/api/v2/mobile/gst/approve", {
        method: "POST",
        headers: buildHeaders,
        body: JSON.stringify({
          filing_id: Number(filingId),
          sme_id: Number(smeId),
          approved_by_ca_id: Number(caId),
        }),
      })) as { filing_id?: number };
      appendLog(`GST approved: filing ${data.filing_id || filingId}`);
    } catch (error) {
      appendLog(`Approval error: ${String(error)}`);
    }
  };

  const createUpiIntent = async () => {
    try {
      const data = (await requestJson("/api/v2/mobile/tax/upi/intent", {
        method: "POST",
        headers: buildHeaders,
        body: JSON.stringify({
          sme_id: Number(smeId),
          filing_id: Number(filingId),
          amount_inr: Number(amount),
          challan_reference: challanRef,
        }),
      })) as { payment_id?: string };
      setPaymentId(data.payment_id || "");
      appendLog(`UPI intent created: ${data.payment_id || "unknown"}`);
    } catch (error) {
      appendLog(`UPI intent error: ${String(error)}`);
    }
  };

  const simulateWebhookPaid = async () => {
    if (!paymentId) {
      appendLog("Create intent before webhook simulation.");
      return;
    }
    try {
      const data = (await requestJson("/api/v2/mobile/tax/upi/webhook", {
        method: "POST",
        headers: buildHeaders,
        body: JSON.stringify({ payment_id: paymentId, gateway_status: "PAID", utr: "UTR-123456" }),
      })) as { ledger_state?: string };
      appendLog(`Ledger state: ${data.ledger_state || "UNKNOWN"}`);
    } catch (error) {
      appendLog(`Webhook error: ${String(error)}`);
    }
  };

  const refreshTransactions = async () => {
    try {
      const data = (await requestJson("/api/v2/mobile/transactions", {
        method: "GET",
        headers: buildHeaders,
      })) as { items?: Txn[]; count?: number };
      setTransactions(Array.isArray(data.items) ? data.items : []);
      appendLog(`Transactions synced: ${data.count || 0}`);
    } catch (error) {
      appendLog(`Txn load error: ${String(error)}`);
    }
  };

  const startVoiceSession = async () => {
    try {
      const data = (await requestJson("/api/v2/mobile/voice/session/start", {
        method: "POST",
        headers: buildHeaders,
        body: JSON.stringify({
          sme_id: Number(smeId),
          ca_id: Number(caId),
          language: "en-IN",
        }),
      })) as { session_id?: string };
      setVoiceSessionId(data.session_id || "");
      setLiveTranscript("");
      chunkQueueRef.current = [];
      setQueuedChunks(0);
      appendLog(`Voice session started: ${data.session_id || "unknown"}`);
    } catch (error) {
      appendLog(`Voice session error: ${String(error)}`);
    }
  };

  const startMic = async () => {
    if (!voiceSessionId) {
      appendLog("Start voice session first.");
      return;
    }
    try {
      await Voice.start("en-IN");
      setIsListening(true);
      setPausedBySystem(false);
      appendLog("Microphone listening started");
    } catch (error) {
      setIsListening(false);
      appendLog(`Microphone start failed: ${String(error)}`);
    }
  };

  const stopMic = async () => {
    try {
      await Voice.stop();
      setIsListening(false);
      appendLog("Microphone listening stopped");
    } catch (error) {
      appendLog(`Microphone stop failed: ${String(error)}`);
    }
  };

  const commitVoiceToLedger = async () => {
    if (!voiceSessionId) {
      appendLog("No active voice session to commit");
      return;
    }
    try {
      if (isListening) {
        await Voice.stop();
        setIsListening(false);
      }
      await flushChunkQueue();
      const data = (await requestJson("/api/v2/mobile/voice/session/commit", {
        method: "POST",
        headers: buildHeaders,
        body: JSON.stringify({
          session_id: voiceSessionId,
          currency_code: voiceCurrency,
        }),
      })) as { ledger_result?: { reference?: string; entry_id?: string } };
      const reference = data?.ledger_result?.reference || "";
      const entryId = data?.ledger_result?.entry_id || "";
      setLastVoiceEntry(`${reference} (entry ${entryId})`);
      appendLog(`Voice committed to ledger: ${reference}`);
    } catch (error) {
      appendLog(`Voice commit error: ${String(error)}`);
    }
  };

  return (
    <SafeAreaView style={styles.root}>
      <StatusBar barStyle="light-content" />
      <ScrollView contentContainerStyle={styles.container}>
        <Text style={styles.title}>Accord Network Mobile</Text>
        <Text style={styles.subtitle}>iOS + Android, phone + tablet, CA approvals and UPI tax flow</Text>

        {!networkOnline ? <Text style={styles.bannerWarn}>Offline mode: requests will retry when network returns.</Text> : null}
        {retryingRequest ? <Text style={styles.bannerInfo}>Retrying unstable network request...</Text> : null}
        {sessionExpired ? <Text style={styles.bannerWarn}>Session expired: start mobile session to continue.</Text> : null}
        {pausedBySystem ? <Text style={styles.bannerInfo}>Voice paused by OS interruption. Tap Start Mic to resume.</Text> : null}

        <View style={[styles.grid, { flexDirection: isTablet ? "row" : "column" }]}> 
          <View style={[styles.card, { width: `${100 / gridColumns}%` }]}> 
            <Text style={styles.cardTitle}>Identity</Text>
            <TextInput style={styles.input} value={smeId} onChangeText={setSmeId} placeholder="SME ID" placeholderTextColor="#8ca0c0" />
            <TextInput style={styles.input} value={caId} onChangeText={setCaId} placeholder="CA ID" placeholderTextColor="#8ca0c0" />
            <TouchableOpacity style={styles.button} onPress={startSession}><Text style={styles.buttonText}>Start Mobile Session</Text></TouchableOpacity>
            <TouchableOpacity style={styles.button} onPress={connectCA}><Text style={styles.buttonText}>Link SME to CA</Text></TouchableOpacity>
            <Text style={styles.meta}>Session: {sessionToken || "not started"}</Text>
            <Text style={styles.meta}>Realtime: {sseStatus}{lastSyncAt ? ` (last sync ${lastSyncAt})` : ""}</Text>
          </View>

          <View style={[styles.card, { width: `${100 / gridColumns}%` }]}> 
            <Text style={styles.cardTitle}>GST + UPI</Text>
            <TextInput style={styles.input} value={filingId} onChangeText={setFilingId} placeholder="Filing ID" placeholderTextColor="#8ca0c0" />
            <TextInput style={styles.input} value={amount} onChangeText={setAmount} placeholder="Amount INR" placeholderTextColor="#8ca0c0" />
            <TextInput style={styles.input} value={challanRef} onChangeText={setChallanRef} placeholder="Challan Ref" placeholderTextColor="#8ca0c0" />
            <TouchableOpacity style={styles.button} onPress={requestApproval}><Text style={styles.buttonText}>Request CA Approval</Text></TouchableOpacity>
            <TouchableOpacity style={styles.button} onPress={createUpiIntent}><Text style={styles.buttonText}>Create UPI Tax Intent</Text></TouchableOpacity>
            <TouchableOpacity style={styles.button} onPress={simulateWebhookPaid}><Text style={styles.buttonText}>Simulate UPI Paid Webhook</Text></TouchableOpacity>
            <Text style={styles.meta}>Payment: {paymentId || "none"}</Text>
          </View>
        </View>

        <View style={styles.cardFull}>
          <View style={styles.rowBetween}>
            <Text style={styles.cardTitle}>Online Transactions</Text>
            <TouchableOpacity style={styles.inlineButton} onPress={refreshTransactions}><Text style={styles.inlineButtonText}>Refresh</Text></TouchableOpacity>
          </View>
          {transactions.map((txn: Txn) => (
            <View key={txn.payment_id} style={styles.txnRow}>
              <Text style={styles.txnText}>{txn.payment_id}</Text>
              <Text style={styles.txnText}>{txn.status}</Text>
              <Text style={styles.txnText}>INR {txn.amount_inr}</Text>
            </View>
          ))}
          {transactions.length === 0 ? <Text style={styles.meta}>No transactions yet.</Text> : null}
        </View>

        <View style={styles.cardFull}>
          <Text style={styles.cardTitle}>Voice To Ledger</Text>
          <View style={styles.rowBetween}>
            <TouchableOpacity style={styles.inlineButton} onPress={startVoiceSession}>
              <Text style={styles.inlineButtonText}>Start Voice Session</Text>
            </TouchableOpacity>
            <Text style={styles.meta}>{voiceSessionId ? "Session Active" : "No Session"}</Text>
          </View>

          <TextInput
            style={styles.input}
            value={voiceCurrency}
            onChangeText={setVoiceCurrency}
            placeholder="Voice Currency (INR/USD)"
            placeholderTextColor="#8ca0c0"
          />

          <View style={styles.rowBetween}>
            <TouchableOpacity
              style={[styles.button, isListening ? styles.buttonWarn : null]}
              onPress={isListening ? stopMic : startMic}
            >
              <Text style={styles.buttonText}>{isListening ? "Stop Mic" : "Start Mic"}</Text>
            </TouchableOpacity>
            <TouchableOpacity style={styles.button} onPress={commitVoiceToLedger}>
              <Text style={styles.buttonText}>Commit To Ledger</Text>
            </TouchableOpacity>
          </View>

          <Text style={styles.meta}>Live transcript: {liveTranscript || "(waiting for speech)"}</Text>
          <Text style={styles.meta}>Last voice posting: {lastVoiceEntry || "none"}</Text>
          <Text style={styles.meta}>Queued voice chunks: {queuedChunks}</Text>
          <TouchableOpacity style={styles.inlineButton} onPress={flushChunkQueue}>
            <Text style={styles.inlineButtonText}>Retry Pending Chunks</Text>
          </TouchableOpacity>
        </View>

        <View style={styles.cardFull}>
          <View style={styles.rowBetween}>
            <Text style={styles.cardTitle}>Realtime Sync Monitor</Text>
            <TouchableOpacity style={styles.inlineButton} onPress={connectSse}>
              <Text style={styles.inlineButtonText}>Reconnect SSE</Text>
            </TouchableOpacity>
          </View>
          <Text style={styles.meta}>Status: {sseStatus}</Text>
          <Text style={styles.meta}>Last sync: {lastSyncAt || "never"}</Text>
          {liveEvents.length === 0 ? <Text style={styles.meta}>No live events yet.</Text> : null}
          {liveEvents.map((event) => (
            <View key={event.id} style={styles.eventRow}>
              <Text style={styles.eventType}>{event.type}</Text>
              <Text style={styles.eventSummary}>{event.summary}</Text>
              <Text style={styles.eventTime}>{event.occurred_at}</Text>
            </View>
          ))}
        </View>

        <View style={styles.cardFull}>
          <Text style={styles.cardTitle}>Activity Log</Text>
          <Text style={styles.log}>{log}</Text>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: "#0b1220" },
  container: { padding: 16, gap: 12 },
  title: { color: "#e8f0ff", fontSize: 28, fontWeight: "800" },
  subtitle: { color: "#9fb2ce", fontSize: 13, marginBottom: 8 },
  grid: { gap: 10 },
  card: {
    backgroundColor: "#131d30",
    borderColor: "#2b3c5d",
    borderWidth: 1,
    borderRadius: 12,
    padding: 12,
    gap: 8,
  },
  cardFull: {
    backgroundColor: "#131d30",
    borderColor: "#2b3c5d",
    borderWidth: 1,
    borderRadius: 12,
    padding: 12,
    gap: 8,
  },
  cardTitle: { color: "#dbe6fa", fontWeight: "700", fontSize: 16 },
  input: {
    backgroundColor: "#0c1527",
    borderColor: "#31486f",
    borderWidth: 1,
    borderRadius: 8,
    color: "#e3ecff",
    paddingHorizontal: 10,
    paddingVertical: 8,
  },
  button: {
    backgroundColor: "#2d6cdf",
    borderRadius: 8,
    paddingVertical: 10,
    paddingHorizontal: 12,
    alignItems: "center",
    minWidth: 130,
  },
  buttonWarn: {
    backgroundColor: "#b45309",
  },
  buttonText: { color: "#ffffff", fontWeight: "700" },
  meta: { color: "#9fb2ce", fontSize: 12 },
  rowBetween: { flexDirection: "row", justifyContent: "space-between", alignItems: "center" },
  inlineButton: {
    borderColor: "#3e5f92",
    borderWidth: 1,
    borderRadius: 6,
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  inlineButtonText: { color: "#b9cef1", fontWeight: "700", fontSize: 12 },
  txnRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    borderBottomColor: "#273954",
    borderBottomWidth: 1,
    paddingVertical: 6,
  },
  txnText: { color: "#d2dff8", fontSize: 12 },
  log: { color: "#99afcf", fontSize: 11, lineHeight: 16 },
  bannerWarn: {
    backgroundColor: "#5c1d0e",
    borderColor: "#9a3412",
    borderWidth: 1,
    borderRadius: 8,
    color: "#fed7aa",
    paddingHorizontal: 10,
    paddingVertical: 8,
    fontSize: 12,
  },
  bannerInfo: {
    backgroundColor: "#172554",
    borderColor: "#1d4ed8",
    borderWidth: 1,
    borderRadius: 8,
    color: "#bfdbfe",
    paddingHorizontal: 10,
    paddingVertical: 8,
    fontSize: 12,
  },
  eventRow: {
    borderBottomColor: "#273954",
    borderBottomWidth: 1,
    paddingVertical: 6,
    gap: 2,
  },
  eventType: {
    color: "#bfdbfe",
    fontWeight: "700",
    fontSize: 11,
    textTransform: "uppercase",
  },
  eventSummary: {
    color: "#d2dff8",
    fontSize: 12,
  },
  eventTime: {
    color: "#9fb2ce",
    fontSize: 11,
  },
});
