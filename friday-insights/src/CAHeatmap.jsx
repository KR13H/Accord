import { useEffect, useMemo, useState } from "react";
import { AlertTriangle, Activity, ShieldCheck, Loader2 } from "lucide-react";

function formatDateTime(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString("en-IN", { dateStyle: "medium", timeStyle: "short" });
}

function levelClass(level) {
  const normalized = String(level || "MEDIUM").toUpperCase();
  if (normalized === "CRITICAL") return "text-rose-200 bg-rose-500/20 border-rose-400/40";
  if (normalized === "HIGH") return "text-orange-200 bg-orange-500/20 border-orange-400/40";
  if (normalized === "MEDIUM") return "text-amber-200 bg-amber-500/20 border-amber-400/40";
  return "text-emerald-200 bg-emerald-500/20 border-emerald-400/40";
}

export default function CAHeatmap() {
  const currentCaId = 201;
  const [payload, setPayload] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [alerts, setAlerts] = useState([]);
  const [alertsLoading, setAlertsLoading] = useState(false);
  const [alertsError, setAlertsError] = useState("");
  const [alertsActionMsg, setAlertsActionMsg] = useState("");
  const [liveEvents, setLiveEvents] = useState([]);
  const [liveStatus, setLiveStatus] = useState("connecting");

  const loadHeatmap = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await fetch("/api/v1/ca/heatmap?limit=160", {
        headers: {
          "X-Role": "ca",
          "X-Admin-Id": "1001",
        },
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to load CA heatmap (${res.status})`);
      }
      setPayload(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load CA heatmap");
      setPayload(null);
    } finally {
      setLoading(false);
    }
  };

  const loadAlerts = async () => {
    setAlertsLoading(true);
    setAlertsError("");
    try {
      const res = await fetch("/api/v1/ca/alerts?status=OPEN&limit=200", {
        headers: {
          "X-Role": "ca",
          "X-Admin-Id": "1001",
        },
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to load CA alerts (${res.status})`);
      }
      setAlerts(Array.isArray(data?.alerts) ? data.alerts : []);
    } catch (err) {
      setAlertsError(err instanceof Error ? err.message : "Unable to load CA alerts");
      setAlerts([]);
    } finally {
      setAlertsLoading(false);
    }
  };

  const evaluateAlerts = async () => {
    setAlertsActionMsg("");
    setAlertsError("");
    setAlertsLoading(true);
    try {
      const res = await fetch("/api/v1/ca/alerts/evaluate?limit=160", {
        method: "POST",
        headers: {
          "X-Role": "ca",
          "X-Admin-Id": "1001",
        },
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Alert evaluation failed (${res.status})`);
      }
      setAlertsActionMsg(`Evaluated ${data?.evaluated_clients ?? 0} clients, created ${data?.created_alerts ?? 0} new alerts.`);
      await loadAlerts();
    } catch (err) {
      setAlertsError(err instanceof Error ? err.message : "Alert evaluation failed");
      setAlertsLoading(false);
    }
  };

  const createManualAlert = async (row) => {
    setAlertsActionMsg("");
    setAlertsError("");
    try {
      const res = await fetch("/api/v1/ca/alerts/manual", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Role": "ca",
          "X-Admin-Id": "1001",
        },
        body: JSON.stringify({
          gstin: row.gstin,
          vendor_name: row.vendor_name,
          risk_level: row.risk_level,
          title: `Manual escalation: ${row.vendor_name}`,
          message: `CA flagged ${row.vendor_name} (${row.gstin}) at ${row.risk_level}. ITC risk INR ${row.total_itc_at_risk}.`,
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to create manual alert (${res.status})`);
      }
      setAlertsActionMsg(`Manual alert created for ${row.vendor_name}.`);
      await loadAlerts();
    } catch (err) {
      setAlertsError(err instanceof Error ? err.message : "Unable to create manual alert");
    }
  };

  const acknowledgeAlert = async (alertId) => {
    setAlertsActionMsg("");
    setAlertsError("");
    try {
      const res = await fetch(`/api/v1/ca/alerts/${alertId}/ack`, {
        method: "POST",
        headers: {
          "X-Role": "ca",
          "X-Admin-Id": "1001",
        },
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to acknowledge alert (${res.status})`);
      }
      setAlertsActionMsg(`Alert #${alertId} acknowledged.`);
      await loadAlerts();
    } catch (err) {
      setAlertsError(err instanceof Error ? err.message : "Unable to acknowledge alert");
    }
  };

  const resolveAlert = async (alertId) => {
    setAlertsActionMsg("");
    setAlertsError("");
    try {
      const res = await fetch("/api/v1/ca/playbooks/execute", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Role": "ca",
          "X-Admin-Id": "1001",
        },
        body: JSON.stringify({
          alert_id: alertId,
          hold_hours: 72,
          playbook_key: "ALERT_REMEDIATION_V1",
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to execute playbook (${res.status})`);
      }
      setAlertsActionMsg(
        `Resolved alert #${alertId}. Hold applied until ${formatDateTime(data?.payment_hold?.hold_until || "")}.`
      );
      await Promise.all([loadAlerts(), loadHeatmap()]);
    } catch (err) {
      setAlertsError(err instanceof Error ? err.message : "Unable to execute remediation playbook");
    }
  };

  useEffect(() => {
    void loadHeatmap();
    void loadAlerts();
  }, []);

  useEffect(() => {
    let source = null;
    let disposed = false;

    const onConnected = () => {
      setLiveStatus("connected");
    };

    const onTransaction = (event) => {
      try {
        const data = JSON.parse(event.data || "{}");
        setLiveEvents((prev) => [data, ...prev].slice(0, 20));
        setAlertsActionMsg(`Live update: ${data.summary || "New transaction"}`);
        void Promise.all([loadHeatmap(), loadAlerts()]);
      } catch {
        setAlertsError("Failed to parse live transaction event");
      }
    };

    const bootstrapSSE = async () => {
      try {
        const tokenRes = await fetch(`/api/v1/ca/events/token?ca_id=${currentCaId}`, {
          headers: {
            "X-Role": "ca",
            "X-Admin-Id": String(currentCaId),
          },
        });
        const tokenData = await tokenRes.json();
        if (!tokenRes.ok || !tokenData?.token) {
          throw new Error(tokenData?.detail || `Token fetch failed (${tokenRes.status})`);
        }
        if (disposed) return;
        const streamUrl = `/api/v1/ca/events/stream?ca_id=${currentCaId}&token=${encodeURIComponent(tokenData.token)}`;
        source = new EventSource(streamUrl);
        source.addEventListener("connected", onConnected);
        source.addEventListener("new_transaction", onTransaction);
        source.addEventListener("heartbeat", onConnected);
        source.onerror = () => {
          setLiveStatus("reconnecting");
        };
      } catch (err) {
        setLiveStatus("error");
        setAlertsError(err instanceof Error ? err.message : "Failed to bootstrap live stream");
      }
    };

    void bootstrapSSE();

    return () => {
      disposed = true;
      if (source) {
        source.removeEventListener("connected", onConnected);
        source.removeEventListener("new_transaction", onTransaction);
        source.removeEventListener("heartbeat", onConnected);
        source.close();
      }
      setLiveStatus("disconnected");
    };
  }, []);

  const cells = useMemo(() => (Array.isArray(payload?.cells) ? payload.cells : []), [payload]);

  if (loading) {
    return (
      <div className="min-h-screen bg-[#020617] text-slate-100 p-8 flex items-center justify-center gap-3">
        <Loader2 className="w-5 h-5 animate-spin" />
        <span className="text-sm tracking-wide">Loading CA multi-client heatmap...</span>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#020617] text-slate-100 p-8">
      <header className="mb-8 flex flex-col gap-2">
        <p className="text-xs tracking-[0.2em] text-cyan-300 uppercase font-mono">Tier-3 Revenue Console</p>
        <h1 className="text-3xl font-bold">CA Multi-Client Heatmap</h1>
        <p className="text-sm text-slate-400">
          AI-assisted compliance radar for 100+ clients using trust scores, ITC exposure, filing delay behavior, and market volatility.
        </p>
      </header>

      {error ? (
        <div className="mb-6 rounded-xl border border-rose-600/40 bg-rose-900/20 p-3 text-sm text-rose-200">{error}</div>
      ) : null}

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4 mb-6">
        <div className="rounded-xl border border-slate-800 bg-slate-950/80 p-4">
          <p className="text-[11px] uppercase tracking-[0.14em] text-slate-400">Total Clients</p>
          <p className="mt-1 text-2xl font-bold text-cyan-200">{payload?.count ?? 0}</p>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-950/80 p-4">
          <p className="text-[11px] uppercase tracking-[0.14em] text-slate-400">Aggregate Risk</p>
          <p className="mt-1 text-2xl font-bold text-orange-200">{String(payload?.aggregate_risk || "-")}</p>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-950/80 p-4">
          <p className="text-[11px] uppercase tracking-[0.14em] text-slate-400">Critical Clients</p>
          <p className="mt-1 text-2xl font-bold text-rose-300">{payload?.risk_buckets?.CRITICAL ?? 0}</p>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-950/80 p-4">
          <p className="text-[11px] uppercase tracking-[0.14em] text-slate-400">Market Context</p>
          <p className="mt-1 text-2xl font-bold text-amber-200">{String(payload?.market_context?.risk_level || "-")}</p>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-950/80 p-4">
          <p className="text-[11px] uppercase tracking-[0.14em] text-slate-400">Open Alerts</p>
          <p className="mt-1 text-2xl font-bold text-indigo-200">{Number(payload?.open_alerts_total || 0)}</p>
        </div>
      </div>

      <div className="rounded-2xl border border-cyan-800/40 bg-black/45 p-4 mb-6">
        <div className="flex flex-wrap items-center gap-2 text-xs text-slate-300">
          <ShieldCheck className="w-4 h-4 text-cyan-300" />
          <span>Latest market run: {formatDateTime(payload?.market_context?.created_at)}</span>
          <span className="px-2 py-0.5 rounded-md border border-cyan-600/50 bg-cyan-900/25 text-cyan-100">
            {payload?.market_context?.source_kind || "N/A"}
          </span>
        </div>
        <p className="mt-3 text-sm text-slate-200">{payload?.market_context?.trend_summary || "No market intelligence summary yet."}</p>
      </div>

      <div className="rounded-2xl border border-indigo-800/40 bg-indigo-950/20 p-4 mb-6">
        <div className="flex flex-wrap items-center justify-between gap-3 mb-3">
          <h2 className="text-lg font-semibold text-indigo-100">CA Alerts</h2>
          <div className="flex items-center gap-2">
            <button
              onClick={() => {
                void evaluateAlerts();
              }}
              disabled={alertsLoading}
              className="rounded-lg border border-indigo-500/60 bg-indigo-900/40 px-3 py-1.5 text-xs font-semibold text-indigo-100 disabled:opacity-60"
            >
              Evaluate Rules
            </button>
            <button
              onClick={() => {
                void loadAlerts();
              }}
              disabled={alertsLoading}
              className="rounded-lg border border-slate-600 bg-slate-900/70 px-3 py-1.5 text-xs font-semibold text-slate-200 disabled:opacity-60"
            >
              Refresh Alerts
            </button>
          </div>
        </div>
        <div className="mb-3 rounded-md border border-cyan-700/30 bg-cyan-900/20 px-3 py-2">
          <p className="text-[11px] uppercase tracking-[0.16em] text-cyan-200">Live Feed Status</p>
          <p className="text-xs text-cyan-100 mt-1">{String(liveStatus).toUpperCase()}</p>
        </div>
        {alertsActionMsg ? <p className="mb-2 text-xs text-emerald-300">{alertsActionMsg}</p> : null}
        {alertsError ? <p className="mb-2 text-xs text-rose-300">{alertsError}</p> : null}
        {alertsLoading ? <p className="text-xs text-slate-400">Loading alerts...</p> : null}
        {!alertsLoading && !alerts.length ? <p className="text-xs text-slate-400">No open alerts at this time.</p> : null}
        {!!alerts.length ? (
          <div className="space-y-2">
            {alerts.slice(0, 8).map((alert) => (
              <div key={`alert-${alert.id}`} className="rounded-lg border border-indigo-800/40 bg-slate-950/70 p-2.5">
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <div>
                    <p className="text-sm text-indigo-100 font-medium">{alert.title}</p>
                    <p className="text-xs text-slate-400">{alert.vendor_name} • {alert.gstin} • {alert.risk_level}</p>
                    <p className="text-xs text-slate-300 mt-1">{alert.message}</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => {
                        void resolveAlert(alert.id);
                      }}
                      className="rounded-md border border-cyan-700/60 bg-cyan-900/30 px-2 py-1 text-[11px] font-semibold text-cyan-200"
                    >
                      Resolve
                    </button>
                    <button
                      onClick={() => {
                        void acknowledgeAlert(alert.id);
                      }}
                      className="rounded-md border border-emerald-700/60 bg-emerald-900/30 px-2 py-1 text-[11px] font-semibold text-emerald-200"
                    >
                      Acknowledge
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : null}

        {!!liveEvents.length ? (
          <div className="mt-4">
            <p className="text-[11px] uppercase tracking-[0.16em] text-cyan-200 mb-2">Live Transactions</p>
            <div className="space-y-2">
              {liveEvents.slice(0, 5).map((evt, idx) => (
                <div key={`live-${evt.entry_id || idx}-${evt.reference || "txn"}`} className="rounded-lg border border-cyan-700/30 bg-cyan-950/20 p-2">
                  <p className="text-xs text-cyan-100 font-medium">{evt.summary || "New transaction"}</p>
                  <p className="text-[11px] text-slate-300 mt-1">
                    SME {evt.sme_id || "-"} • Ref {evt.reference || "-"} • Entry {evt.entry_id || "-"}
                  </p>
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </div>

      <div className="rounded-2xl border border-slate-800 bg-slate-950/60 p-4 overflow-x-auto">
        <table className="w-full min-w-[1260px] text-left text-sm">
          <thead className="border-b border-slate-800 text-[11px] uppercase tracking-[0.12em] text-slate-500">
            <tr>
              <th className="pb-3">Vendor</th>
              <th className="pb-3">GSTIN</th>
              <th className="pb-3">Risk</th>
              <th className="pb-3">Alerts</th>
              <th className="pb-3">Trust</th>
              <th className="pb-3">ITC At Risk</th>
              <th className="pb-3">Delay Days</th>
              <th className="pb-3">Entry Count</th>
              <th className="pb-3">Payment Advice</th>
              <th className="pb-3">Risk Reasons</th>
              <th className="pb-3">Last Activity</th>
              <th className="pb-3">Action</th>
            </tr>
          </thead>
          <tbody>
            {cells.map((row) => (
              <tr key={`heat-${row.gstin}`} className="border-b border-slate-800/60 align-top">
                <td className="py-4 pr-3 font-medium text-slate-100">{row.vendor_name}</td>
                <td className="py-4 pr-3 font-mono text-cyan-200">{row.gstin}</td>
                <td className="py-4 pr-3">
                  <span className={`inline-flex items-center gap-1 rounded-lg border px-2 py-1 text-[11px] font-semibold ${levelClass(row.risk_level)}`}>
                    <AlertTriangle className="w-3 h-3" />
                    {row.risk_level}
                  </span>
                </td>
                <td className="py-4 pr-3">
                  <span className="inline-flex items-center gap-1 rounded-md border border-indigo-700/60 bg-indigo-900/30 px-2 py-1 text-[11px] font-semibold text-indigo-200">
                    {Number(row.open_alert_count || 0)}
                  </span>
                </td>
                <td className="py-4 pr-3 font-semibold text-cyan-200">{Number(row.trust_score || 0).toFixed(2)}%</td>
                <td className="py-4 pr-3 text-orange-200">INR {row.total_itc_at_risk}</td>
                <td className="py-4 pr-3">{row.avg_filing_delay_days}</td>
                <td className="py-4 pr-3">{row.total_entries}</td>
                <td className="py-4 pr-3">
                  <span className="inline-flex items-center gap-1 rounded-md border border-slate-700 bg-slate-900/70 px-2 py-1 text-[11px] text-slate-200">
                    <Activity className="w-3 h-3" />
                    {row?.payment_advice?.advice || "N/A"}
                  </span>
                </td>
                <td className="py-4 pr-3 text-xs text-slate-300 max-w-[340px]">
                  {(Array.isArray(row.risk_reasons) ? row.risk_reasons : []).join(" | ")}
                </td>
                <td className="py-4 pr-3 text-slate-400">{formatDateTime(row.last_activity)}</td>
                <td className="py-4 pr-3">
                  <button
                    onClick={() => {
                      void createManualAlert(row);
                    }}
                    className="rounded-md border border-rose-700/60 bg-rose-900/25 px-2 py-1 text-[11px] font-semibold text-rose-200"
                  >
                    Create Alert
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {!cells.length ? <p className="py-4 text-sm text-slate-400">No client heatmap rows available yet.</p> : null}
      </div>

      <div className="mt-4 text-xs text-slate-500">
        Generated at: {formatDateTime(payload?.generated_at)}
      </div>
    </div>
  );
}
