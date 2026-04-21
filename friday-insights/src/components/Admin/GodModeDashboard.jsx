import { useEffect, useMemo, useState } from "react";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { getStoredSmeAccessToken, getStoredSmeUsername } from "../../api/smeAuth";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "";

function formatCurrency(value) {
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2,
  }).format(Number(value || 0));
}

export default function GodModeDashboard() {
  const [payload, setPayload] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [pendingFlag, setPendingFlag] = useState("");

  const token = getStoredSmeAccessToken();
  const actor = getStoredSmeUsername() || "unknown";

  const dailyGrowth = useMemo(() => {
    if (!Array.isArray(payload?.daily_growth)) return [];
    return payload.daily_growth.map((row) => ({
      ...row,
      label: new Date(`${row.date}T00:00:00Z`).toLocaleDateString("en-IN", { month: "short", day: "numeric" }),
    }));
  }, [payload]);

  const loadMetrics = async () => {
    if (!token) {
      setError("Missing access token. Login with passkey first.");
      setLoading(false);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const response = await fetch(`${API_BASE}/api/v1/admin/platform-metrics`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.detail || `Failed to load metrics (${response.status})`);
      }
      setPayload(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load platform metrics");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadMetrics();
  }, []);

  const toggleFlag = async (flagName, enabled) => {
    if (!token) return;
    setPendingFlag(flagName);
    try {
      const response = await fetch(`${API_BASE}/api/v1/admin/feature-flags/${encodeURIComponent(flagName)}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ enabled: !enabled }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.detail || `Failed to update ${flagName}`);
      }
      await loadMetrics();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to update feature flag");
    } finally {
      setPendingFlag("");
    }
  };

  if (loading) {
    return <div className="min-h-screen px-6 py-10 text-slate-700 dark:text-slate-200">Loading God-Mode metrics...</div>;
  }

  return (
    <div className="min-h-screen px-4 py-6 md:px-8 md:py-10 bg-slate-100 text-slate-900 dark:bg-slate-950 dark:text-white">
      <div className="mx-auto max-w-7xl space-y-6">
        <div className="rounded-3xl border border-slate-300 bg-white/90 p-6 dark:border-cyan-400/25 dark:bg-slate-900/80">
          <p className="text-xs uppercase tracking-[0.18em] text-amber-600 dark:text-cyan-300">Accord Super-Admin</p>
          <h1 className="mt-2 text-3xl md:text-4xl font-black">God-Mode Command Center</h1>
          <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">Operator: {actor}</p>
          {error ? <p className="mt-3 text-sm text-rose-500 dark:text-rose-300">{error}</p> : null}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="rounded-2xl border border-emerald-300 bg-emerald-100/70 p-5 dark:border-emerald-400/30 dark:bg-emerald-500/10">
            <p className="text-xs uppercase tracking-wider text-emerald-700 dark:text-emerald-200">Active SMEs</p>
            <p className="mt-2 text-4xl font-black">{payload?.metrics?.active_smes ?? 0}</p>
          </div>
          <div className="rounded-2xl border border-cyan-300 bg-cyan-100/70 p-5 dark:border-cyan-400/30 dark:bg-cyan-500/10">
            <p className="text-xs uppercase tracking-wider text-cyan-700 dark:text-cyan-200">Platform GMV</p>
            <p className="mt-2 text-4xl font-black">{formatCurrency(payload?.metrics?.total_gmv || 0)}</p>
          </div>
          <div className="rounded-2xl border border-violet-300 bg-violet-100/70 p-5 dark:border-violet-400/30 dark:bg-violet-500/10">
            <p className="text-xs uppercase tracking-wider text-violet-700 dark:text-violet-200">Active Subscriptions</p>
            <p className="mt-2 text-4xl font-black">{payload?.metrics?.active_subscriptions ?? 0}</p>
          </div>
        </div>

        <div className="rounded-3xl border border-slate-300 bg-white p-5 dark:border-slate-700 dark:bg-slate-900/80">
          <h2 className="text-xl font-black mb-4">30-Day Growth Trend</h2>
          <div className="h-[360px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={dailyGrowth}>
                <defs>
                  <linearGradient id="gmvGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.6} />
                    <stop offset="95%" stopColor="#06b6d4" stopOpacity={0.05} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(100,116,139,0.25)" />
                <XAxis dataKey="label" stroke="#64748b" />
                <YAxis stroke="#64748b" />
                <Tooltip
                  formatter={(value, name) => {
                    if (name === "gmv") return [formatCurrency(value), "GMV"];
                    return [value, name];
                  }}
                  contentStyle={{ borderRadius: 12, border: "1px solid #334155" }}
                />
                <Area type="monotone" dataKey="gmv" stroke="#06b6d4" fill="url(#gmvGradient)" strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="rounded-3xl border border-slate-300 bg-white p-5 dark:border-slate-700 dark:bg-slate-900/80">
          <h2 className="text-xl font-black mb-3">Emergency Feature Flags</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {Object.entries(payload?.feature_flags || {}).map(([flag, enabled]) => (
              <button
                type="button"
                key={flag}
                onClick={() => void toggleFlag(flag, Boolean(enabled))}
                disabled={pendingFlag === flag}
                className="rounded-2xl border border-slate-300 bg-slate-50 px-4 py-3 text-left dark:border-slate-700 dark:bg-slate-950"
              >
                <p className="font-semibold">{flag}</p>
                <p className={`text-sm mt-1 ${enabled ? "text-emerald-600 dark:text-emerald-300" : "text-rose-600 dark:text-rose-300"}`}>
                  {enabled ? "Enabled" : "Maintenance Mode"}
                </p>
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
