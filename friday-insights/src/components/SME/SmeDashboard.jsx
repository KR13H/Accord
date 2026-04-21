import { useEffect, useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

function formatCurrency(value) {
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2,
  }).format(value || 0);
}

function shortDateLabel(isoDate) {
  const parsed = new Date(`${isoDate}T00:00:00Z`);
  return parsed.toLocaleDateString("en-IN", { month: "short", day: "numeric" });
}

export default function SmeDashboard({ syncEvent }) {
  const [weeklyRows, setWeeklyRows] = useState([]);
  const [todayRevenue, setTodayRevenue] = useState(0);
  const [todayTransactions, setTodayTransactions] = useState(0);
  const [predictions, setPredictions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const smeRole = useMemo(() => {
    const role = window.localStorage.getItem("smeRole");
    return role && role.trim() ? role.trim().toLowerCase() : "owner";
  }, []);

  useEffect(() => {
    let mounted = true;

    async function loadWeeklySummary() {
      setLoading(true);
      setError("");
      try {
        const dates = Array.from({ length: 7 }, (_, index) => {
          const d = new Date();
          d.setUTCDate(d.getUTCDate() - (6 - index));
          return d.toISOString().slice(0, 10);
        });

        const responses = await Promise.all(
          dates.map((day) =>
            fetch(`/api/v1/sme/summary?target_date=${encodeURIComponent(day)}`, {
              headers: { "X-SME-Role": smeRole },
            })
          )
        );

        const payloads = await Promise.all(responses.map((res) => res.json()));
        const rows = payloads.map((payload, index) => {
          const summary = payload?.summary || {};
          return {
            date: dates[index],
            label: shortDateLabel(dates[index]),
            income: Number.parseFloat(summary.income_total || "0") || 0,
            expense: Number.parseFloat(summary.expense_total || "0") || 0,
            transactions: Number.parseInt(summary.transaction_count || 0, 10) || 0,
          };
        });

        if (!mounted) return;
        setWeeklyRows(rows);

        const today = rows[rows.length - 1] || { income: 0, transactions: 0 };
        setTodayRevenue(today.income);
        setTodayTransactions(today.transactions);

        const predictionRes = await fetch("/api/v1/sme/inventory/restock-predictions");
        const predictionPayload = await predictionRes.json();
        setPredictions(Array.isArray(predictionPayload?.predictions) ? predictionPayload.predictions : []);
      } catch (err) {
        if (!mounted) return;
        setError(err?.message || "Unable to load SME dashboard");
      } finally {
        if (mounted) {
          setLoading(false);
        }
      }
    }

    loadWeeklySummary();
    return () => {
      mounted = false;
    };
  }, [smeRole]);

  useEffect(() => {
    if (!syncEvent) {
      return;
    }
    if (syncEvent.event !== "TRANSACTION_RECORDED" && syncEvent.event !== "INVENTORY_UPDATED") {
      return;
    }

    let cancelled = false;
      const refresh = async () => {
      try {
        const todayIso = new Date().toISOString().slice(0, 10);
        const response = await fetch(`/api/v1/sme/summary?target_date=${encodeURIComponent(todayIso)}`, {
          headers: { "X-SME-Role": smeRole },
        });
        const payload = await response.json();
        if (cancelled) {
          return;
        }
        const summary = payload?.summary || {};
        setTodayRevenue(Number.parseFloat(summary.income_total || "0") || 0);
        setTodayTransactions(Number.parseInt(summary.transaction_count || 0, 10) || 0);

        const predictionRes = await fetch("/api/v1/sme/inventory/restock-predictions");
        const predictionPayload = await predictionRes.json();
        if (!cancelled) {
          setPredictions(Array.isArray(predictionPayload?.predictions) ? predictionPayload.predictions : []);
        }
      } catch {
        // Keep stale values if refresh fails.
      }
    };

    refresh();
    return () => {
      cancelled = true;
    };
  }, [syncEvent, smeRole]);

  const totalWeekRevenue = useMemo(() => weeklyRows.reduce((sum, row) => sum + row.income, 0), [weeklyRows]);

  return (
    <div className="min-h-screen bg-slate-100 text-slate-900 dark:bg-slate-950 dark:text-white px-4 py-6 md:px-8 md:py-10">
      <div className="mx-auto max-w-6xl">
        <div className="mb-6">
          <p className="text-xs uppercase tracking-[0.18em] text-cyan-700 dark:text-cyan-300">SME Command Center</p>
          <h1 className="text-3xl md:text-4xl font-black mt-2">Sales Analytics Dashboard</h1>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 md:gap-5 mb-6">
          <div className="rounded-3xl border border-emerald-300 bg-emerald-100/80 dark:border-emerald-300/30 dark:bg-emerald-500/10 p-5 md:p-6">
            <p className="text-sm text-emerald-800 dark:text-emerald-100/90">Today&apos;s Revenue</p>
            <p className="text-3xl md:text-5xl font-black mt-2">{formatCurrency(todayRevenue)}</p>
          </div>
          <div className="rounded-3xl border border-cyan-300 bg-cyan-100/80 dark:border-cyan-300/30 dark:bg-cyan-500/10 p-5 md:p-6">
            <p className="text-sm text-cyan-800 dark:text-cyan-100/90">Total Transactions</p>
            <p className="text-3xl md:text-5xl font-black mt-2">{todayTransactions}</p>
          </div>
          <div className="rounded-3xl border border-indigo-300 bg-indigo-100/80 dark:border-indigo-300/30 dark:bg-indigo-500/10 p-5 md:p-6">
            <p className="text-sm text-indigo-800 dark:text-indigo-100/90">Last 7 Days Revenue</p>
            <p className="text-3xl md:text-5xl font-black mt-2">{formatCurrency(totalWeekRevenue)}</p>
          </div>
        </div>

        <div className="rounded-3xl border border-slate-300 bg-white dark:border-slate-700 dark:bg-slate-900/80 p-4 md:p-6">
          <div className="h-[380px] md:h-[420px] w-full">
            {loading ? (
              <div className="h-full flex items-center justify-center text-slate-300">Loading weekly chart...</div>
            ) : error ? (
              <div className="h-full flex items-center justify-center text-rose-300">{error}</div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={weeklyRows} barGap={8} barSize={24}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.25)" />
                  <XAxis dataKey="label" stroke="#cbd5e1" />
                  <YAxis stroke="#cbd5e1" />
                  <Tooltip
                    formatter={(value) => formatCurrency(Number(value || 0))}
                    contentStyle={{ backgroundColor: "#020617", border: "1px solid #334155", borderRadius: 12 }}
                  />
                  <Legend />
                  <Bar dataKey="income" name="Income" fill="#22c55e" radius={[8, 8, 0, 0]} />
                  <Bar dataKey="expense" name="Expense" fill="#f97316" radius={[8, 8, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>

        <div className="rounded-3xl border border-slate-300 bg-white dark:border-slate-700 dark:bg-slate-900/80 p-4 md:p-6 mt-6">
          <h2 className="text-xl md:text-2xl font-black text-cyan-700 dark:text-cyan-100 mb-3">Predictive Restock (Next 7 Days)</h2>
          {predictions.length === 0 ? (
            <p className="text-slate-600 dark:text-slate-300 text-sm">No prediction data yet. Record a few days of sales to unlock restock AI.</p>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {predictions.slice(0, 8).map((prediction, index) => (
                <div key={`${prediction.item_name}-${index}`} className="rounded-2xl border border-cyan-300 bg-cyan-100/70 dark:border-cyan-400/25 dark:bg-cyan-500/10 p-4">
                  <p className="text-cyan-900 dark:text-cyan-100 font-semibold">{prediction.item_name}</p>
                  <p className="text-2xl font-black mt-1">Order {prediction.predicted_order_qty}</p>
                  <p className="text-xs text-cyan-900/80 dark:text-cyan-50/80 mt-2">{prediction.justification}</p>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
