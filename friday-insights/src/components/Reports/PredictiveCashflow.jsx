import { useEffect, useMemo, useState } from "react";
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import apiClient from "../../api/client";

function money(value) {
  return Number(value || 0).toLocaleString("en-IN", { style: "currency", currency: "INR", maximumFractionDigits: 0 });
}

export default function PredictiveCashflow() {
  const [rows, setRows] = useState([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;

    async function load() {
      setLoading(true);
      setError("");
      try {
        const res = await apiClient.get("/cashflow/predictive", { params: { months: 6 } });
        const historical = Array.isArray(res.data?.historical) ? res.data.historical : [];
        const projection = Array.isArray(res.data?.projection) ? res.data.projection : [];

        const monthMap = new Map();
        historical.forEach((row) => {
          monthMap.set(row.month, {
            month: row.month,
            historical: Number(row.rera_inflow || 0),
            projected: null,
          });
        });
        projection.forEach((row) => {
          const existing = monthMap.get(row.month) || {
            month: row.month,
            historical: null,
            projected: null,
          };
          existing.projected = Number(row.projected_rera_inflow || 0);
          monthMap.set(row.month, existing);
        });

        const ordered = [...monthMap.values()].sort((a, b) => a.month.localeCompare(b.month));
        if (mounted) {
          setRows(ordered);
        }
      } catch (err) {
        if (mounted) {
          setError(err?.response?.data?.detail || err.message || "Unable to load predictive cashflow");
        }
      } finally {
        if (mounted) {
          setLoading(false);
        }
      }
    }

    load();
    return () => {
      mounted = false;
    };
  }, []);

  const totalProjected = useMemo(
    () => rows.reduce((sum, row) => sum + Number(row.projected || 0), 0),
    [rows]
  );

  return (
    <section className="rounded-2xl border border-emerald-400/25 bg-slate-950/70 p-4 sm:p-5 space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-emerald-100">Predictive RERA Cashflow</h2>
          <p className="text-xs text-slate-400">Historical inflows with 6-month milestone projection overlay</p>
        </div>
        <div className="text-xs text-emerald-200">Projected 6M: {money(totalProjected)}</div>
      </div>

      {error ? <div className="text-sm text-red-300">{error}</div> : null}
      {loading ? <div className="text-sm text-slate-400">Loading predictive model...</div> : null}

      {!loading ? (
        <div className="h-72 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={rows} margin={{ top: 12, right: 24, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.2)" />
              <XAxis dataKey="month" stroke="#94a3b8" />
              <YAxis stroke="#94a3b8" tickFormatter={(value) => `${Math.round(Number(value) / 100000)}L`} />
              <Tooltip
                formatter={(value) => money(value)}
                contentStyle={{ background: "#020617", border: "1px solid rgba(148,163,184,0.35)", borderRadius: 12 }}
              />
              <Legend />
              <Line type="monotone" dataKey="historical" name="Historical" stroke="#22d3ee" strokeWidth={2.5} dot={false} />
              <Line
                type="monotone"
                dataKey="projected"
                name="Projected"
                stroke="#34d399"
                strokeDasharray="8 6"
                strokeWidth={2.5}
                dot={{ r: 2 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      ) : null}
    </section>
  );
}
