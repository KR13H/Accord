import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

const DATA = [
  { day: "Mon", inflow: 120, leakage: 45 },
  { day: "Tue", inflow: 140, leakage: 42 },
  { day: "Wed", inflow: 165, leakage: 44 },
  { day: "Thu", inflow: 182, leakage: 41 },
  { day: "Fri", inflow: 210, leakage: 38 },
  { day: "Sat", inflow: 190, leakage: 34 },
  { day: "Sun", inflow: 176, leakage: 32 },
];

export default function SupremacyChart() {
  return (
    <div className="h-full rounded-2xl border border-cyan-400/35 bg-black/80 p-3">
      <p className="text-[11px] uppercase tracking-[0.2em] text-cyan-300 mb-2">Cash-Flow River</p>
      <div className="h-[240px]">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={DATA}>
            <XAxis dataKey="day" stroke="#64748B" tick={{ fontSize: 11 }} />
            <YAxis stroke="#64748B" tick={{ fontSize: 11 }} />
            <Tooltip />
            <Area type="monotone" dataKey="inflow" stroke="#22D3EE" fill="#22D3EE33" strokeWidth={2} />
            <Area type="monotone" dataKey="leakage" stroke="#FFFFFF" fill="#FFFFFF1A" strokeWidth={2} />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
