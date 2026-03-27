import { useEffect, useMemo, useState } from "react";
import { motion, useMotionValue, useTransform, animate } from "framer-motion";

function Counter({ to }) {
  const mv = useMotionValue(0);
  const out = useTransform(mv, (v) => Math.round(v).toLocaleString("en-IN"));
  useEffect(() => {
    const ctl = animate(mv, to, { duration: 1.2, ease: "easeOut" });
    return () => ctl.stop();
  }, [mv, to]);
  return <motion.span>{out}</motion.span>;
}

export default function ProfitAndLoss() {
  const [displayCurrency, setDisplayCurrency] = useState("INR");
  const revenue = 2284200;
  const expenses = 1328800;
  const profit = revenue - expenses;

  const fx = {
    USD: 83.15,
    GBP: 105.42,
  };

  const view = useMemo(() => {
    if (displayCurrency === "USD") {
      return {
        label: "USD",
        revenue: revenue / fx.USD,
        expenses: expenses / fx.USD,
        profit: profit / fx.USD,
      };
    }
    if (displayCurrency === "TRANSACTION") {
      return {
        label: "GBP",
        revenue: revenue / fx.GBP,
        expenses: expenses / fx.GBP,
        profit: profit / fx.GBP,
      };
    }
    return {
      label: "INR",
      revenue,
      expenses,
      profit,
    };
  }, [displayCurrency, expenses, profit, revenue]);

  return (
    <div className="min-h-screen bg-black text-white p-6">
      <div className="mx-auto max-w-5xl rounded-3xl border border-cyan-400/25 bg-slate-950/70 p-6">
        <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-300">Stark Reports</p>
        <h1 className="text-3xl font-bold mt-2">Profit & Loss</h1>
        <div className="mt-4 inline-flex rounded-lg border border-slate-700 bg-black/30 p-1 text-xs">
          {[
            { key: "INR", label: "INR" },
            { key: "USD", label: "USD" },
            { key: "TRANSACTION", label: "Transaction" },
          ].map((item) => (
            <button
              key={item.key}
              onClick={() => setDisplayCurrency(item.key)}
              className={`px-3 py-1 rounded-md transition-colors ${
                displayCurrency === item.key ? "bg-cyan-500/25 text-cyan-100" : "text-slate-400 hover:text-slate-200"
              }`}
            >
              {item.label}
            </button>
          ))}
        </div>
        <div className="mt-6 space-y-4">
          <div className="rounded-xl border border-cyan-500/30 bg-cyan-900/10 p-4 flex items-center justify-between">
            <span className="text-sm text-slate-300">Revenue</span>
            <span className="text-2xl font-bold text-cyan-200">{view.label} <Counter to={view.revenue} /></span>
          </div>
          <div className="rounded-xl border border-white/20 bg-white/5 p-4 flex items-center justify-between">
            <span className="text-sm text-slate-300">Expenses</span>
            <span className="text-2xl font-bold text-white">{view.label} <Counter to={view.expenses} /></span>
          </div>
          <div className="rounded-xl border border-cyan-400/35 bg-cyan-950/20 p-4 flex items-center justify-between">
            <span className="text-sm text-slate-300">Net Profit</span>
            <span className="text-3xl font-bold text-cyan-200">{view.label} <Counter to={view.profit} /></span>
          </div>
        </div>
      </div>
    </div>
  );
}
