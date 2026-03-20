import { useEffect, useMemo, useState } from "react";
import { motion, useMotionValue, useTransform, animate } from "framer-motion";

function CountUp({ value }) {
  const mv = useMotionValue(0);
  const rounded = useTransform(mv, (latest) => Math.round(latest).toLocaleString("en-IN"));
  useEffect(() => {
    const controls = animate(mv, value, { duration: 1.1, ease: "easeOut" });
    return () => controls.stop();
  }, [mv, value]);
  return <motion.span>{rounded}</motion.span>;
}

export default function BalanceSheet() {
  const [data] = useState({ assets: 1482500, liabilities: 512400, equity: 970100 });
  const [displayCurrency, setDisplayCurrency] = useState("INR");

  const fx = {
    USD: 83.15,
    AED: 22.64,
  };

  const view = useMemo(() => {
    if (displayCurrency === "USD") {
      return {
        label: "USD",
        assets: data.assets / fx.USD,
        liabilities: data.liabilities / fx.USD,
        equity: data.equity / fx.USD,
      };
    }
    if (displayCurrency === "TRANSACTION") {
      return {
        label: "AED",
        assets: data.assets / fx.AED,
        liabilities: data.liabilities / fx.AED,
        equity: data.equity / fx.AED,
      };
    }
    return {
      label: "INR",
      assets: data.assets,
      liabilities: data.liabilities,
      equity: data.equity,
    };
  }, [data.assets, data.equity, data.liabilities, displayCurrency]);

  return (
    <div className="min-h-screen bg-black text-white p-6">
      <div className="mx-auto max-w-5xl rounded-3xl border border-cyan-400/25 bg-slate-950/70 p-6">
        <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-300">Stark Reports</p>
        <h1 className="text-3xl font-bold mt-2">Balance Sheet</h1>
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
        <div className="mt-6 grid grid-cols-1 sm:grid-cols-3 gap-4">
          <div className="rounded-xl border border-cyan-400/25 bg-cyan-900/10 p-4">
            <p className="text-xs text-slate-400">Assets</p>
            <p className="text-2xl font-bold text-cyan-200 mt-2">{view.label} <CountUp value={view.assets} /></p>
          </div>
          <div className="rounded-xl border border-white/20 bg-white/5 p-4">
            <p className="text-xs text-slate-400">Liabilities</p>
            <p className="text-2xl font-bold text-white mt-2">{view.label} <CountUp value={view.liabilities} /></p>
          </div>
          <div className="rounded-xl border border-cyan-400/25 bg-cyan-900/10 p-4">
            <p className="text-xs text-slate-400">Equity</p>
            <p className="text-2xl font-bold text-cyan-200 mt-2">{view.label} <CountUp value={view.equity} /></p>
          </div>
        </div>
      </div>
    </div>
  );
}
