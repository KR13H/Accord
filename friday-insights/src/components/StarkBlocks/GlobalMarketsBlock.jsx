import { Globe, TrendingUp } from "lucide-react";

export default function GlobalMarketsBlock({ rates = {} }) {
  return (
    <div className="bg-[#020617] border border-slate-900 p-6 rounded-2xl" style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" }}>
      <h3 className="text-[10px] font-black text-cyan-400 uppercase tracking-widest mb-6 flex items-center gap-2">
        <Globe className="w-3 h-3" /> Global FX Pulse
      </h3>
      <div className="grid grid-cols-2 gap-4">
        {Object.entries(rates).map(([code, rate]) => (
          <div key={code} className="border-b border-slate-900 pb-2">
            <span className="text-slate-500 text-[9px] uppercase">{code}/INR</span>
            <p className="text-lg font-bold text-white">INR {rate}</p>
          </div>
        ))}
      </div>
      <div className="mt-6 flex items-center gap-2 text-[10px] text-emerald-500">
        <TrendingUp className="w-3 h-3" /> Revaluation Active: M3 Optimized
      </div>
    </div>
  );
}
