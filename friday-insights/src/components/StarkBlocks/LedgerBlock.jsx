import { motion } from "framer-motion";

export default function LedgerBlock({ entries = [] }) {
  const rows = Array.isArray(entries) ? entries.slice(0, 6) : [];
  return (
    <section className="h-full rounded-2xl border border-cyan-400/35 bg-black/80 p-3">
      <p className="text-[11px] uppercase tracking-[0.2em] text-cyan-300">Live Ledger</p>
      <div className="mt-3 space-y-2">
        {rows.length > 0 ? (
          rows.map((row, idx) => (
            <motion.div
              key={`${row.reference || "entry"}-${idx}`}
              initial={{ opacity: 0, y: 4 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.2, delay: idx * 0.02 }}
              className="rounded-lg border border-white/10 bg-slate-950/80 px-2.5 py-2 text-xs"
            >
              <p className="text-cyan-200 font-semibold">{row.reference || "ACC/--/000000"}</p>
              <p className="text-slate-400">{row.description || "Ledger movement"}</p>
            </motion.div>
          ))
        ) : (
          <p className="text-xs text-slate-500">No ledger entries loaded.</p>
        )}
      </div>
    </section>
  );
}
