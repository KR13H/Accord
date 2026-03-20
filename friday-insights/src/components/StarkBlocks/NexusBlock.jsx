import { Fingerprint, Network } from "lucide-react";

export default function NexusBlock({ nodes = 0, edges = 0, integrityScore = 100 }) {
  return (
    <section className="h-full rounded-2xl border border-cyan-400/35 bg-black/80 p-3">
      <div className="flex items-center justify-between">
        <p className="text-[11px] uppercase tracking-[0.2em] text-cyan-300">Nexus Graph</p>
        <Network className="w-4 h-4 text-cyan-300" />
      </div>
      <div className="mt-3 grid grid-cols-3 gap-2 text-center">
        <div className="rounded-lg border border-white/10 p-2">
          <p className="text-[10px] text-slate-400">Nodes</p>
          <p className="text-sm font-semibold text-white">{nodes}</p>
        </div>
        <div className="rounded-lg border border-white/10 p-2">
          <p className="text-[10px] text-slate-400">Edges</p>
          <p className="text-sm font-semibold text-white">{edges}</p>
        </div>
        <div className="rounded-lg border border-cyan-500/35 p-2 bg-cyan-900/15">
          <p className="text-[10px] text-slate-400 inline-flex items-center gap-1 justify-center"><Fingerprint className="w-3 h-3" /> SHA</p>
          <p className="text-sm font-semibold text-cyan-200">{Math.round(Number(integrityScore || 0))}%</p>
        </div>
      </div>
    </section>
  );
}
