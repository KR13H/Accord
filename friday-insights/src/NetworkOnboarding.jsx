import { Fingerprint, Network, ShieldCheck } from "lucide-react";

function scoreTone(score) {
  if (score >= 85) return "text-emerald-300";
  if (score >= 65) return "text-cyan-300";
  if (score >= 40) return "text-amber-300";
  return "text-red-300";
}

export default function NetworkOnboarding({ network }) {
  const tenants = Array.isArray(network?.tenants) ? network.tenants.slice(0, 12) : [];

  return (
    <section className="rounded-2xl border border-cyan-800/45 bg-slate-950/70 p-5">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="text-[11px] uppercase tracking-[0.2em] text-cyan-300">CA Network</p>
          <h3 className="text-lg font-semibold text-slate-100 mt-1">Consolidated Integrity Map</h3>
        </div>
        <Network className="w-5 h-5 text-cyan-300" />
      </div>

      <p className="text-xs text-slate-400 mt-2">
        Trust score and SHA-256 validation status for invited client nodes.
      </p>

      <div className="mt-4 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
        {tenants.length > 0 ? (
          tenants.map((tenant) => {
            const score = Math.round(Number(tenant?.integrity_score || 0));
            const verified = Number(tenant?.total_entries || 0) > 0 && Number(tenant?.verified_entries || 0) === Number(tenant?.total_entries || 0);
            return (
              <article key={`${tenant.gstin}-${tenant.vendor_name}`} className="rounded-xl border border-slate-800 bg-black/60 p-3 space-y-2">
                <p className="text-sm font-semibold text-slate-100 truncate">{tenant.vendor_name || "Client Node"}</p>
                <p className="text-[11px] font-mono text-cyan-200 break-all">{tenant.gstin || "GSTIN N/A"}</p>
                <div className="flex items-center justify-between text-xs">
                  <span className="text-slate-400 inline-flex items-center gap-1"><Fingerprint className="w-3.5 h-3.5" /> Trust Score</span>
                  <span className={`font-semibold ${scoreTone(score)}`}>{score}%</span>
                </div>
                <div className="flex items-center justify-between text-xs">
                  <span className="text-slate-400 inline-flex items-center gap-1"><ShieldCheck className="w-3.5 h-3.5" /> SHA-256</span>
                  <span className={verified ? "text-emerald-300 font-semibold" : "text-amber-300 font-semibold"}>
                    {verified ? "VALIDATED" : "PENDING"}
                  </span>
                </div>
              </article>
            );
          })
        ) : (
          <p className="text-sm text-slate-400">No invited clients are visible yet in the integrity map.</p>
        )}
      </div>
    </section>
  );
}
