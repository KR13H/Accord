import { useEffect, useMemo, useState } from "react";
import { Fingerprint, FileText, Loader2, ShieldCheck } from "lucide-react";
import { AnimatePresence, motion } from "framer-motion";

function formatDateTime(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString("en-IN", { dateStyle: "medium", timeStyle: "short" });
}

export default function CaDashboard() {
  const [entries, setEntries] = useState([]);
  const [network, setNetwork] = useState(null);
  const [verifyResult, setVerifyResult] = useState(null);
  const [isVerifyingIntegrity, setIsVerifyingIntegrity] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    const fetchAuditSummary = async () => {
      setLoading(true);
      setError("");
      try {
        const res = await fetch("/api/v1/ca/audit-summary", {
          headers: {
            "X-Role": "ca",
            "X-Admin-Id": "1001",
          },
        });
        const data = await res.json();
        if (!res.ok) {
          throw new Error(data?.detail || `Unable to load CA audit summary (${res.status})`);
        }

        const networkRes = await fetch("/api/v1/ca/network-integrity?limit=120", {
          headers: {
            "X-Role": "ca",
            "X-Admin-Id": "1001",
          },
        });
        const networkData = await networkRes.json();
        if (!networkRes.ok) {
          throw new Error(networkData?.detail || `Unable to load CA network integrity (${networkRes.status})`);
        }

        setEntries(Array.isArray(data?.entries) ? data.entries : []);
        setNetwork(networkData || null);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load CA audit summary");
      } finally {
        setLoading(false);
      }
    };

    void fetchAuditSummary();
  }, []);

  const verifyIntegrity = async () => {
    setIsVerifyingIntegrity(true);
    setError("");
    try {
      const res = await fetch("/api/v1/ca/verify-integrity?limit=1500", {
        method: "POST",
        headers: {
          "X-Role": "ca",
          "X-Admin-Id": "1001",
        },
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Integrity verification failed (${res.status})`);
      }
      setVerifyResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to verify integrity");
    } finally {
      setIsVerifyingIntegrity(false);
    }
  };

  const hasEntries = useMemo(() => entries.length > 0, [entries]);
  const timeline = useMemo(() => {
    const source = Array.isArray(verifyResult?.timeline) ? verifyResult.timeline : entries;
    return source.slice(0, 20);
  }, [verifyResult, entries]);

  if (loading) {
    return (
      <div className="min-h-screen bg-[#020617] text-slate-100 p-8 flex items-center justify-center gap-3">
        <Loader2 className="w-5 h-5 animate-spin" />
        <span className="text-sm tracking-wide">Loading CA audit console...</span>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#020617] text-slate-100 p-8">
      <header className="mb-8">
        <p className="text-xs tracking-[0.2em] text-emerald-400 uppercase font-mono">Auditor Portal</p>
        <h1 className="text-3xl font-bold">Chartered Accountant Dashboard</h1>
        <p className="text-sm text-slate-400 mt-2">Read-only trail for Rule 37A reversal events and SHA-256 integrity verification.</p>
      </header>

      <div className="grid grid-cols-1 gap-6">
        <div className="p-6 bg-slate-900/40 border border-slate-800 rounded-2xl">
          <h3 className="text-lg font-bold mb-4 flex items-center gap-2">
            <ShieldCheck className="w-5 h-5 text-cyan-300" /> Multi-Tenant Integrity Grid
          </h3>

          <div className="mb-4 flex flex-wrap items-center gap-3">
            <button
              onClick={() => {
                void verifyIntegrity();
              }}
              disabled={isVerifyingIntegrity}
              className="inline-flex items-center gap-2 rounded-lg border border-cyan-600/70 bg-cyan-900/30 px-3 py-2 text-xs font-semibold text-cyan-100 hover:bg-cyan-800/40 disabled:opacity-60"
            >
              {isVerifyingIntegrity ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Fingerprint className="w-3.5 h-3.5" />}
              Verify Integrity
            </button>
            {verifyResult ? (
              <span className="text-xs text-slate-300">
                Score {Math.round(Number(verifyResult.integrity_score || 0))}% | Checked {verifyResult.total_checked} | Mismatch {verifyResult.mismatch_count}
              </span>
            ) : null}
          </div>

          {verifyResult?.forensic_summary ? (
            <p className="mb-4 text-xs text-cyan-200 border border-cyan-800/50 bg-cyan-950/30 rounded-lg px-3 py-2">
              {verifyResult.forensic_summary}
            </p>
          ) : null}

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-4">
            <div className="rounded-xl border border-slate-800 bg-slate-950/75 p-3">
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-400">Aggregate Integrity</p>
              <p className="text-2xl font-bold text-cyan-200 mt-1">{Math.round(Number(network?.aggregate_integrity_score || 0))}%</p>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/75 p-3">
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-400">Visible Tenants</p>
              <p className="text-2xl font-bold text-emerald-300 mt-1">{network?.tenant_count ?? 0}</p>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/75 p-3">
              <p className="text-[11px] uppercase tracking-[0.12em] text-slate-400">Accepted Clients</p>
              <p className="text-2xl font-bold text-indigo-300 mt-1">{network?.accepted_clients ?? 0}</p>
            </div>
          </div>

          <p className="text-xs text-slate-400 mb-3 inline-flex items-center gap-1">
            <Fingerprint className="w-3.5 h-3.5" /> Verification Mode: {network?.hash_algorithm || "SHA-256"} (read-only)
          </p>

          {Array.isArray(network?.tenants) && network.tenants.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm min-w-[860px]">
                <thead className="text-slate-500 uppercase text-xs border-b border-slate-800">
                  <tr>
                    <th className="pb-3">GSTIN</th>
                    <th className="pb-3">Vendor</th>
                    <th className="pb-3">Entries</th>
                    <th className="pb-3">Verified</th>
                    <th className="pb-3">Integrity Score</th>
                    <th className="pb-3">Last Activity</th>
                  </tr>
                </thead>
                <tbody>
                  {network.tenants.slice(0, 60).map((tenant) => (
                    <tr key={`tenant-${tenant.gstin}-${tenant.vendor_name}`} className="border-b border-slate-800/50 align-top">
                      <td className="py-4 pr-3 font-mono text-cyan-200">{tenant.gstin}</td>
                      <td className="py-4 pr-3">{tenant.vendor_name}</td>
                      <td className="py-4 pr-3">{tenant.total_entries}</td>
                      <td className="py-4 pr-3">{tenant.verified_entries}</td>
                      <td className="py-4 pr-3 text-emerald-300 font-semibold">{Math.round(Number(tenant.integrity_score || 0))}%</td>
                      <td className="py-4 pr-3">{formatDateTime(tenant.last_activity)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-sm text-slate-400">No tenant integrity records available yet.</p>
          )}

          <div className="mt-6 rounded-xl border border-cyan-800/50 bg-black/70 p-4">
            <p className="text-xs uppercase tracking-[0.18em] text-cyan-200 mb-3">Chain Of Trust Timeline</p>
            <div className="relative pl-6">
              <div className="absolute left-2 top-1 bottom-1 w-[2px] bg-cyan-400/70 shadow-[0_0_16px_rgba(34,211,238,0.8)]" />
              <AnimatePresence>
                <div className="space-y-3">
                  {timeline.map((item, idx) => (
                    <motion.div
                      key={`timeline-${item.entry_id || item.id || idx}`}
                      initial={{ opacity: 0, x: 8 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ duration: 0.2, delay: idx * 0.03 }}
                      className="relative rounded-lg border border-slate-800 bg-slate-950/70 px-3 py-2"
                    >
                      <span className="absolute -left-5 top-3 h-3 w-3 rounded-full bg-white shadow-[0_0_14px_rgba(255,255,255,0.9)]" />
                      <p className="text-[11px] text-cyan-200 font-semibold">{item.reference || item.ref || `ENTRY-${item.entry_id || item.id}`}</p>
                      <p className="text-[10px] text-slate-500 break-all">
                        {(item.stored_fingerprint || item.fingerprint || "").slice(0, 20)}...
                      </p>
                    </motion.div>
                  ))}
                </div>
              </AnimatePresence>
            </div>
          </div>
        </div>

        <div className="p-6 bg-slate-900/40 border border-slate-800 rounded-2xl">
          <h3 className="text-lg font-bold mb-4 flex items-center gap-2">
            <ShieldCheck className="w-5 h-5 text-emerald-400" /> Verified Reversal History
          </h3>

          {error ? <p className="mb-4 text-sm text-red-300">{error}</p> : null}
          {!error && !hasEntries ? <p className="text-sm text-slate-400">No reversal records in the selected audit window.</p> : null}

          {hasEntries ? (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm min-w-[860px]">
                <thead className="text-slate-500 uppercase text-xs border-b border-slate-800">
                  <tr>
                    <th className="pb-3">Date</th>
                    <th className="pb-3">Reference</th>
                    <th className="pb-3">GSTIN</th>
                    <th className="pb-3">Amount</th>
                    <th className="pb-3">Fingerprint</th>
                    <th className="pb-3">Mode</th>
                  </tr>
                </thead>
                <tbody>
                  {entries.map((item) => (
                    <tr key={item.id} className="border-b border-slate-800/50 align-top">
                      <td className="py-4 pr-3">{formatDateTime(item.date)}</td>
                      <td className="py-4 pr-3 font-mono text-cyan-200">{item.ref}</td>
                      <td className="py-4 pr-3">{item.gstin}</td>
                      <td className="py-4 pr-3 text-emerald-400 font-semibold">INR {item.amount}</td>
                      <td className="py-4 pr-3 text-[10px] text-slate-500 font-mono break-all inline-flex items-start gap-1">
                        <Fingerprint className="w-3.5 h-3.5 mt-0.5" />
                        <span>{item.fingerprint}</span>
                      </td>
                      <td className="py-4 pr-3 text-[11px] text-slate-300 inline-flex items-center gap-1">
                        <FileText className="w-3.5 h-3.5" />
                        {item.read_only ? "READ_ONLY" : "N/A"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
