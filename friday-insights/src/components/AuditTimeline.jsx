import { Clock, Copy, Download, Fingerprint, Shield } from "lucide-react";

function downloadSignature(log) {
  const payload = {
    timestamp: log?.timestamp || "",
    action: log?.action || "",
    fingerprint: log?.fingerprint || "",
    details: log?.details || {},
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `AuditVault_${log?.action || "event"}_${(log?.id || "0")}.json`;
  link.click();
  URL.revokeObjectURL(url);
}

export default function AuditTimeline({ logs = [] }) {
  return (
    <div className="bg-[#020617] border border-slate-900 rounded-2xl p-6" style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" }}>
      <h3 className="text-xs font-black text-cyan-400 uppercase tracking-[0.3em] mb-8 flex items-center gap-2">
        <Shield className="w-4 h-4" /> Live Integrity Vault
      </h3>

      <div className="space-y-10 relative before:absolute before:left-[15px] before:top-2 before:bottom-2 before:w-[1px] before:bg-slate-800">
        {logs.length > 0 ? (
          logs.map((log, index) => (
            <div key={`${log?.id || index}-${log?.action || "entry"}`} className="relative pl-10 group">
              <div className="absolute left-0 top-1 w-8 h-8 rounded-full bg-black border border-slate-800 flex items-center justify-center group-hover:border-cyan-500 transition-all">
                <div className="w-2 h-2 bg-cyan-500 rounded-full animate-pulse" />
              </div>

              <div className="space-y-2">
                <div className="flex justify-between items-center">
                  <span className="text-[10px] text-slate-500 uppercase">{log?.action || "EVENT"}</span>
                  <span className="text-[10px] text-slate-600 flex items-center gap-1">
                    <Clock className="w-3 h-3" /> {new Date(log?.timestamp || Date.now()).toLocaleTimeString()}
                  </span>
                </div>

                <div className="bg-black/40 border border-slate-900 p-3 rounded-lg flex items-center justify-between group-hover:border-slate-700 transition-colors">
                  <div className="flex items-center gap-3 min-w-0">
                    <Fingerprint className="w-4 h-4 text-slate-500" />
                    <code className="text-[10px] text-cyan-400 font-bold truncate w-44 sm:w-56">{log?.fingerprint || "NO_FINGERPRINT"}</code>
                  </div>
                  <div className="flex gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                    <button
                      onClick={() => {
                        void navigator.clipboard.writeText(log?.fingerprint || "");
                      }}
                      className="p-1 hover:text-cyan-400"
                      title="Copy fingerprint"
                    >
                      <Copy className="w-3 h-3" />
                    </button>
                    <button onClick={() => downloadSignature(log)} className="p-1 hover:text-cyan-400" title="Download signature payload">
                      <Download className="w-3 h-3" />
                    </button>
                  </div>
                </div>
              </div>
            </div>
          ))
        ) : (
          <p className="text-xs text-slate-500">No filing events in the audit vault yet.</p>
        )}
      </div>
    </div>
  );
}
