import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";

export default function CaAccept() {
  const { token } = useParams();
  const navigate = useNavigate();

  const [adminId, setAdminId] = useState("1001");
  const [email, setEmail] = useState("");
  const [status, setStatus] = useState("Verifying invite token...");
  const [error, setError] = useState("");
  const [isAccepting, setIsAccepting] = useState(false);

  useEffect(() => {
    const verifyToken = async () => {
      if (!token) {
        setError("Missing invite token.");
        setStatus("");
        return;
      }
      try {
        const res = await fetch(`/api/v1/ca/verify-token/${encodeURIComponent(token)}`);
        const data = await res.json();
        if (!res.ok) {
          throw new Error(data?.detail || `Invalid invite token (${res.status})`);
        }
        setEmail(data.email || "");
        setStatus(`Invite valid for ${data.email || "CA"}.`);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Invite verification failed");
        setStatus("");
      }
    };

    void verifyToken();
  }, [token]);

  const acceptInvite = async () => {
    const parsedId = Number(adminId.trim());
    if (!Number.isInteger(parsedId) || parsedId <= 0) {
      setError("Enter a valid positive Admin ID.");
      return;
    }
    if (!token) {
      setError("Missing invite token.");
      return;
    }

    setIsAccepting(true);
    setError("");
    try {
      const res = await fetch(`/api/v1/ca/accept/${encodeURIComponent(token)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ admin_id: parsedId }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to accept invite (${res.status})`);
      }
      setStatus(`Invite accepted for ${data.email}. Redirecting to CA Dashboard...`);
      setTimeout(() => navigate("/ca/dashboard"), 800);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to accept invite");
    } finally {
      setIsAccepting(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#020617] text-slate-100 p-6 sm:p-8 flex items-center justify-center">
      <div className="w-full max-w-xl rounded-2xl border border-slate-800 bg-slate-900/60 p-6">
        <p className="text-xs tracking-[0.2em] text-emerald-400 uppercase font-mono">Accord CA Invite</p>
        <h1 className="text-2xl font-bold mt-2">Accept Auditor Access</h1>
        <p className="text-sm text-slate-400 mt-2">This access is restricted to read-only compliance views.</p>

        {status ? <p className="mt-4 text-sm text-emerald-200">{status}</p> : null}
        {email ? <p className="mt-1 text-xs text-slate-400">Invite Email: {email}</p> : null}
        {error ? <p className="mt-4 text-sm text-red-300">{error}</p> : null}

        <label className="block mt-6 text-xs uppercase tracking-widest text-slate-400">Admin ID</label>
        <input
          value={adminId}
          onChange={(e) => setAdminId(e.target.value)}
          className="mt-2 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
          placeholder="Numeric Admin ID"
        />

        <div className="mt-5 flex gap-3">
          <button
            onClick={acceptInvite}
            disabled={isAccepting || !token}
            className="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-700 text-white text-sm font-semibold disabled:opacity-60"
          >
            {isAccepting ? "Accepting..." : "Accept Invite"}
          </button>
          <button
            onClick={() => navigate("/ca/dashboard")}
            className="px-4 py-2 rounded-lg border border-slate-700 bg-slate-900 hover:bg-slate-800 text-sm"
          >
            Open Dashboard
          </button>
        </div>
      </div>
    </div>
  );
}
