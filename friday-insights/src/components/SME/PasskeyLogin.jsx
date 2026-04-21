import { useState } from "react";
import { startAuthentication, startRegistration } from "@simplewebauthn/browser";

import { clearSmeSession, getStoredSmeRole, getStoredSmeUsername, persistSmeSession } from "../../api/smeAuth";

const AUTH_BASE = import.meta.env.VITE_API_BASE_URL || "";

function extractErrorMessage(error) {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error || "Passkey operation failed");
}

export default function PasskeyLogin({ role: roleProp, onAuthenticated, onLoggedOut }) {
  const [status, setStatus] = useState(() => {
    const token = window.localStorage.getItem("smeSessionToken");
    return token ? "Logged in with passkey" : "Passkey not enrolled";
  });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const role = (roleProp || getStoredSmeRole()).trim().toLowerCase() || "owner";

  const requestJson = async (path, payload) => {
    const response = await fetch(`${AUTH_BASE}${path}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload || {}),
    });
    const body = await response.json();
    if (!response.ok) {
      throw new Error(body?.detail || `Request failed (${response.status})`);
    }
    return body;
  };

  const handleEnroll = async () => {
    if (busy) return;
    setBusy(true);
    setError("");
    try {
      const suggestedUsername = getStoredSmeUsername() || "cashier@accord.local";
      const username = window.prompt("Enter a passkey username", suggestedUsername)?.trim();
      if (!username) {
        setStatus("Enrollment cancelled");
        return;
      }

      const registration = await requestJson("/api/v1/auth/generate-registration-options", {
        username,
        display_name: username,
        role,
      });
      const credential = await startRegistration(registration.options);
      const verification = await requestJson("/api/v1/auth/verify-registration", {
        challenge_id: registration.challenge_id,
        credential,
      });

      persistSmeSession({
        role: verification.role || role,
        sessionToken: verification.session_token,
        username: verification.username || username,
        accessToken: verification.access_token,
      });
      setStatus(`Passkey enrolled for ${verification.username || username}`);
      onAuthenticated?.(verification);
    } catch (err) {
      const message = extractErrorMessage(err);
      setError(message);
      setStatus("Enrollment failed");
    } finally {
      setBusy(false);
    }
  };

  const handleLogin = async () => {
    if (busy) return;
    setBusy(true);
    setError("");
    try {
      const loginOptions = await requestJson("/api/v1/auth/generate-authentication-options", {});
      const credential = await startAuthentication(loginOptions.options);
      const verification = await requestJson("/api/v1/auth/verify-authentication", {
        challenge_id: loginOptions.challenge_id,
        credential,
      });

      persistSmeSession({
        role: verification.role || role,
        sessionToken: verification.session_token,
        username: verification.username,
        accessToken: verification.access_token,
      });
      setStatus(`Authenticated as ${verification.username}`);
      onAuthenticated?.(verification);
    } catch (err) {
      const message = extractErrorMessage(err);
      setError(message);
      setStatus("Passkey login failed");
    } finally {
      setBusy(false);
    }
  };

  const handleLogout = () => {
    clearSmeSession();
    setStatus("Logged out");
    onLoggedOut?.();
  };

  return (
    <div className="rounded-2xl border border-cyan-400/25 bg-slate-950/85 backdrop-blur-xl px-3 py-3 min-w-[260px] max-w-[320px] shadow-2xl shadow-cyan-950/20">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="text-[10px] uppercase tracking-[0.18em] text-cyan-300">Passkey</p>
          <p className="text-[11px] text-slate-300 mt-1">{status}</p>
        </div>
        <span className="rounded-full border border-cyan-400/25 bg-cyan-500/10 px-2 py-1 text-[10px] font-semibold text-cyan-100">
          {role}
        </span>
      </div>

      <div className="mt-3 flex flex-wrap gap-2">
        <button
          type="button"
          onClick={handleLogin}
          disabled={busy}
          className="rounded-xl border border-emerald-400/30 bg-emerald-500/20 px-3 py-1.5 text-xs font-semibold text-emerald-50 disabled:opacity-60"
        >
          Login
        </button>
        <button
          type="button"
          onClick={handleEnroll}
          disabled={busy}
          className="rounded-xl border border-cyan-400/30 bg-cyan-500/20 px-3 py-1.5 text-xs font-semibold text-cyan-50 disabled:opacity-60"
        >
          Enroll Passkey
        </button>
        <button
          type="button"
          onClick={handleLogout}
          className="rounded-xl border border-slate-700 bg-slate-900/70 px-3 py-1.5 text-xs font-semibold text-slate-200"
        >
          Logout
        </button>
      </div>

      {error ? <p className="mt-2 text-[11px] text-rose-300">{error}</p> : null}
    </div>
  );
}