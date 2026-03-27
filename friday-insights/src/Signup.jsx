import { useState } from "react";
import { Apple, Mail, User, Chrome, Loader2 } from "lucide-react";

export default function Signup() {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");

  const submitLead = async (provider) => {
    const trimmedName = name.trim();
    const trimmedEmail = email.trim();
    if (!trimmedName || !trimmedEmail) {
      setError("Enter your name and email to continue.");
      return;
    }

    setIsSubmitting(true);
    setError("");
    setResult(null);
    try {
      const res = await fetch("/api/v1/marketing/signup", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name: trimmedName,
          email: trimmedEmail,
          provider,
          source: "friday-signup-page",
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Signup failed (${res.status})`);
      }
      setResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to save signup");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <main className="mx-auto max-w-4xl px-4 sm:px-6 py-14 sm:py-20">
      <section className="relative overflow-hidden rounded-3xl border border-emerald-400/30 bg-slate-950/75 p-7 sm:p-10">
        <div
          className="pointer-events-none absolute -top-32 -left-20 h-72 w-72 rounded-full"
          style={{ background: "radial-gradient(circle, rgba(16,185,129,0.25), transparent 68%)" }}
        />
        <div
          className="pointer-events-none absolute -bottom-24 right-0 h-64 w-64 rounded-full"
          style={{ background: "radial-gradient(circle, rgba(14,165,233,0.2), transparent 70%)" }}
        />

        <p className="text-xs uppercase tracking-[0.24em] text-emerald-200">Accord Growth Access</p>
        <h1 className="mt-3 text-3xl sm:text-4xl font-black tracking-tight text-slate-100">Join The Compliance Intelligence Waitlist</h1>
        <p className="mt-3 text-sm sm:text-base text-slate-300 max-w-2xl">
          Early access for founders, finance teams, and chartered accountants. Choose your signup method and we will route onboarding updates to your inbox.
        </p>

        <div className="mt-7 grid gap-4 sm:grid-cols-2">
          <label className="rounded-xl border border-slate-700/80 bg-slate-900/70 p-3">
            <span className="text-xs text-slate-400">Full name</span>
            <div className="mt-1 flex items-center gap-2">
              <User className="w-4 h-4 text-emerald-300" />
              <input
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder="Aarav Mehta"
                className="w-full bg-transparent outline-none text-sm text-slate-100 placeholder:text-slate-500"
              />
            </div>
          </label>
          <label className="rounded-xl border border-slate-700/80 bg-slate-900/70 p-3">
            <span className="text-xs text-slate-400">Work email</span>
            <div className="mt-1 flex items-center gap-2">
              <Mail className="w-4 h-4 text-cyan-300" />
              <input
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                placeholder="you@company.com"
                type="email"
                className="w-full bg-transparent outline-none text-sm text-slate-100 placeholder:text-slate-500"
              />
            </div>
          </label>
        </div>

        <div className="mt-5 grid gap-3 sm:grid-cols-3">
          <button
            onClick={() => {
              void submitLead("EMAIL");
            }}
            disabled={isSubmitting}
            className="inline-flex items-center justify-center gap-2 rounded-xl border border-emerald-500/45 bg-emerald-900/30 px-4 py-3 text-sm font-semibold text-emerald-100 hover:bg-emerald-800/35 disabled:opacity-60"
          >
            {isSubmitting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Mail className="w-4 h-4" />}
            Continue with Email
          </button>
          <button
            onClick={() => {
              void submitLead("GOOGLE");
            }}
            disabled={isSubmitting}
            className="inline-flex items-center justify-center gap-2 rounded-xl border border-cyan-500/40 bg-cyan-900/25 px-4 py-3 text-sm font-semibold text-cyan-100 hover:bg-cyan-800/30 disabled:opacity-60"
          >
            <Chrome className="w-4 h-4" />
            Continue with Google
          </button>
          <button
            onClick={() => {
              void submitLead("APPLE");
            }}
            disabled={isSubmitting}
            className="inline-flex items-center justify-center gap-2 rounded-xl border border-slate-500/45 bg-slate-800/65 px-4 py-3 text-sm font-semibold text-slate-100 hover:bg-slate-700/70 disabled:opacity-60"
          >
            <Apple className="w-4 h-4" />
            Continue with Apple
          </button>
        </div>

        {error ? <p className="mt-4 text-sm text-red-300">{error}</p> : null}
        {result ? (
          <div className="mt-4 rounded-xl border border-emerald-500/35 bg-emerald-900/20 px-4 py-3 text-sm text-emerald-100">
            Lead captured for {result.email}. We will send onboarding updates shortly.
          </div>
        ) : null}
      </section>
    </main>
  );
}
