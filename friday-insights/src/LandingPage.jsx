import { Link } from "react-router-dom";
import { motion } from "framer-motion";
import {
  ArrowRight,
  BadgeCheck,
  Bot,
  BrainCircuit,
  FileBadge2,
  Lock,
  ScanLine,
  Shield,
  Sparkles,
  Target,
} from "lucide-react";

const moatCards = [
  {
    icon: Shield,
    title: "Rule 37A Shield",
    text: "Catch ITC reversal exposure before it turns into hard cash loss.",
    tone: "from-red-500/25 via-rose-500/15 to-transparent",
  },
  {
    icon: Target,
    title: "Section 50(3) Sword",
    text: "Turn Safe Harbor evidence into a defensible zero-interest argument.",
    tone: "from-emerald-500/25 via-teal-500/15 to-transparent",
  },
  {
    icon: Lock,
    title: "Audit Immunity Stack",
    text: "Dual-admin controls plus integrity hashes for verifiable, tamper-evident exports.",
    tone: "from-cyan-500/25 via-sky-500/15 to-transparent",
  },
];

const trustSignals = [
  "Rule 37A risk engine with actionable reversal trails",
  "Section 50(3) Safe Harbor certificate workflow",
  "SHA-256 fingerprint verification and chain of control",
  "Local Friday AI on-device for privacy-first analysis",
];

export default function LandingPage() {
  return (
    <div className="min-h-screen text-slate-100 bg-[#020617]">
      <section className="relative overflow-hidden px-4 sm:px-6 lg:px-8 pt-14 pb-20">
        <div className="absolute inset-0 pointer-events-none">
          <div className="absolute -top-36 -left-24 w-[34rem] h-[34rem] rounded-full bg-cyan-500/20 blur-3xl" />
          <motion.div
            className="absolute right-8 top-16 w-[18rem] h-[18rem] rounded-full bg-emerald-400/15 blur-3xl"
            animate={{ opacity: [0.35, 0.75, 0.35], scale: [1, 1.08, 1] }}
            transition={{ duration: 5.2, repeat: Infinity, ease: "easeInOut" }}
          />
        </div>

        <div className="relative mx-auto max-w-6xl">
          <motion.p
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="text-[11px] uppercase tracking-[0.24em] text-cyan-300/90 font-semibold"
          >
            Governance-First ERP
          </motion.p>

          <motion.h1
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.08 }}
            className="mt-4 max-w-4xl text-4xl sm:text-5xl lg:text-6xl font-black leading-tight tracking-tight"
            style={{ fontFamily: '"Sora", "Space Grotesk", sans-serif' }}
          >
            Audit Immunity for Indian SMEs.
            <span className="block text-cyan-300">Compliance that compounds into advantage.</span>
          </motion.h1>

          <motion.p
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.16 }}
            className="mt-6 max-w-2xl text-base sm:text-lg text-slate-300/90"
          >
            Accord turns Rule 37A chaos into operational clarity with CA-ready governance, local AI reasoning,
            and export proofs built for scrutiny.
          </motion.p>

          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.24 }}
            className="mt-8 flex flex-wrap gap-3"
          >
            <Link
              to="/insights"
              className="inline-flex items-center gap-2 rounded-xl bg-cyan-500 hover:bg-cyan-400 text-slate-950 px-5 py-3 text-sm font-bold"
            >
              Start Free Compliance Scan
              <ScanLine className="w-4 h-4" />
            </Link>
            <Link
              to="/pricing"
              className="inline-flex items-center gap-2 rounded-xl border border-cyan-400/50 bg-slate-950/50 hover:bg-slate-900/70 px-5 py-3 text-sm font-semibold"
            >
              View Pricing
              <ArrowRight className="w-4 h-4" />
            </Link>
          </motion.div>

          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.32 }}
            className="mt-10 rounded-2xl border border-cyan-500/25 bg-slate-950/60 p-4 sm:p-5"
          >
            <p className="text-xs tracking-[0.18em] uppercase text-cyan-200/80">Live Compliance Scan</p>
            <div className="relative mt-3 rounded-xl border border-slate-800 bg-black/50 p-4 overflow-hidden">
              <motion.div
                className="absolute inset-x-0 h-12 bg-gradient-to-b from-cyan-300/20 to-transparent"
                animate={{ y: [0, 120, 0] }}
                transition={{ duration: 2.4, repeat: Infinity, ease: "easeInOut" }}
              />
              <div className="relative grid grid-cols-1 sm:grid-cols-3 gap-3 text-sm">
                <div className="rounded-lg border border-red-500/35 bg-red-900/15 p-3">
                  <p className="text-[11px] uppercase tracking-widest text-red-300">Rule 37A Risk</p>
                  <p className="mt-1 text-lg font-bold text-red-200">Critical</p>
                </div>
                <div className="rounded-lg border border-amber-500/35 bg-amber-900/15 p-3">
                  <p className="text-[11px] uppercase tracking-widest text-amber-300">At-Risk Invoices</p>
                  <p className="mt-1 text-lg font-bold text-amber-100">14</p>
                </div>
                <div className="rounded-lg border border-emerald-500/35 bg-emerald-900/15 p-3">
                  <p className="text-[11px] uppercase tracking-widest text-emerald-300">Safe Harbor Offset</p>
                  <p className="mt-1 text-lg font-bold text-emerald-100">INR 2,18,400</p>
                </div>
              </div>
            </div>
          </motion.div>
        </div>
      </section>

      <section className="px-4 sm:px-6 lg:px-8 pb-16">
        <div className="mx-auto max-w-6xl grid grid-cols-1 md:grid-cols-3 gap-4">
          {moatCards.map((card, idx) => {
            const Icon = card.icon;
            return (
              <motion.div
                key={card.title}
                initial={{ opacity: 0, y: 14 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true, amount: 0.4 }}
                transition={{ delay: idx * 0.08 }}
                className={`rounded-2xl border border-slate-700/80 bg-gradient-to-br ${card.tone} p-5`}
              >
                <Icon className="w-5 h-5 text-cyan-200" />
                <h3 className="mt-3 text-lg font-bold">{card.title}</h3>
                <p className="mt-2 text-sm text-slate-300">{card.text}</p>
              </motion.div>
            );
          })}
        </div>
      </section>

      <section className="px-4 sm:px-6 lg:px-8 pb-20">
        <div className="mx-auto max-w-6xl rounded-3xl border border-slate-700/80 bg-slate-900/45 p-6 sm:p-8 grid grid-cols-1 lg:grid-cols-[1.1fr_0.9fr] gap-8">
          <div>
            <p className="text-[11px] tracking-[0.2em] uppercase text-cyan-300/80">Friday Advantage</p>
            <h2 className="mt-2 text-2xl sm:text-3xl font-bold">Local AI. Founder-Grade Clarity. CA-Grade Defensibility.</h2>
            <p className="mt-3 text-sm sm:text-base text-slate-300">
              Friday runs locally for privacy-sensitive analysis while your governance stack keeps every compliance claim
              traceable, reviewable, and certifiable.
            </p>
            <div className="mt-5 space-y-2">
              {trustSignals.map((item) => (
                <div key={item} className="flex items-start gap-2 text-sm text-slate-200">
                  <BadgeCheck className="w-4 h-4 text-emerald-300 mt-0.5" />
                  <span>{item}</span>
                </div>
              ))}
            </div>
          </div>

          <motion.div
            className="rounded-2xl border border-cyan-500/30 bg-slate-950/70 p-5"
            animate={{ boxShadow: ["0 0 0 rgba(34,211,238,0.0)", "0 0 26px rgba(34,211,238,0.25)", "0 0 0 rgba(34,211,238,0.0)"] }}
            transition={{ duration: 3.4, repeat: Infinity, ease: "easeInOut" }}
          >
            <div className="inline-flex items-center gap-2 rounded-full border border-cyan-700/60 bg-cyan-900/20 px-3 py-1 text-[11px] uppercase tracking-wider text-cyan-200">
              <Bot className="w-3.5 h-3.5" /> Friday AI - Local Runtime
            </div>
            <p className="mt-4 text-sm text-slate-300">
              "Top 3 risks today: pending vendor GSTR-1 filings, dual-admin delays on high-value reversals, and
              unverified export fingerprints."
            </p>
            <div className="mt-5 rounded-lg border border-slate-800 bg-black/40 p-3 text-[11px] text-emerald-300 font-mono">
              READY &gt; M3 local model loaded
              <br />
              READY &gt; Compliance graph synced
              <br />
              READY &gt; Governance alerts armed
            </div>
            <div className="mt-5 flex gap-2">
              <Link
                to="/insights"
                className="inline-flex items-center gap-2 rounded-lg bg-cyan-500 hover:bg-cyan-400 text-slate-950 px-4 py-2 text-xs font-bold"
              >
                Open Friday Control Room
                <BrainCircuit className="w-3.5 h-3.5" />
              </Link>
              <Link
                to="/pricing"
                className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900 hover:bg-slate-800 px-4 py-2 text-xs"
              >
                Compare Plans
                <Sparkles className="w-3.5 h-3.5" />
              </Link>
            </div>
          </motion.div>
        </div>
      </section>

      <section className="px-4 sm:px-6 lg:px-8 pb-24">
        <div className="mx-auto max-w-6xl rounded-2xl border border-emerald-500/35 bg-emerald-900/10 p-6 sm:p-8">
          <div className="flex items-center gap-2 text-emerald-300">
            <FileBadge2 className="w-5 h-5" />
            <p className="text-xs uppercase tracking-[0.2em] font-semibold">Safe Harbor Defense</p>
          </div>
          <h3 className="mt-2 text-2xl font-bold">Institutional-grade certificate for GST auditor review</h3>
          <p className="mt-2 text-sm text-slate-300 max-w-3xl">
            Hover to preview the evidence footprint your CA can defend: MMB, legal basis under Section 50(3), and
            the immutable Accord integrity hash.
          </p>
          <motion.div
            whileHover={{ y: -4 }}
            className="mt-5 rounded-xl border border-emerald-600/45 bg-slate-950/70 p-4"
          >
            <p className="text-[11px] uppercase tracking-[0.2em] text-emerald-300">Certificate Preview</p>
            <p className="mt-2 text-sm text-slate-200">Accord Safe Harbor Certificate - Batch 90871</p>
            <p className="mt-1 text-xs text-slate-400">MMB: INR 3,45,000.00 | Interest Outcome: 0.0000% | Legal Basis: Sec_50(3)_Full_Cover</p>
            <p className="mt-2 text-[11px] text-emerald-300 font-mono break-all">Integrity Hash: 8ac30d7f9e2f89cc1ab0954f0bb18872e3cc1fd1a090f4b9ce42ff95f22165d0</p>
          </motion.div>
        </div>
      </section>
    </div>
  );
}
