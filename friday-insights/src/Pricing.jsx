import { Link } from "react-router-dom";
import { motion } from "framer-motion";
import { BadgeCheck, Crown, ShieldCheck, Sparkles } from "lucide-react";

const plans = [
  {
    name: "Starter",
    target: "Owner-led teams",
    price: "INR 2,999/mo",
    highlight: false,
    features: [
      "Rule 37A monitoring and alerting",
      "Vendor trust scoring",
      "Reversal workflow essentials",
      "Standard support",
    ],
  },
  {
    name: "Growth",
    target: "Internal finance ops",
    price: "INR 8,999/mo",
    highlight: false,
    features: [
      "Dual-admin approval controls",
      "Safe Harbor attestation workflow",
      "Export fingerprint verification",
      "Priority support",
    ],
  },
  {
    name: "CA Pro",
    target: "CA firms and audit practices",
    price: "INR 19,999/mo",
    highlight: true,
    features: [
      "Multi-client CA portal access",
      "Invite lifecycle and read-only audit views",
      "Advanced audit trail exports",
      "Dedicated onboarding",
    ],
  },
];

export default function Pricing() {
  return (
    <div className="min-h-screen bg-[#020617] text-slate-100 px-4 sm:px-6 lg:px-8 py-14">
      <div className="mx-auto max-w-6xl">
        <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-300/90 font-semibold">Pricing</p>
        <h1 className="mt-3 text-4xl sm:text-5xl font-black tracking-tight" style={{ fontFamily: '"Sora", "Space Grotesk", sans-serif' }}>
          Simple plans. Serious compliance posture.
        </h1>
        <p className="mt-4 text-slate-300 max-w-2xl">
          Choose the tier that matches your governance maturity. Upgrade as your volume, controls, and audit complexity grow.
        </p>

        <div className="mt-10 grid grid-cols-1 md:grid-cols-3 gap-5">
          {plans.map((plan, idx) => (
            <motion.div
              key={plan.name}
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: idx * 0.07 }}
              className={`rounded-2xl border p-5 ${
                plan.highlight
                  ? "border-cyan-400/60 bg-cyan-950/20 shadow-xl shadow-cyan-900/30"
                  : "border-slate-700/80 bg-slate-900/45"
              }`}
            >
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold">{plan.name}</h2>
                {plan.highlight ? <Crown className="w-4 h-4 text-cyan-300" /> : <ShieldCheck className="w-4 h-4 text-slate-400" />}
              </div>
              <p className="mt-1 text-xs text-slate-400 uppercase tracking-wider">{plan.target}</p>
              <p className="mt-4 text-2xl font-black text-cyan-200">{plan.price}</p>
              <div className="mt-4 space-y-2">
                {plan.features.map((feature) => (
                  <div key={feature} className="flex items-start gap-2 text-sm text-slate-200">
                    <BadgeCheck className="w-4 h-4 text-emerald-300 mt-0.5" />
                    <span>{feature}</span>
                  </div>
                ))}
              </div>
              <Link
                to="/insights"
                className={`mt-5 inline-flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-semibold ${
                  plan.highlight
                    ? "bg-cyan-500 hover:bg-cyan-400 text-slate-950"
                    : "border border-slate-700 bg-slate-900 hover:bg-slate-800"
                }`}
              >
                {plan.highlight ? "Book CA Demo" : "Start Free Scan"}
                <Sparkles className="w-4 h-4" />
              </Link>
            </motion.div>
          ))}
        </div>

        <div className="mt-10 rounded-2xl border border-slate-700/80 bg-slate-900/40 p-5 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <p className="text-sm font-semibold">Enterprise</p>
            <p className="text-sm text-slate-400">Custom controls, SSO, SLA, and implementation support.</p>
          </div>
          <Link
            to="/insights"
            className="inline-flex items-center gap-2 rounded-lg border border-cyan-500/50 bg-cyan-900/20 hover:bg-cyan-900/35 px-4 py-2 text-sm font-semibold"
          >
            Talk to Compliance Team
          </Link>
        </div>
      </div>
    </div>
  );
}
