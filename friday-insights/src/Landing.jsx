import { useState } from "react";
import {
  BarChart3,
  ShieldCheck,
  Brain,
  Database,
  Smartphone,
  CheckCircle2,
  ArrowRight,
  Menu,
  X,
  ChevronRight,
  Zap,
  Mail,
} from "lucide-react";

// WhatsApp SVG — not in lucide-react
const WhatsAppIcon = ({ className = "w-5 h-5" }) => (
  <svg viewBox="0 0 24 24" className={className} fill="currentColor" xmlns="http://www.w3.org/2000/svg">
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413z" />
  </svg>
);

// ─── Data ─────────────────────────────────────────────────────────────────────

const NAV_LINKS = [
  { label: "Features", href: "#features" },
  { label: "How It Works", href: "#how-it-works" },
  { label: "Contact", href: "#contact" },
];

const STATS = [
  { value: "500+", label: "SMEs Onboarded" },
  { value: "3 hrs", label: "Saved per day" },
  { value: "99.9%", label: "Report accuracy" },
  { value: "₹0", label: "Manual effort" },
];

const VALUE_PROPS = [
  {
    id: "no-entry",
    icon: <Database className="w-6 h-6" />,
    title: "Zero Manual Data Entry",
    description:
      "Direct integration with Tally Prime, ERP 9, and Busy. No exports, no CSV uploads, no human error—your data flows automatically, every single day.",
    accent: "blue",
    points: ["Tally Prime & ERP 9 support", "Busy 21 & legacy versions", "Real-time sync, zero touch"],
  },
  {
    id: "gst",
    icon: <ShieldCheck className="w-6 h-6" />,
    title: "Instant GST Compliance Overview",
    description:
      "Auto-generated GSTR-3B summaries, ITC reconciliation alerts, and filing deadline reminders—delivered to WhatsApp before your CA opens their laptop.",
    accent: "green",
    points: ["GSTR-1 & GSTR-3B summaries", "ITC mismatch alerts", "Deadline reminders on WhatsApp"],
  },
  {
    id: "ai",
    icon: <Brain className="w-6 h-6" />,
    title: "AI-Driven Cross-Checked Analytics",
    description:
      "Our intelligence engine validates your books, flags discrepancies between ledgers, and surfaces P&L insights you can act on in seconds—not days.",
    accent: "purple",
    points: ["Ledger discrepancy detection", "Trend analysis & forecasting", "Natural language summaries"],
  },
];

const HOW_IT_WORKS = [
  {
    step: "01",
    icon: <Database className="w-6 h-6" />,
    title: "Connect Your Books",
    description: "Install our lightweight agent on your Tally or Busy system. One-time setup in under 5 minutes.",
  },
  {
    step: "02",
    icon: <Brain className="w-6 h-6" />,
    title: "We Process Everything",
    description: "Our cloud engine extracts, validates, and generates structured financial reports automatically—every single day.",
  },
  {
    step: "03",
    icon: <Smartphone className="w-6 h-6" />,
    title: "Receive on WhatsApp",
    description: "Clean, readable summaries arrive in your WhatsApp every morning. Forward instantly to your CA or investors.",
  },
];

const ACCENT = {
  blue:   { border: "rgba(59,130,246,0.3)",   shadow: "rgba(59,130,246,0.08)",   icon: "bg-blue-500/15 text-blue-400",   check: "text-blue-400"   },
  green:  { border: "rgba(37,211,102,0.3)",   shadow: "rgba(37,211,102,0.07)",   icon: "bg-emerald-500/15 text-emerald-400", check: "text-emerald-400" },
  purple: { border: "rgba(139,92,246,0.3)",   shadow: "rgba(139,92,246,0.07)",   icon: "bg-violet-500/15 text-violet-400",   check: "text-violet-400"  },
};

const DEMO_HREF = "mailto:mannssocialmedia@gmail.com?subject=Demo%20Request%20%E2%80%94%20Friday%20Insights";

// ─── Component ────────────────────────────────────────────────────────────────

export default function Landing() {
  const [menuOpen, setMenuOpen] = useState(false);

  return (
    <div className="min-h-screen text-white overflow-x-hidden" style={{ background: "#020817" }}>

      {/* ── Ambient gradient orbs ───────────────────────────────────────────── */}
      <div className="fixed inset-0 pointer-events-none overflow-hidden" aria-hidden="true">
        <div
          className="absolute -top-60 -left-60 w-[800px] h-[800px] rounded-full opacity-20"
          style={{ background: "radial-gradient(circle, #1d4ed8 0%, transparent 65%)" }}
        />
        <div
          className="absolute top-1/2 -right-80 w-[600px] h-[600px] rounded-full opacity-10"
          style={{ background: "radial-gradient(circle, #25D366 0%, transparent 65%)" }}
        />
        <div
          className="absolute bottom-10 left-1/3 w-[500px] h-[500px] rounded-full opacity-10"
          style={{ background: "radial-gradient(circle, #7c3aed 0%, transparent 65%)" }}
        />
      </div>

      {/* ── Navbar ──────────────────────────────────────────────────────────── */}
      <header
        className="sticky top-0 z-50 border-b border-white/5"
        style={{ background: "rgba(2,8,23,0.82)", backdropFilter: "blur(20px)" }}
      >
        <div className="max-w-7xl mx-auto px-4 sm:px-6 flex items-center justify-between h-16">
          {/* Logo */}
          <a href="/" className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center flex-shrink-0">
              <BarChart3 className="w-4 h-4 text-white" />
            </div>
            <span className="font-bold text-lg tracking-tight select-none">
              Friday <span className="text-blue-400">Insights</span>
            </span>
          </a>

          {/* Desktop nav */}
          <nav className="hidden md:flex items-center gap-8" aria-label="Main navigation">
            {NAV_LINKS.map((l) => (
              <a key={l.label} href={l.href} className="text-sm text-slate-400 hover:text-white transition-colors duration-200">
                {l.label}
              </a>
            ))}
          </nav>

          {/* Desktop CTA */}
          <div className="hidden md:flex items-center gap-3">
            <a
              href={DEMO_HREF}
              className="inline-flex items-center gap-2 bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold px-4 py-2 rounded-lg transition-all duration-200 hover:shadow-lg hover:shadow-blue-500/25"
            >
              Book a Demo <ArrowRight className="w-3.5 h-3.5" />
            </a>
          </div>

          {/* Mobile toggle */}
          <button
            className="md:hidden p-2 text-slate-400 hover:text-white transition-colors rounded-lg"
            onClick={() => setMenuOpen(!menuOpen)}
            aria-label="Toggle menu"
            aria-expanded={menuOpen}
          >
            {menuOpen ? <X className="w-5 h-5" /> : <Menu className="w-5 h-5" />}
          </button>
        </div>

        {/* Mobile drawer */}
        {menuOpen && (
          <div className="md:hidden border-t border-white/5 px-4 pb-5 pt-2">
            {NAV_LINKS.map((l) => (
              <a
                key={l.label}
                href={l.href}
                className="block py-3 text-slate-300 hover:text-white border-b border-white/5 text-sm"
                onClick={() => setMenuOpen(false)}
              >
                {l.label}
              </a>
            ))}
            <a
              href={DEMO_HREF}
              className="mt-4 w-full flex items-center justify-center gap-2 bg-blue-600 text-white text-sm font-semibold px-4 py-3 rounded-lg"
              onClick={() => setMenuOpen(false)}
            >
              Book a Demo <ArrowRight className="w-3.5 h-3.5" />
            </a>
          </div>
        )}
      </header>

      {/* ── Hero ────────────────────────────────────────────────────────────── */}
      <section className="relative pt-20 pb-28 px-4 sm:px-6 max-w-7xl mx-auto">
        {/* Trust badge */}
        <div className="flex justify-center mb-8">
          <span className="inline-flex items-center gap-2 bg-white/5 border border-white/10 rounded-full text-xs font-medium px-4 py-1.5 text-slate-300">
            <span className="w-2 h-2 rounded-full bg-green-400 animate-pulse flex-shrink-0" />
            Trusted by 500+ Indian SMEs · Tally &amp; Busy supported
          </span>
        </div>

        {/* Headline */}
        <h1 className="text-4xl sm:text-5xl md:text-6xl lg:text-[4.25rem] font-extrabold text-center leading-[1.08] tracking-tight max-w-5xl mx-auto">
          Your Tally &amp; Busy Reports,
          <br className="hidden sm:block" />
          <span
            style={{
              background: "linear-gradient(130deg, #60a5fa 10%, #34d399 90%)",
              WebkitBackgroundClip: "text",
              WebkitTextFillColor: "transparent",
            }}
          >
            {" "}Delivered to WhatsApp.
          </span>
          <br />
          <span className="text-slate-200">Automatically.</span>
        </h1>

        {/* Sub-headline */}
        <p className="mt-6 text-base sm:text-lg md:text-xl text-slate-400 text-center max-w-2xl mx-auto leading-relaxed">
          Stop burning your CA's valuable hours on manual MIS reports. Friday Insights
          extracts, validates, and delivers audit-ready financial summaries straight to
          WhatsApp&nbsp;— every morning, without fail.
        </p>

        {/* CTA buttons */}
        <div className="mt-10 flex flex-col sm:flex-row items-center justify-center gap-4">
          <a
            href={DEMO_HREF}
            className="inline-flex items-center gap-2.5 bg-blue-600 hover:bg-blue-500 text-white font-semibold px-7 py-3.5 rounded-xl transition-all duration-200 hover:shadow-xl hover:shadow-blue-500/30 hover:-translate-y-0.5 text-sm sm:text-base w-full sm:w-auto justify-center"
          >
            <WhatsAppIcon className="w-4 h-4" />
            Book a Free Demo
          </a>
          <a
            href="#how-it-works"
            className="inline-flex items-center gap-2 text-slate-300 hover:text-white border border-white/10 hover:border-white/25 px-7 py-3.5 rounded-xl bg-white/5 hover:bg-white/10 transition-all duration-200 text-sm sm:text-base w-full sm:w-auto justify-center"
          >
            See How It Works <ChevronRight className="w-4 h-4" />
          </a>
        </div>

        {/* WhatsApp message mockup */}
        <div className="mt-20 flex justify-center">
          <div className="relative w-full max-w-xs sm:max-w-sm">
            <div
              className="rounded-3xl overflow-hidden shadow-2xl shadow-blue-950/60 border border-white/10"
              style={{ background: "rgba(255,255,255,0.04)", backdropFilter: "blur(12px)" }}
            >
              {/* WhatsApp header bar */}
              <div className="flex items-center gap-3 px-4 py-3" style={{ background: "#075E54" }}>
                <div className="w-9 h-9 rounded-full bg-green-200 flex items-center justify-center text-xs font-extrabold text-green-900 flex-shrink-0">
                  FI
                </div>
                <div className="min-w-0">
                  <p className="text-white text-sm font-semibold leading-none">Friday Insights</p>
                  <p className="text-green-200 text-xs mt-0.5 flex items-center gap-1">
                    <span className="w-1.5 h-1.5 rounded-full bg-green-300 inline-block" />
                    Daily report ready
                  </p>
                </div>
              </div>

              {/* Chat body */}
              <div className="p-4 space-y-3" style={{ background: "#0B1418" }}>
                {/* Incoming message */}
                <div
                  className="rounded-2xl rounded-tl-none p-4 max-w-[90%]"
                  style={{ background: "#1F2C34" }}
                >
                  <p className="text-green-400 text-xs font-bold mb-2.5">
                    📊 Friday Insights — 16 Mar 2026
                  </p>
                  <p className="text-slate-200 text-xs leading-5">
                    <span className="text-white font-semibold">Krish &amp; Co. Pvt. Ltd.</span>
                    <br />
                    <br />
                    <span className="text-green-400 font-medium">Revenue (MTD):</span> ₹14,82,500{" "}
                    <span className="text-green-400">↑ 12%</span>
                    <br />
                    <span className="text-red-400 font-medium">Outstanding Payables:</span> ₹3,12,000
                    <br />
                    <span className="text-blue-400 font-medium">GST Liability (Mar):</span> ₹1,44,300
                    <br />
                    <span className="text-yellow-400 font-medium">ITC Available:</span> ₹88,200
                    <br />
                    <br />
                    <span className="text-slate-400">
                      ⚠️ 2 ledger discrepancies flagged for review.
                    </span>
                  </p>
                  <p className="text-slate-500 text-right text-xs mt-3">9:01 AM ✓✓</p>
                </div>

                {/* Outgoing reply */}
                <div
                  className="rounded-2xl rounded-tr-none p-3 max-w-[70%] ml-auto"
                  style={{ background: "#005C4B" }}
                >
                  <p className="text-white text-xs">Excellent! Forwarding to CA now 🙌</p>
                  <p className="text-slate-400 text-right text-xs mt-1">9:02 AM ✓✓</p>
                </div>
              </div>
            </div>

            {/* Live badge */}
            <div className="absolute -top-3 -right-3 bg-green-500 text-white text-xs font-extrabold px-3 py-1 rounded-full shadow-lg shadow-green-500/40 tracking-wide">
              LIVE ●
            </div>
          </div>
        </div>
      </section>

      {/* ── Stats band ──────────────────────────────────────────────────────── */}
      <div className="border-y border-white/5 py-10" style={{ background: "rgba(255,255,255,0.018)" }}>
        <div className="max-w-5xl mx-auto px-4 sm:px-6 grid grid-cols-2 sm:grid-cols-4 gap-8 text-center">
          {STATS.map((s) => (
            <div key={s.label}>
              <p className="text-3xl sm:text-4xl font-extrabold text-white tracking-tight">{s.value}</p>
              <p className="text-xs sm:text-sm text-slate-400 mt-1.5">{s.label}</p>
            </div>
          ))}
        </div>
      </div>

      {/* ── Value Propositions ──────────────────────────────────────────────── */}
      <section id="features" className="py-24 px-4 sm:px-6 max-w-7xl mx-auto">
        <div className="text-center mb-16">
          <span className="text-blue-400 text-xs font-bold uppercase tracking-[0.2em]">Why Friday Insights</span>
          <h2 className="mt-4 text-3xl sm:text-4xl md:text-5xl font-extrabold tracking-tight">
            Why leading SMEs are switching
          </h2>
          <p className="mt-4 text-slate-400 text-base sm:text-lg max-w-2xl mx-auto">
            Manual reporting is costing you money, time, and accuracy. We fix all three — simultaneously.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {VALUE_PROPS.map((vp) => {
            const a = ACCENT[vp.accent];
            return (
              <div
                key={vp.id}
                className="group relative rounded-2xl p-7 border transition-all duration-300 hover:-translate-y-1.5 cursor-default"
                style={{
                  background: "linear-gradient(145deg, rgba(255,255,255,0.045) 0%, rgba(255,255,255,0.015) 100%)",
                  borderColor: a.border,
                  boxShadow: `0 0 50px ${a.shadow}`,
                }}
              >
                <div className={`w-12 h-12 rounded-xl flex items-center justify-center mb-5 ${a.icon}`}>
                  {vp.icon}
                </div>
                <h3 className="text-lg font-bold text-white mb-3">{vp.title}</h3>
                <p className="text-slate-400 text-sm leading-relaxed mb-5">{vp.description}</p>
                <ul className="space-y-2.5">
                  {vp.points.map((pt) => (
                    <li key={pt} className="flex items-center gap-2.5 text-xs text-slate-300">
                      <CheckCircle2 className={`w-4 h-4 flex-shrink-0 ${a.check}`} />
                      {pt}
                    </li>
                  ))}
                </ul>
              </div>
            );
          })}
        </div>
      </section>

      {/* ── How It Works ────────────────────────────────────────────────────── */}
      <section id="how-it-works" className="py-24 px-4 sm:px-6" style={{ background: "rgba(255,255,255,0.015)" }}>
        <div className="max-w-5xl mx-auto">
          <div className="text-center mb-16">
            <span className="text-blue-400 text-xs font-bold uppercase tracking-[0.2em]">Simple Setup</span>
            <h2 className="mt-4 text-3xl sm:text-4xl md:text-5xl font-extrabold tracking-tight">
              Up and running in minutes
            </h2>
            <p className="mt-4 text-slate-400 text-base sm:text-lg max-w-xl mx-auto">
              No IT team required. No complex integrations. Just connect and watch it work.
            </p>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-10 relative">
            {/* Connector line — desktop */}
            <div
              className="hidden sm:block absolute top-10 left-[calc(16.67%+2.5rem)] right-[calc(16.67%+2.5rem)] h-px"
              style={{ background: "linear-gradient(90deg, rgba(59,130,246,0.3), rgba(59,130,246,0.6), rgba(52,211,153,0.3))" }}
            />
            {HOW_IT_WORKS.map((step) => (
              <div key={step.step} className="relative flex flex-col items-center text-center">
                <div className="relative mb-5">
                  <div
                    className="w-20 h-20 rounded-2xl border border-white/10 flex items-center justify-center relative z-10"
                    style={{ background: "linear-gradient(145deg, rgba(255,255,255,0.07) 0%, rgba(255,255,255,0.02) 100%)" }}
                  >
                    <span className="text-blue-400">{step.icon}</span>
                  </div>
                  <span className="absolute -top-3 -right-2 text-[11px] font-black text-blue-500/50 tabular-nums">
                    {step.step}
                  </span>
                </div>
                <h3 className="text-base font-bold text-white mb-2">{step.title}</h3>
                <p className="text-slate-400 text-sm leading-relaxed">{step.description}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── CTA Banner ──────────────────────────────────────────────────────── */}
      <section className="py-24 px-4 sm:px-6">
        <div className="max-w-4xl mx-auto">
          <div
            className="relative overflow-hidden rounded-3xl p-10 sm:p-16 text-center border border-blue-500/20"
            style={{
              background: "linear-gradient(135deg, rgba(29,78,216,0.3) 0%, rgba(37,211,102,0.12) 100%)",
              backdropFilter: "blur(24px)",
            }}
          >
            {/* Radial highlight */}
            <div
              className="absolute inset-0 pointer-events-none"
              aria-hidden="true"
              style={{ background: "radial-gradient(ellipse at 50% -20%, rgba(59,130,246,0.25) 0%, transparent 65%)" }}
            />

            <span className="relative inline-flex items-center gap-2 bg-blue-500/15 border border-blue-400/25 rounded-full text-blue-300 text-xs font-semibold px-4 py-1.5 mb-6">
              <Zap className="w-3 h-3" /> Limited onboarding slots available
            </span>

            <h2 className="relative text-3xl sm:text-4xl md:text-[2.75rem] font-extrabold tracking-tight mb-4 leading-tight">
              Ready to automate your
              <br />
              <span
                style={{
                  background: "linear-gradient(90deg, #60a5fa, #34d399)",
                  WebkitBackgroundClip: "text",
                  WebkitTextFillColor: "transparent",
                }}
              >
                financial reporting?
              </span>
            </h2>

            <p className="relative text-slate-300 text-base sm:text-lg mb-10 max-w-xl mx-auto">
              Book a free 30-minute demo and see live how Friday Insights turns your Tally or Busy
              data into actionable WhatsApp reports.
            </p>

            <a
              href={DEMO_HREF}
              className="relative inline-flex items-center gap-3 bg-white text-slate-900 font-bold px-8 py-4 rounded-xl hover:bg-blue-50 transition-all duration-200 hover:shadow-2xl hover:-translate-y-0.5 text-sm sm:text-base"
            >
              <WhatsAppIcon className="w-5 h-5 text-green-600" />
              Book Your Free Demo
              <ArrowRight className="w-4 h-4" />
            </a>

            <p className="relative mt-4 text-slate-500 text-xs">
              No credit card required · Setup in under 5 minutes · Cancel anytime
            </p>
          </div>
        </div>
      </section>

      {/* ── Footer ──────────────────────────────────────────────────────────── */}
      <footer
        id="contact"
        className="border-t border-white/5 py-14 px-4 sm:px-6"
        style={{ background: "rgba(0,0,0,0.35)" }}
      >
        <div className="max-w-7xl mx-auto">
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-10 mb-12">
            {/* Brand */}
            <div>
              <div className="flex items-center gap-2.5 mb-4">
                <div className="w-7 h-7 rounded-lg bg-blue-600 flex items-center justify-center flex-shrink-0">
                  <BarChart3 className="w-3.5 h-3.5 text-white" />
                </div>
                <span className="font-bold text-base tracking-tight">
                  Friday <span className="text-blue-400">Insights</span>
                </span>
              </div>
              <p className="text-slate-400 text-sm leading-relaxed max-w-[260px]">
                Automating financial reporting for India's SME backbone. From Tally &amp; Busy to
                WhatsApp — seamlessly.
              </p>
            </div>

            {/* Product links */}
            <div>
              <p className="text-white text-sm font-semibold mb-5">Product</p>
              <ul className="space-y-3 text-sm text-slate-400">
                <li>
                  <a href="#features" className="hover:text-white transition-colors">
                    Features
                  </a>
                </li>
                <li>
                  <a href="#how-it-works" className="hover:text-white transition-colors">
                    How It Works
                  </a>
                </li>
                <li>
                  <a href={DEMO_HREF} className="hover:text-white transition-colors">
                    Book a Demo
                  </a>
                </li>
              </ul>
            </div>

            {/* Contact */}
            <div>
              <p className="text-white text-sm font-semibold mb-5">Get in Touch</p>
              <a
                href="mailto:mannssocialmedia@gmail.com"
                className="inline-flex items-center gap-2 text-blue-400 hover:text-blue-300 transition-colors text-sm font-medium"
              >
                <Mail className="w-4 h-4 flex-shrink-0" />
                mannssocialmedia@gmail.com
              </a>
              <p className="mt-2 text-slate-500 text-xs">We typically respond within 2 business hours.</p>
              <a
                href={DEMO_HREF}
                className="mt-5 inline-flex items-center gap-2 bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold px-4 py-2.5 rounded-lg transition-all duration-200"
              >
                Book a Free Demo <ArrowRight className="w-3 h-3" />
              </a>
            </div>
          </div>

          {/* Bottom bar */}
          <div className="border-t border-white/5 pt-6 flex flex-col sm:flex-row items-center justify-between gap-3 text-xs text-slate-500">
            <p>© 2026 Friday Insights. All rights reserved.</p>
            <p>Made with ♥ for India's SME backbone</p>
          </div>
        </div>
      </footer>
    </div>
  );
}
