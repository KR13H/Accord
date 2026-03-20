import { BrowserRouter, Link, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { motion } from "framer-motion";
import LandingPage from "./LandingPage";
import Dashboard from "./Dashboard";
import AiInsights from "./AiInsights";
import Ledger from "./Ledger";
import CaDashboard from "./CaDashboard";
import CAHeatmap from "./CAHeatmap";
import CaAccept from "./CaAccept";
import Pricing from "./Pricing";
import Signup from "./Signup";
import ReportsCenter from "./ReportsCenter";
import StarkStudio from "./StarkStudio";
import DraggableDashboard from "./DraggableDashboard";
import BalanceSheet from "./BalanceSheet";
import ProfitAndLoss from "./ProfitAndLoss";
import GstFiling from "./GstFiling";

const NAV_ITEMS = [
  { to: "/", label: "Home" },
  { to: "/pricing", label: "Pricing" },
  { to: "/dashboard", label: "Dashboard" },
  { to: "/ledger", label: "Ledger" },
  { to: "/insights", label: "Friday" },
  { to: "/reports", label: "Reports" },
  { to: "/drag", label: "Drag" },
  { to: "/balance-sheet", label: "BS" },
  { to: "/profit-loss", label: "P&L" },
  { to: "/gst-filing", label: "GST" },
  { to: "/studio", label: "Studio" },
  { to: "/signup", label: "Signup" },
  { to: "/ca/dashboard", label: "CA" },
  { to: "/ca/heatmap", label: "Heatmap" },
];

function TopRail() {
  const location = useLocation();

  return (
    <div className="fixed top-4 left-1/2 -translate-x-1/2 z-50">
      <div className="rounded-2xl border border-cyan-400/20 bg-slate-950/75 backdrop-blur-xl px-2 py-2 flex items-center gap-1">
        {NAV_ITEMS.map((item) => {
          const active = item.to === "/" ? location.pathname === "/" : location.pathname.startsWith(item.to);
          return (
            <Link
              key={item.to}
              to={item.to}
              className={`relative px-3 py-1.5 rounded-xl text-xs font-semibold transition-colors ${
                active ? "text-cyan-100" : "text-slate-300 hover:text-white"
              }`}
            >
              {active ? (
                <motion.span
                  layoutId="top-rail-active"
                  className="absolute inset-0 rounded-xl bg-cyan-500/20 border border-cyan-400/30"
                  transition={{ type: "spring", stiffness: 380, damping: 30 }}
                />
              ) : null}
              <span className="relative">{item.label}</span>
            </Link>
          );
        })}
      </div>
    </div>
  );
}

function BottomDock() {
  const location = useLocation();
  return (
    <footer className="border-t border-cyan-400/15 bg-slate-950/85 backdrop-blur-xl">
      <div className="mx-auto max-w-7xl px-4 sm:px-6 py-3 flex flex-wrap items-center justify-center gap-2 sm:gap-3">
        {NAV_ITEMS.map((item) => {
          const active = item.to === "/" ? location.pathname === "/" : location.pathname.startsWith(item.to);
          return (
            <Link
              key={`bottom-${item.to}`}
              to={item.to}
              className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors border ${
                active
                  ? "text-cyan-100 bg-cyan-500/20 border-cyan-400/40"
                  : "text-slate-300 bg-slate-900/70 border-slate-700/80 hover:text-white hover:border-cyan-500/40"
              }`}
            >
              {item.label}
            </Link>
          );
        })}
      </div>
    </footer>
  );
}

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-black text-white flex flex-col">
        <div
          className="fixed inset-0 pointer-events-none"
          style={{
            background:
              "radial-gradient(1100px 500px at 15% -10%, rgba(34,211,238,0.16), transparent 45%), radial-gradient(900px 460px at 90% 0%, rgba(255,255,255,0.08), transparent 40%)",
            willChange: "transform",
            transform: "translateZ(0)",
          }}
        />
        <TopRail />
        <div className="pt-20 flex-1">
          <Routes>
            <Route path="/" element={<LandingPage />} />
            <Route path="/pricing" element={<Pricing />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/ledger" element={<Ledger />} />
            <Route path="/insights" element={<AiInsights />} />
            <Route path="/reports" element={<ReportsCenter />} />
            <Route path="/drag" element={<DraggableDashboard />} />
            <Route path="/balance-sheet" element={<BalanceSheet />} />
            <Route path="/profit-loss" element={<ProfitAndLoss />} />
            <Route path="/gst-filing" element={<GstFiling />} />
            <Route path="/studio" element={<StarkStudio />} />
            <Route path="/signup" element={<Signup />} />
            <Route path="/ca/dashboard" element={<CaDashboard />} />
            <Route path="/ca/heatmap" element={<CAHeatmap />} />
            <Route path="/ca/accept/:token" element={<CaAccept />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </div>
        <BottomDock />
      </div>
    </BrowserRouter>
  );
}

export default App;
