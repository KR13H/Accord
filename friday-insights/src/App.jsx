import { BrowserRouter, Link, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { motion } from "framer-motion";
import { useEffect, useMemo, useState } from "react";
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
import VendorPortal from "./components/Vendors/VendorPortal";
import SupportChatWidget from "./components/AI/SupportChatWidget";
import { ChatProvider } from "./context/ChatContext";

const LANGUAGE_ORDER = ["en", "hi", "pa", "ur"];

const LANGUAGE_NAMES = {
  en: "English",
  hi: "हिंदी",
  pa: "ਪੰਜਾਬੀ",
  ur: "اردو",
};

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

const NAV_LABELS_HI = {
  Home: "होम",
  Pricing: "मूल्य",
  Dashboard: "डैशबोर्ड",
  Ledger: "लेजर",
  Friday: "फ्राइडे",
  Reports: "रिपोर्ट",
  Drag: "ड्रैग",
  BS: "बैलेंस शीट",
  "P&L": "लाभ-हानि",
  GST: "जीएसटी",
  Studio: "स्टूडियो",
  Signup: "साइनअप",
  CA: "सीए",
  Heatmap: "हीटमैप",
};

const NAV_LABELS_PA = {
  Home: "ਹੋਮ",
  Pricing: "ਕੀਮਤ",
  Dashboard: "ਡੈਸ਼ਬੋਰਡ",
  Ledger: "ਲੇਜਰ",
  Friday: "ਫ੍ਰਾਈਡੇ",
  Reports: "ਰਿਪੋਰਟਾਂ",
  Drag: "ਡ੍ਰੈਗ",
  BS: "ਬੈਲੇਂਸ ਸ਼ੀਟ",
  "P&L": "ਮੁਨਾਫਾ-ਨੁਕਸਾਨ",
  GST: "ਜੀਐਸਟੀ",
  Studio: "ਸਟੂਡੀਓ",
  Signup: "ਸਾਈਨਅਪ",
  CA: "ਸੀਏ",
  Heatmap: "ਹੀਟਮੈਪ",
};

const NAV_LABELS_UR = {
  Home: "ہوم",
  Pricing: "قیمت",
  Dashboard: "ڈیش بورڈ",
  Ledger: "لیجر",
  Friday: "فرائیڈے",
  Reports: "رپورٹس",
  Drag: "ڈریگ",
  BS: "بیلنس شیٹ",
  "P&L": "منافع و نقصان",
  GST: "جی ایس ٹی",
  Studio: "اسٹوڈیو",
  Signup: "سائن اپ",
  CA: "سی اے",
  Heatmap: "ہیٹ میپ",
};

function navLabelForLanguage(language, label) {
  if (language === "hi") return NAV_LABELS_HI[label] || label;
  if (language === "pa") return NAV_LABELS_PA[label] || label;
  if (language === "ur") return NAV_LABELS_UR[label] || label;
  return label;
}

function TopRail({ language, simpleMode, onToggleLanguage, onToggleSimpleMode }) {
  const location = useLocation();
  const navItems = useMemo(
    () =>
      NAV_ITEMS.map((item) => ({
        ...item,
        label: navLabelForLanguage(language, item.label),
      })),
    [language]
  );

  return (
    <div className="fixed top-4 left-1/2 -translate-x-1/2 z-50">
      <div className="rounded-2xl border border-cyan-400/20 bg-slate-950/75 backdrop-blur-xl px-2 py-2 flex items-center gap-1 flex-wrap justify-center">
        <button
          type="button"
          onClick={onToggleLanguage}
          className="px-3 py-1.5 rounded-xl text-xs font-semibold text-cyan-100 border border-cyan-400/35 bg-cyan-500/15"
        >
          {LANGUAGE_NAMES[language]}
        </button>
        <button
          type="button"
          onClick={onToggleSimpleMode}
          className="px-3 py-1.5 rounded-xl text-xs font-semibold text-cyan-100 border border-cyan-400/35 bg-cyan-500/15"
        >
          {simpleMode
            ? language === "hi"
              ? "सरल मोड चालू"
              : language === "pa"
                ? "ਸਧਾਰਨ ਮੋਡ ਚਾਲੂ"
                : language === "ur"
                  ? "سادہ موڈ آن"
                  : "Simple Mode On"
            : language === "hi"
              ? "सरल मोड बंद"
              : language === "pa"
                ? "ਸਧਾਰਨ ਮੋਡ ਬੰਦ"
                : language === "ur"
                  ? "سادہ موڈ آف"
                  : "Simple Mode Off"}
        </button>
        {navItems.map((item) => {
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

function BottomDock({ language }) {
  const location = useLocation();
  const navItems = useMemo(
    () =>
      NAV_ITEMS.map((item) => ({
        ...item,
        label: navLabelForLanguage(language, item.label),
      })),
    [language]
  );
  return (
    <footer className="border-t border-cyan-400/15 bg-slate-950/85 backdrop-blur-xl">
      <div className="mx-auto max-w-7xl px-4 sm:px-6 py-3 flex flex-wrap items-center justify-center gap-2 sm:gap-3">
        {navItems.map((item) => {
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
  const [language, setLanguage] = useState("en");
  const [simpleMode, setSimpleMode] = useState(true);

  useEffect(() => {
    document.documentElement.lang = language;
  }, [language]);

  return (
    <ChatProvider>
      <BrowserRouter>
        <div className={`min-h-screen bg-black text-white flex flex-col ${simpleMode ? "simple-mode" : ""}`}>
          <div
            className="fixed inset-0 pointer-events-none"
            style={{
              background:
                "radial-gradient(1100px 500px at 15% -10%, rgba(34,211,238,0.16), transparent 45%), radial-gradient(900px 460px at 90% 0%, rgba(255,255,255,0.08), transparent 40%)",
              willChange: "transform",
              transform: "translateZ(0)",
            }}
          />
          <TopRail
            language={language}
            simpleMode={simpleMode}
            onToggleLanguage={() =>
              setLanguage((prev) => LANGUAGE_ORDER[(LANGUAGE_ORDER.indexOf(prev) + 1) % LANGUAGE_ORDER.length])
            }
            onToggleSimpleMode={() => setSimpleMode((prev) => !prev)}
          />
          <div className="pt-20 flex-1">
            <Routes>
              <Route path="/" element={<LandingPage language={language} simpleMode={simpleMode} />} />
              <Route path="/pricing" element={<Pricing />} />
              <Route path="/dashboard" element={<Dashboard />} />
              <Route path="/ledger" element={<Ledger />} />
              <Route path="/insights" element={<AiInsights />} />
              <Route path="/reports" element={<ReportsCenter language={language} simpleMode={simpleMode} />} />
              <Route path="/drag" element={<DraggableDashboard />} />
              <Route path="/balance-sheet" element={<BalanceSheet />} />
              <Route path="/profit-loss" element={<ProfitAndLoss />} />
              <Route path="/gst-filing" element={<GstFiling />} />
              <Route path="/studio" element={<StarkStudio />} />
              <Route path="/signup" element={<Signup />} />
              <Route path="/ca/dashboard" element={<CaDashboard />} />
              <Route path="/ca/heatmap" element={<CAHeatmap />} />
              <Route path="/ca/accept/:token" element={<CaAccept />} />
              <Route path="/vendor/:vendorLinkId" element={<VendorPortal />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </div>
          <BottomDock language={language} />
          <SupportChatWidget />
        </div>
      </BrowserRouter>
    </ChatProvider>
  );
}

export default App;
