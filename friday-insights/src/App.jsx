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
import BrokerPortal from "./components/Brokers/BrokerPortal";
import GodModeDashboard from "./components/Admin/GodModeDashboard";
import SmeDashboard from "./components/SME/SmeDashboard";
import PasskeyLogin from "./components/SME/PasskeyLogin";
import QuickSaleTerminal from "./components/SME/QuickSaleTerminal";
import InventoryManager from "./components/SME/InventoryManager";
import SupplierPortal from "./components/SME/SupplierPortal";
import VendorPortal from "./components/Vendors/VendorPortal";
import SupportChatWidget from "./components/AI/SupportChatWidget";
import { ChatProvider } from "./context/ChatContext";
import { flushQueuedSales } from "./api/offlineQueue";
import { initMeshSync } from "./api/meshSync";
import { getStoredSmeUsername } from "./api/smeAuth";

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
  { to: "/sme-pos", label: "SME POS" },
  { to: "/sme-dashboard", label: "SME Dash" },
  { to: "/sme-inventory", label: "Inventory" },
  { to: "/supplier-portal", label: "Supplier" },
  { to: "/god-mode", label: "God Mode" },
  { to: "/brokers/register", label: "Brokers" },
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
  "SME POS": "एसएमई पीओएस",
  "SME Dash": "एसएमई डैश",
  Inventory: "इन्वेंटरी",
  Supplier: "सप्लायर",
  "God Mode": "गॉड मोड",
  Brokers: "ब्रोकर",
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
  "SME POS": "ਐਸਐਮਈ ਪੀਓਐਸ",
  "SME Dash": "ਐਸਐਮਈ ਡੈਸ਼",
  Inventory: "ਇਨਵੈਂਟਰੀ",
  Supplier: "ਸਪਲਾਇਰ",
  "God Mode": "ਗਾਡ ਮੋਡ",
  Brokers: "ਬ੍ਰੋਕਰ",
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
  "SME POS": "ایس ایم ای پی او ایس",
  "SME Dash": "ایس ایم ای ڈیش",
  Inventory: "انوینٹری",
  Supplier: "سپلائر",
  "God Mode": "گاڈ موڈ",
  Brokers: "بروکر",
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

function visibleNavItemsForRole(role) {
  const normalizedRole = (role || "").trim().toLowerCase();
  if (normalizedRole !== "cashier") {
    return NAV_ITEMS;
  }
  return NAV_ITEMS.filter((item) => item.label !== "Dashboard" && item.label !== "Udhaar");
}

function TopRail({ language, simpleMode, onToggleLanguage, onToggleSimpleMode, smeRole }) {
  const location = useLocation();
  const navItems = useMemo(
    () =>
      visibleNavItemsForRole(smeRole).map((item) => ({
        ...item,
        label: navLabelForLanguage(language, item.label),
      })),
    [language, smeRole]
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

function BottomDock({ language, smeRole }) {
  const location = useLocation();
  const navItems = useMemo(
    () =>
      visibleNavItemsForRole(smeRole).map((item) => ({
        ...item,
        label: navLabelForLanguage(language, item.label),
      })),
    [language, smeRole]
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
  const [syncEvent, setSyncEvent] = useState(null);
  const [meshStatus, setMeshStatus] = useState("");
  const [darkMode, setDarkMode] = useState(() => window.localStorage.getItem("theme") === "dark");
  const [smeRole, setSmeRole] = useState(() => {
    const savedRole = window.localStorage.getItem("smeRole");
    return savedRole && savedRole.trim() ? savedRole.trim().toLowerCase() : "owner";
  });

  useEffect(() => {
    document.documentElement.lang = language;
  }, [language]);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", darkMode);
    window.localStorage.setItem("theme", darkMode ? "dark" : "light");
  }, [darkMode]);

  useEffect(() => {
    window.localStorage.setItem("smeRole", smeRole);
  }, [smeRole]);

  useEffect(() => {
    const syncQueuedSales = async () => {
      if (!navigator.onLine) {
        return;
      }
      await flushQueuedSales();
    };

    syncQueuedSales();
    window.addEventListener("online", syncQueuedSales);
    return () => window.removeEventListener("online", syncQueuedSales);
  }, []);

  useEffect(() => {
    const stopMesh = initMeshSync({
      businessId: "SME-001",
      onStatus: setMeshStatus,
    });

    return () => {
      if (typeof stopMesh === "function") {
        stopMesh();
      }
    };
  }, []);

  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    const backendWsBase =
      import.meta.env.VITE_WS_BASE_URL || `${protocol}://${window.location.hostname}:8000`;
    const wsUrl = `${backendWsBase}/api/v1/sme/ws/sync?business_id=SME-001`;
    const socket = new WebSocket(wsUrl);

    socket.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        setSyncEvent(payload);
      } catch {
        // Ignore malformed websocket payloads.
      }
    };

    const keepAliveId = window.setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ type: "PING" }));
      }
    }, 25000);

    return () => {
      window.clearInterval(keepAliveId);
      socket.close();
    };
  }, []);

  useEffect(() => {
    const isTypingContext = () => {
      const element = document.activeElement;
      if (!element) return false;
      const tag = String(element.tagName || "").toLowerCase();
      return tag === "input" || tag === "textarea" || element.isContentEditable;
    };

    const dispatchPosShortcut = (action) => {
      window.dispatchEvent(new CustomEvent("accord:pos-shortcut", { detail: { action } }));
    };

    const handleGlobalKeydown = (event) => {
      if (!window.location.pathname.startsWith("/sme-pos")) {
        return;
      }
      if (isTypingContext()) {
        return;
      }

      if (event.key === "F2") {
        event.preventDefault();
        dispatchPosShortcut("focus-amount");
        return;
      }
      if (event.code === "Space") {
        event.preventDefault();
        dispatchPosShortcut("exact-cash");
        return;
      }
      if (event.key === "Escape") {
        event.preventDefault();
        dispatchPosShortcut("clear");
      }
    };

    window.addEventListener("keydown", handleGlobalKeydown);
    return () => window.removeEventListener("keydown", handleGlobalKeydown);
  }, []);

  return (
    <ChatProvider>
      <BrowserRouter>
        <div className={`min-h-screen bg-slate-100 text-slate-900 dark:bg-black dark:text-white flex flex-col ${simpleMode ? "simple-mode" : ""}`}>
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
            smeRole={smeRole}
            onToggleLanguage={() =>
              setLanguage((prev) => LANGUAGE_ORDER[(LANGUAGE_ORDER.indexOf(prev) + 1) % LANGUAGE_ORDER.length])
            }
            onToggleSimpleMode={() => setSimpleMode((prev) => !prev)}
          />
          <div className="fixed top-4 right-4 z-50">
            <div className="flex flex-col gap-2 items-end">
              <PasskeyLogin
                role={smeRole}
                onAuthenticated={(session) => {
                  if (session?.role) {
                    setSmeRole(session.role.toLowerCase());
                  }
                }}
                onLoggedOut={() => setSmeRole("owner")}
              />
              <button
                type="button"
                onClick={() => setDarkMode((prev) => !prev)}
                className="rounded-xl border border-cyan-400/35 bg-slate-950/75 px-3 py-1.5 text-xs font-semibold text-cyan-100"
              >
                {darkMode ? "Dark On" : "Dark Off"}
              </button>
              <button
                type="button"
                onClick={() => setSmeRole((prev) => (prev === "owner" ? "cashier" : "owner"))}
                className="rounded-xl border border-cyan-400/35 bg-slate-950/75 px-3 py-1.5 text-xs font-semibold text-cyan-100"
              >
                Role: {smeRole}
              </button>
            </div>
          </div>
          <div className="pt-20 flex-1">
            {syncEvent?.event ? (
              <div className="mx-auto max-w-7xl px-4 sm:px-6">
                <div className="mb-3 rounded-xl border border-cyan-400/25 bg-cyan-500/10 px-3 py-2 text-xs text-cyan-100">
                  Live Sync: {syncEvent.event}
                </div>
              </div>
            ) : null}
            {meshStatus ? (
              <div className="mx-auto max-w-7xl px-4 sm:px-6">
                <div className="mb-3 rounded-xl border border-emerald-400/25 bg-emerald-500/10 px-3 py-2 text-xs text-emerald-100">
                  Mesh: {meshStatus}
                </div>
              </div>
            ) : null}
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
              <Route path="/brokers/register" element={<BrokerPortal />} />
              <Route path="/signup" element={<Signup />} />
              <Route path="/ca/dashboard" element={<CaDashboard />} />
              <Route path="/ca/heatmap" element={<CAHeatmap />} />
              <Route path="/ca/accept/:token" element={<CaAccept />} />
              <Route path="/sme-pos" element={<QuickSaleTerminal smeRole={smeRole} />} />
              <Route path="/sme-dashboard" element={<SmeDashboard syncEvent={syncEvent} />} />
              <Route path="/sme-inventory" element={<InventoryManager syncEvent={syncEvent} />} />
              <Route path="/supplier-portal" element={<SupplierPortal />} />
              <Route
                path="/god-mode"
                element={
                  getStoredSmeUsername().trim().toLowerCase() ===
                  (import.meta.env.VITE_SUPER_ADMIN_ID || "krish@accord.local").trim().toLowerCase() ? (
                    <GodModeDashboard />
                  ) : (
                    <Navigate to="/sme-pos" replace />
                  )
                }
              />
              <Route path="/vendor/:vendorLinkId" element={<VendorPortal />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </div>
          <BottomDock language={language} smeRole={smeRole} />
          <SupportChatWidget />
        </div>
      </BrowserRouter>
    </ChatProvider>
  );
}

export default App;
