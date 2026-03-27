import { useEffect, useMemo, useState } from "react";
import apiClient from "./api/client";
import BookingManager from "./components/Bookings/BookingManager";
import ReraAllocator from "./components/Compliance/ReraAllocator";
import InvoiceUploader from "./components/Compliance/InvoiceUploader";
import GstReconciliation from "./components/Compliance/GstReconciliation";
import DynamicReportTable from "./components/Reports/DynamicReportTable";
import PredictiveCashflow from "./components/Reports/PredictiveCashflow";
import { useChatContext } from "./context/ChatContext";

const LANG_ORDER = ["en", "hi", "pa", "ur"];

const LABELS = {
  en: {
    title: "SPV Master Dashboard",
    spv: "SPV Selector",
    language: "Language",
    totalBookings: "Total Bookings",
    awaitingRera: "Funds Awaiting RERA Allocation",
    pendingRent: "Pending Rent Due",
    sampleReport: "Collections Snapshot",
  },
  hi: {
    title: "एसपीवी मास्टर डैशबोर्ड",
    spv: "एसपीवी चयन",
    language: "भाषा",
    totalBookings: "कुल बुकिंग",
    awaitingRera: "RERA आवंटन लंबित राशि",
    pendingRent: "लंबित किराया देय",
    sampleReport: "कलेक्शन स्नैपशॉट",
  },
  pa: {
    title: "ਐਸਪੀਵੀ ਮਾਸਟਰ ਡੈਸ਼ਬੋਰਡ",
    spv: "ਐਸਪੀਵੀ ਚੋਣ",
    language: "ਭਾਸ਼ਾ",
    totalBookings: "ਕੁੱਲ ਬੁਕਿੰਗਾਂ",
    awaitingRera: "RERA ਵੰਡ ਲਈ ਬਕਾਇਆ ਫੰਡ",
    pendingRent: "ਬਕਾਇਆ ਕਿਰਾਇਆ",
    sampleReport: "ਕਲੈਕਸ਼ਨ ਸਨੈਪਸ਼ਾਟ",
  },
  ur: {
    title: "ایس پی وی ماسٹر ڈیش بورڈ",
    spv: "ایس پی وی انتخاب",
    language: "زبان",
    totalBookings: "کل بکنگز",
    awaitingRera: "RERA مختص کیلئے زیر التوا فنڈز",
    pendingRent: "زیر التوا کرایہ",
    sampleReport: "کلیکشن اسنیپ شاٹ",
  },
};

const REPORT_COLUMNS = [
  { key: "booking_id", label: "Booking" },
  { key: "customer_name", label: "Customer" },
  { key: "status", label: "Status" },
  { key: "total_consideration", label: "Consideration", type: "currency" },
];

export default function Dashboard() {
  const [language, setLanguage] = useState("en");
  const [spvs, setSpvs] = useState([]);
  const [activeSpv, setActiveSpv] = useState("");
  const [summary, setSummary] = useState({ total_bookings: 0, funds_awaiting_rera_allocation: "0.00", pending_rent_due: "0.00" });
  const [bookings, setBookings] = useState([]);
  const [error, setError] = useState("");
  const [isProfileOpen, setIsProfileOpen] = useState(false);
  const { toggleChat } = useChatContext();

  const t = LABELS[language] || LABELS.en;

  const loadSummary = async () => {
    try {
      const [spvRes, summaryRes, bookingsRes] = await Promise.all([
        apiClient.get("/organizations/ORG-001/spvs"),
        apiClient.get("/dashboard/summary"),
        apiClient.get("/bookings", { params: { limit: 10 } }),
      ]);
      const spvItems = spvRes.data.items || [];
      setSpvs(spvItems);
      if (!activeSpv && spvItems.length > 0) {
        setActiveSpv(spvItems[0].id);
      }
      setSummary(summaryRes.data || {});
      setBookings(bookingsRes.data.items || []);
      setError("");
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || "Failed to load dashboard data");
    }
  };

  useEffect(() => {
    loadSummary();
    const timer = setInterval(() => {
      loadSummary();
    }, 30000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!isProfileOpen) {
      return undefined;
    }

    const closeMenu = () => setIsProfileOpen(false);
    window.addEventListener("click", closeMenu);
    return () => window.removeEventListener("click", closeMenu);
  }, [isProfileOpen]);

  const cards = useMemo(
    () => [
      { id: "total-bookings", label: t.totalBookings, value: Number(summary.total_bookings || 0).toLocaleString("en-IN") },
      {
        id: "awaiting-rera",
        label: t.awaitingRera,
        value: Number(summary.funds_awaiting_rera_allocation || 0).toLocaleString("en-IN", { style: "currency", currency: "INR" }),
      },
      {
        id: "pending-rent",
        label: t.pendingRent,
        value: Number(summary.pending_rent_due || 0).toLocaleString("en-IN", { style: "currency", currency: "INR" }),
      },
    ],
    [summary, t]
  );

  return (
    <div className="mx-auto w-full max-w-7xl px-4 sm:px-6 py-6 space-y-5">
      <header className="rounded-2xl border border-cyan-400/25 bg-slate-950/75 p-4 sm:p-5 flex flex-col lg:flex-row lg:items-center gap-3 lg:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-cyan-100">{t.title}</h1>
          <p className="text-sm text-slate-400 mt-1">Unified booking, compliance, and reconciliation control center</p>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <label className="text-sm text-slate-300">{t.spv}</label>
          <select value={activeSpv} onChange={(e) => setActiveSpv(e.target.value)} className="rounded-lg bg-slate-900 border border-slate-700 px-3 py-2 text-sm min-w-[180px]">
            {spvs.map((spv) => (
              <option key={spv.id} value={spv.id}>{spv.name}</option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => setLanguage((prev) => LANG_ORDER[(LANG_ORDER.indexOf(prev) + 1) % LANG_ORDER.length])}
            className="rounded-lg bg-cyan-700/70 hover:bg-cyan-600 px-3 py-2 text-sm font-semibold"
          >
            {t.language}: {language.toUpperCase()}
          </button>
          <div className="relative">
            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                setIsProfileOpen((prev) => !prev);
              }}
              className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-slate-100 hover:border-cyan-500"
            >
              Manager Menu
            </button>

            {isProfileOpen ? (
              <div
                className="absolute right-0 z-30 mt-2 w-52 rounded-xl border border-slate-700 bg-slate-900/95 p-2 shadow-xl"
                onClick={(event) => event.stopPropagation()}
              >
                <button
                  type="button"
                  onClick={() => {
                    toggleChat();
                    setIsProfileOpen(false);
                  }}
                  className="w-full rounded-lg px-3 py-2 text-left text-sm text-slate-100 hover:bg-slate-800"
                >
                  AI IT Support
                </button>
              </div>
            ) : null}
          </div>
        </div>
      </header>

      {error ? <div className="rounded-xl border border-red-500/40 bg-red-500/10 p-3 text-red-200 text-sm">{error}</div> : null}

      <section className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {cards.map((card) => (
          <div key={card.label} className="rounded-xl border border-cyan-500/20 bg-slate-950/60 p-4" data-cy={card.id}>
            <div className="text-xs text-slate-400">{card.label}</div>
            <div className="text-xl font-bold text-cyan-100 mt-1" data-cy={`${card.id}-value`}>{card.value}</div>
          </div>
        ))}
      </section>

      <section className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <BookingManager />
        <ReraAllocator />
        <InvoiceUploader />
        <GstReconciliation />
      </section>

      <section className="rounded-2xl border border-cyan-400/20 bg-slate-950/70 p-4 sm:p-5 space-y-3">
        <h2 className="text-lg font-semibold text-cyan-100">{t.sampleReport}</h2>
        <DynamicReportTable columns={REPORT_COLUMNS} data={bookings} />
      </section>

      <PredictiveCashflow />
    </div>
  );
}
