import { useEffect, useMemo, useState } from "react";
import { BarChart3, FileText, Loader2, RefreshCw, ShieldCheck, Users } from "lucide-react";

const TOP_REPORTS_INDIA = [
  { key: "balance_sheet", en: "Balance Sheet", hi: "बैलेंस शीट", pa: "ਬੈਲੇਂਸ ਸ਼ੀਟ", ur: "بیلنس شیٹ", category: "finance" },
  { key: "profit_and_loss", en: "Profit and Loss", hi: "लाभ-हानि विवरण", pa: "ਮੁਨਾਫਾ-ਨੁਕਸਾਨ", ur: "منافع و نقصان", category: "finance" },
  { key: "cash_flow", en: "Cash Flow Statement", hi: "कैश फ्लो स्टेटमेंट", pa: "ਕੈਸ਼ ਫ਼ਲੋ ਸਟੇਟਮੈਂਟ", ur: "کیش فلو اسٹیٹمنٹ", category: "finance" },
  { key: "trial_balance", en: "Trial Balance", hi: "ट्रायल बैलेंस", pa: "ਟ੍ਰਾਇਲ ਬੈਲੇਂਸ", ur: "ٹرائل بیلنس", category: "finance" },
  { key: "bank_reco", en: "Bank Reconciliation", hi: "बैंक मिलान", pa: "ਬੈਂਕ ਮਿਲਾਣ", ur: "بینک ملاپ", category: "finance" },
  { key: "receivables_aging", en: "Receivables Aging", hi: "देय राशि आयु विश्लेषण", pa: "ਰਿਸੀਵੇਬਲਜ਼ ਏਜਿੰਗ", ur: "وصولیوں کی عمر", category: "finance" },
  { key: "payables_aging", en: "Payables Aging", hi: "देनदार आयु विश्लेषण", pa: "ਪੇਏਬਲਜ਼ ਏਜਿੰਗ", ur: "اداگیوں کی عمر", category: "finance" },
  { key: "gst_summary", en: "GST Summary (GSTR-1/3B)", hi: "GST सारांश (GSTR-1/3B)", pa: "GST ਸੰਖੇਪ (GSTR-1/3B)", ur: "جی ایس ٹی خلاصہ (GSTR-1/3B)", category: "compliance" },
  { key: "itc_reco", en: "ITC Reconciliation (2B vs Books)", hi: "आईटीसी मिलान (2B बनाम किताबें)", pa: "ITC ਮਿਲਾਣ (2B ਵਿਰੁੱਧ ਬੁੱਕਸ)", ur: "آئی ٹی سی ملاپ (2B بمقابلہ کتابیں)", category: "compliance" },
  { key: "tds_payable", en: "TDS Payable and Return Status", hi: "टीडीएस देय और रिटर्न स्थिति", pa: "TDS ਦੇਯ ਅਤੇ ਰਿਟਰਨ ਸਥਿਤੀ", ur: "ٹی ڈی ایس واجب الادا اور ریٹرن اسٹیٹس", category: "compliance" },
  { key: "rent_roll", en: "Rent Roll and Occupancy", hi: "रेंट रोल और ऑक्यूपेंसी", pa: "ਕਿਰਾਇਆ ਰੋਲ ਅਤੇ ਆਕਿਊਪੈਂਸੀ", ur: "رینٹ رول اور آکیوپینسی", category: "real_estate" },
  { key: "lease_expiry", en: "Lease Expiry Dashboard", hi: "लीज एक्सपायरी डैशबोर्ड", pa: "ਲੀਜ਼ ਮਿਆਦ ਡੈਸ਼ਬੋਰਡ", ur: "لیز ایکسپائری ڈیش بورڈ", category: "real_estate" },
  { key: "project_profit", en: "Project Profitability by Site", hi: "साइट अनुसार प्रोजेक्ट लाभ", pa: "ਸਾਈਟ ਅਨੁਸਾਰ ਪ੍ਰੋਜੈਕਟ ਮੁਨਾਫ਼ਾ", ur: "سائٹ کے حساب سے پروجیکٹ منافع", category: "real_estate" },
  { key: "wip_construction", en: "Construction WIP", hi: "निर्माण प्रगति (WIP)", pa: "ਕੰਸਟਰਕਸ਼ਨ WIP", ur: "تعمیراتی WIP", category: "real_estate" },
  { key: "rera_milestone", en: "RERA Milestone Compliance", hi: "RERA माइलस्टोन अनुपालन", pa: "RERA ਮਾਈਲਸਟੋਨ ਕੰਪਲਾਇੰਸ", ur: "ریرا مائل اسٹون کمپلائنس", category: "real_estate" },
  { key: "broker_commission", en: "Broker Commission Payable", hi: "ब्रोकर कमीशन देय", pa: "ਬ੍ਰੋਕਰ ਕਮਿਸ਼ਨ ਦੇਯ", ur: "بروکر کمیشن واجب الادا", category: "real_estate" },
  { key: "inventory_material", en: "Material Consumption and Inventory", hi: "सामग्री खपत और इन्वेंटरी", pa: "ਮੈਟੀਰੀਅਲ ਖਪਤ ਅਤੇ ਇਨਵੈਂਟਰੀ", ur: "میٹیریل کھپت اور انوینٹری", category: "real_estate" },
  { key: "budget_vs_actual", en: "Budget vs Actual by Project", hi: "प्रोजेक्ट बजट बनाम वास्तविक", pa: "ਪ੍ਰੋਜੈਕਟ ਬਜਟ ਵਿਰੁੱਧ ਅਸਲ", ur: "پروجیکٹ بجٹ بمقابلہ اصل", category: "real_estate" },
  { key: "loan_emi", en: "Loan EMI and Interest Tracker", hi: "लोन EMI और ब्याज ट्रैकर", pa: "ਲੋਨ EMI ਅਤੇ ਵਿਆਜ ਟ੍ਰੈਕਰ", ur: "لون EMI اور سود ٹریکر", category: "finance" },
  { key: "sales_booking", en: "Sales Booking and Collection", hi: "सेल्स बुकिंग और कलेक्शन", pa: "ਸੇਲਜ਼ ਬੁਕਿੰਗ ਅਤੇ ਕਲੈਕਸ਼ਨ", ur: "سیلز بکنگ اور کلیکشن", category: "real_estate" },
];

const CATEGORY_LABEL = {
  en: { finance: "Finance", compliance: "Compliance", real_estate: "Real Estate" },
  hi: { finance: "वित्त", compliance: "अनुपालन", real_estate: "रियल एस्टेट" },
  pa: { finance: "ਵਿੱਤ", compliance: "ਕੰਪਲਾਇੰਸ", real_estate: "ਰੀਅਲ ਐਸਟੇਟ" },
  ur: { finance: "مالیات", compliance: "تعمیل", real_estate: "ریئل اسٹیٹ" },
};

const UI_TEXT = {
  en: {
    center_title: "Accord Report Center",
    center_subtitle: "Top India Reports for Real-Estate Managers",
    refresh: "Refresh Reports",
    top20: "Top 20 Reports (India Demand)",
    top20_sub: "Market-priority report pack for North-India real-estate operators.",
  },
  hi: {
    center_title: "अकॉर्ड रिपोर्ट सेंटर",
    center_subtitle: "रियल-एस्टेट मैनेजर के लिए भारत के शीर्ष रिपोर्ट",
    refresh: "रिपोर्ट रीफ्रेश करें",
    top20: "शीर्ष 20 रिपोर्ट (भारत में मांग)",
    top20_sub: "नॉर्थ इंडिया रियल-एस्टेट संचालन के लिए प्राथमिक रिपोर्ट पैक।",
  },
  pa: {
    center_title: "ਅਕੋਰਡ ਰਿਪੋਰਟ ਸੈਂਟਰ",
    center_subtitle: "ਰੀਅਲ ਐਸਟੇਟ ਮੈਨੇਜਰ ਲਈ ਭਾਰਤ ਦੀਆਂ ਟਾਪ ਰਿਪੋਰਟਾਂ",
    refresh: "ਰਿਪੋਰਟਾਂ ਰਿਫ੍ਰੈਸ਼ ਕਰੋ",
    top20: "ਟਾਪ 20 ਰਿਪੋਰਟਾਂ (ਭਾਰਤੀ ਮੰਗ)",
    top20_sub: "ਉੱਤਰੀ ਭਾਰਤ ਰੀਅਲ ਐਸਟੇਟ ਓਪਰੇਟਰਾਂ ਲਈ ਪ੍ਰਾਇਰਟੀ ਰਿਪੋਰਟ ਪੈਕ।",
  },
  ur: {
    center_title: "اکورڈ رپورٹ سینٹر",
    center_subtitle: "ریئل اسٹیٹ منیجرز کے لیے بھارت کی ٹاپ رپورٹس",
    refresh: "رپورٹس ریفریش کریں",
    top20: "ٹاپ 20 رپورٹس (بھارتی طلب)",
    top20_sub: "شمالی ہند ریئل اسٹیٹ آپریٹرز کے لیے ترجیحی رپورٹ پیک۔",
  },
};

function formatDate(value) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString("en-IN", { dateStyle: "medium", timeStyle: "short" });
}

export default function ReportsCenter({ language = "en", simpleMode = false }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [exportHistory, setExportHistory] = useState([]);
  const [marketingRows, setMarketingRows] = useState([]);
  const [forensic, setForensic] = useState(null);
  const [reversalSummary, setReversalSummary] = useState(null);

  const refreshReports = async () => {
    setLoading(true);
    setError("");
    try {
      const [historyRes, marketingRes, forensicRes, reversalRes] = await Promise.all([
        fetch("/api/v1/reports/export-history?limit=40"),
        fetch("/api/v1/reports/marketing-signups?limit=150"),
        fetch("/api/v1/insights/forensic-audit?limit=150", {
          method: "POST",
          headers: {
            "X-Role": "admin",
            "X-Admin-Id": "101",
          },
        }),
        fetch("/api/v1/journal/reversal-summary/recent?hours=72&include_filed=false"),
      ]);

      const [historyData, marketingData, forensicData, reversalData] = await Promise.all([
        historyRes.json(),
        marketingRes.json(),
        forensicRes.json(),
        reversalRes.json(),
      ]);

      if (!historyRes.ok) {
        throw new Error(historyData?.detail || `Export history failed (${historyRes.status})`);
      }
      if (!marketingRes.ok) {
        throw new Error(marketingData?.detail || `Marketing report failed (${marketingRes.status})`);
      }
      if (!forensicRes.ok) {
        throw new Error(forensicData?.detail || `Forensic report failed (${forensicRes.status})`);
      }
      if (!reversalRes.ok) {
        throw new Error(reversalData?.detail || `Reversal report failed (${reversalRes.status})`);
      }

      setExportHistory(Array.isArray(historyData) ? historyData : []);
      setMarketingRows(Array.isArray(marketingData?.rows) ? marketingData.rows : []);
      setForensic(forensicData);
      setReversalSummary(reversalData);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load reports");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refreshReports();
  }, []);

  const totals = useMemo(() => {
    return {
      exports: exportHistory.length,
      leads: marketingRows.length,
      atRisk: Number(reversalSummary?.summary?.at_risk_invoice_count || 0),
      riskScore: Number(forensic?.risk_score || 0),
    };
  }, [exportHistory, marketingRows, reversalSummary, forensic]);

  const text = UI_TEXT[language] || UI_TEXT.en;

  const top20Localized = useMemo(() => {
    return TOP_REPORTS_INDIA.map((row, idx) => ({
      ...row,
      title: row[language] || row.en,
      priority: idx + 1,
      categoryLabel: CATEGORY_LABEL[language]?.[row.category] || CATEGORY_LABEL.en[row.category],
    }));
  }, [language]);

  return (
    <main className={`mx-auto max-w-7xl px-4 sm:px-6 py-12 space-y-6 ${simpleMode ? "text-base" : ""}`}>
      <section className="rounded-3xl border border-cyan-500/25 bg-slate-950/70 p-6 sm:p-8">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <p className="text-xs uppercase tracking-[0.24em] text-cyan-200">{text.center_title}</p>
            <h1 className="mt-2 text-3xl font-black tracking-tight text-slate-100">{text.center_subtitle}</h1>
          </div>
          <button
            onClick={() => {
              void refreshReports();
            }}
            disabled={loading}
            className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900/80 px-3 py-2 text-sm font-semibold text-slate-100 hover:bg-slate-800 disabled:opacity-60"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
            {text.refresh}
          </button>
        </div>
        {error ? <p className="mt-4 text-sm text-red-300">{error}</p> : null}
      </section>

      <section className="rounded-2xl border border-indigo-500/25 bg-indigo-950/20 p-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <h2 className="font-semibold text-indigo-100">{text.top20}</h2>
            <p className="text-xs text-indigo-200/80 mt-1">{text.top20_sub}</p>
          </div>
          <span className="text-xs text-indigo-200 rounded-md border border-indigo-400/40 px-2 py-1">20</span>
        </div>
        <div className="mt-4 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
          {top20Localized.map((report) => (
            <article key={report.key} className="rounded-lg border border-slate-700/80 bg-slate-900/70 p-3">
              <div className="flex items-center justify-between gap-2">
                <p className="text-xs font-black text-cyan-200">#{report.priority}</p>
                <p className="text-[11px] text-slate-400">{report.categoryLabel}</p>
              </div>
              <p className="mt-1 text-sm font-semibold text-slate-100">{report.title}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <article className="rounded-2xl border border-cyan-500/30 bg-cyan-900/20 p-4">
          <p className="text-xs text-cyan-200">Total Exports</p>
          <p className="mt-1 text-2xl font-black text-cyan-100">{totals.exports}</p>
        </article>
        <article className="rounded-2xl border border-emerald-500/30 bg-emerald-900/20 p-4">
          <p className="text-xs text-emerald-200">Marketing Leads</p>
          <p className="mt-1 text-2xl font-black text-emerald-100">{totals.leads}</p>
        </article>
        <article className="rounded-2xl border border-amber-500/30 bg-amber-900/20 p-4">
          <p className="text-xs text-amber-200">Invoices At Risk</p>
          <p className="mt-1 text-2xl font-black text-amber-100">{totals.atRisk}</p>
        </article>
        <article className="rounded-2xl border border-violet-500/30 bg-violet-900/20 p-4">
          <p className="text-xs text-violet-200">Forensic Risk Score</p>
          <p className="mt-1 text-2xl font-black text-violet-100">{Math.round(totals.riskScore)}</p>
        </article>
      </section>

      <section className="grid gap-4 xl:grid-cols-2">
        <article className="rounded-2xl border border-slate-700 bg-slate-950/70 p-4">
          <div className="flex items-center gap-2">
            <FileText className="w-4 h-4 text-cyan-300" />
            <h2 className="font-semibold text-slate-100">Filing Export History</h2>
          </div>
          <div className="mt-3 space-y-2 max-h-80 overflow-y-auto">
            {exportHistory.slice(0, 20).map((row) => (
              <div key={row.id} className="rounded-lg border border-slate-800 bg-slate-900/70 p-3 text-xs">
                <div className="flex items-center justify-between gap-3">
                  <span className="font-semibold text-cyan-200">{row.report_type}</span>
                  <span className="text-slate-400">{row.status}</span>
                </div>
                <p className="mt-1 text-slate-300">{row.period_from} to {row.period_to}</p>
                <p className="text-slate-500">Generated: {formatDate(row.generated_at)}</p>
              </div>
            ))}
            {!loading && exportHistory.length === 0 ? <p className="text-xs text-slate-400">No export records found.</p> : null}
          </div>
        </article>

        <article className="rounded-2xl border border-slate-700 bg-slate-950/70 p-4">
          <div className="flex items-center gap-2">
            <Users className="w-4 h-4 text-emerald-300" />
            <h2 className="font-semibold text-slate-100">Marketing Signup Leads</h2>
          </div>
          <div className="mt-3 space-y-2 max-h-80 overflow-y-auto">
            {marketingRows.slice(0, 20).map((row) => (
              <div key={row.id} className="rounded-lg border border-slate-800 bg-slate-900/70 p-3 text-xs">
                <div className="flex items-center justify-between gap-2">
                  <span className="font-semibold text-emerald-200">{row.name}</span>
                  <span className="text-slate-400">{row.provider}</span>
                </div>
                <p className="mt-1 text-slate-300">{row.email}</p>
                <p className="text-slate-500">Updated: {formatDate(row.updated_at)}</p>
              </div>
            ))}
            {!loading && marketingRows.length === 0 ? <p className="text-xs text-slate-400">No marketing leads captured yet.</p> : null}
          </div>
        </article>
      </section>

      <section className="grid gap-4 xl:grid-cols-2">
        <article className="rounded-2xl border border-slate-700 bg-slate-950/70 p-4">
          <div className="flex items-center gap-2">
            <ShieldCheck className="w-4 h-4 text-amber-300" />
            <h2 className="font-semibold text-slate-100">Reversal Snapshot</h2>
          </div>
          <div className="mt-3 text-sm text-slate-300 space-y-1">
            <p>At-risk invoices: {reversalSummary?.summary?.at_risk_invoice_count ?? 0}</p>
            <p>Immediate reversal risk: {reversalSummary?.summary?.immediate_reversal_risk ?? "0.00"}</p>
            <p>Generated at: {formatDate(reversalSummary?.generated_at)}</p>
          </div>
        </article>

        <article className="rounded-2xl border border-slate-700 bg-slate-950/70 p-4">
          <div className="flex items-center gap-2">
            <BarChart3 className="w-4 h-4 text-violet-300" />
            <h2 className="font-semibold text-slate-100">Forensic Summary</h2>
          </div>
          <div className="mt-3 text-sm text-slate-300 space-y-1">
            <p>Risk score: {Math.round(Number(forensic?.risk_score || 0))}</p>
            <p>Flagged entries: {Array.isArray(forensic?.flagged_entries) ? forensic.flagged_entries.length : 0}</p>
            <p>Model: {forensic?.model || "-"}</p>
          </div>
        </article>
      </section>
    </main>
  );
}
