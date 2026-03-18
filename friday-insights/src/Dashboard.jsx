import { useState } from "react";
import {
  LayoutDashboard,
  ShieldCheck,
  CalendarDays,
  Settings,
  Search,
  Bell,
  ChevronDown,
  TrendingUp,
  TrendingDown,
  Users,
  Wallet,
  ReceiptText,
  AlertCircle,
  CheckCircle2,
  Clock,
  BarChart3,
  Menu,
  X,
  LogOut,
  BadgeCheck,
  Building2,
  Package,
} from "lucide-react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Area,
  AreaChart,
} from "recharts";

// ─── Mock Data ────────────────────────────────────────────────────────────────

const WEEKLY_REVENUE = [
  { day: "Mon", revenue: 112000, expenses: 43000 },
  { day: "Tue", revenue: 98500,  expenses: 31000 },
  { day: "Wed", revenue: 143200, expenses: 52000 },
  { day: "Thu", revenue: 127800, expenses: 47500 },
  { day: "Fri", revenue: 168400, expenses: 61000 },
  { day: "Sat", revenue: 89600,  expenses: 28000 },
  { day: "Sun", revenue: 54300,  expenses: 19500 },
];

const RECENT_TRANSACTIONS = [
  { id: "TXN-9841", date: "16 Mar 2026", client: "Mehta Textiles Ltd.",    amount: 182500, status: "Paid",    type: "credit" },
  { id: "TXN-9840", date: "15 Mar 2026", client: "Sharma Enterprises",     amount: 67300,  status: "Pending", type: "credit" },
  { id: "TXN-9839", date: "15 Mar 2026", client: "Patel Auto Components",  amount: 234000, status: "Paid",    type: "credit" },
  { id: "TXN-9838", date: "14 Mar 2026", client: "Gupta Pharma Wholesale", amount: 98750,  status: "Pending", type: "credit" },
  { id: "TXN-9837", date: "13 Mar 2026", client: "Singh & Sons Traders",   amount: 315200, status: "Paid",    type: "credit" },
];

const CA_CLIENTS = [
  { id: "sharma",  name: "Sharma Traders",       gst: "29AAGCS1234K1Z5", lastSync: "10 mins ago"  },
  { id: "global",  name: "Global Auto Parts",     gst: "27AABCG5678P1ZQ", lastSync: "2 hrs ago"    },
  { id: "apex",    name: "Apex Tech Solutions",   gst: "07AADCA8765M1Z2", lastSync: "Yesterday"    },
  { id: "mehta",   name: "Mehta Textiles Ltd.",   gst: "24AADFM4321N1ZX", lastSync: "3 days ago"   },
  { id: "patel",   name: "Patel Auto Components", gst: "29AAECP9876K1ZM", lastSync: "1 week ago"   },
];

const INVENTORY_DATA = {
  fastestMoving: { name: "Honda Shine Engine Oil 1L", turnover: "4.2× / month", value: "₹48,200" },
  deadStock: [
    { name: "Hero Splendor Brake Cable",   days: 94, value: "₹12,400", sku: "SKU-4421" },
    { name: "Castrol 20W50 Engine Oil 5L", days: 78, value: "₹31,750", sku: "SKU-1834" },
    { name: "Bajaj Pulsar Clutch Plate",   days: 67, value: "₹8,900",  sku: "SKU-2290" },
  ],
  totalTiedCapital: "₹53,050",
};

const KPI_CARDS = [
  {
    id: "revenue",
    label: "Total Revenue (MTD)",
    value: "₹14,82,500",
    change: "+12.4%",
    up: true,
    icon: <Wallet className="w-5 h-5" />,
    accent: "blue",
    sub: "vs ₹13,19,200 last month",
  },
  {
    id: "receivables",
    label: "Pending Receivables",
    value: "₹3,12,000",
    change: "-8.1%",
    up: false,
    icon: <ReceiptText className="w-5 h-5" />,
    accent: "amber",
    sub: "4 invoices outstanding",
  },
  {
    id: "gst",
    label: "GST Liability (Mar)",
    value: "₹1,44,300",
    change: "+5.2%",
    up: false,
    icon: <ShieldCheck className="w-5 h-5" />,
    accent: "red",
    sub: "Due 20 Mar 2026",
  },
  {
    id: "clients",
    label: "Active Clients",
    value: "47",
    change: "+3",
    up: true,
    icon: <Users className="w-5 h-5" />,
    accent: "green",
    sub: "3 new this month",
  },
];

const NAV_ITEMS = [
  { id: "home",    label: "Dashboard",      icon: <LayoutDashboard className="w-[18px] h-[18px]" />, href: "#" },
  { id: "gst",    label: "GST Overview",   icon: <ShieldCheck className="w-[18px] h-[18px]" />,      href: "#" },
  { id: "reports",label: "Weekly Reports", icon: <CalendarDays className="w-[18px] h-[18px]" />,     href: "#" },
  { id: "settings",label: "Settings",      icon: <Settings className="w-[18px] h-[18px]" />,         href: "#" },
];

// ─── Helpers ──────────────────────────────────────────────────────────────────

const ACCENT_STYLES = {
  blue:  { glow: "rgba(59,130,246,0.12)",  border: "rgba(59,130,246,0.28)",  icon: "bg-blue-500/15 text-blue-400",   badge: "text-blue-400"   },
  amber: { glow: "rgba(245,158,11,0.10)",  border: "rgba(245,158,11,0.28)",  icon: "bg-amber-500/15 text-amber-400", badge: "text-amber-400"  },
  red:   { glow: "rgba(239,68,68,0.10)",   border: "rgba(239,68,68,0.28)",   icon: "bg-red-500/15 text-red-400",     badge: "text-red-400"    },
  green: { glow: "rgba(52,211,153,0.10)",  border: "rgba(52,211,153,0.28)",  icon: "bg-emerald-500/15 text-emerald-400", badge: "text-emerald-400" },
};

const fmt = (n) => "₹" + Number(n).toLocaleString("en-IN");

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div
      className="rounded-xl px-4 py-3 text-xs border border-white/10 shadow-2xl"
      style={{ background: "rgba(2,8,23,0.92)", backdropFilter: "blur(16px)" }}
    >
      <p className="text-slate-400 font-medium mb-2">{label}</p>
      {payload.map((p) => (
        <p key={p.dataKey} className="font-semibold" style={{ color: p.color }}>
          {p.dataKey === "revenue" ? "Revenue" : "Expenses"}: {fmt(p.value)}
        </p>
      ))}
    </div>
  );
};

// ─── Component ────────────────────────────────────────────────────────────────

export default function Dashboard() {
  const [activeNav, setActiveNav]           = useState("home");
  const [sidebarOpen, setSidebarOpen]       = useState(false);
  const [profileOpen, setProfileOpen]       = useState(false);
  const [notifOpen, setNotifOpen]           = useState(false);
  const [activeClient, setActiveClient]     = useState(CA_CLIENTS[0]);
  const [clientDropdown, setClientDropdown] = useState(false);

  return (
    <div
      className="min-h-screen text-white flex overflow-hidden"
      style={{ background: "#020817" }}
    >
      {/* ── Ambient orbs ──────────────────────────────────────────────────── */}
      <div className="fixed inset-0 pointer-events-none overflow-hidden z-0" aria-hidden>
        <div className="absolute -top-40 -left-40 w-[600px] h-[600px] rounded-full opacity-15"
          style={{ background: "radial-gradient(circle, #1d4ed8 0%, transparent 65%)" }} />
        <div className="absolute bottom-0 right-0 w-[500px] h-[500px] rounded-full opacity-8"
          style={{ background: "radial-gradient(circle, #25D366 0%, transparent 65%)" }} />
      </div>

      {/* ── Sidebar ───────────────────────────────────────────────────────── */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 z-30 bg-black/60 md:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      <aside
        className={`
          fixed top-0 left-0 h-full z-40 flex flex-col
          border-r border-white/[0.06] transition-transform duration-300
          w-[240px]
          ${sidebarOpen ? "translate-x-0" : "-translate-x-full"}
          md:translate-x-0 md:static md:flex
        `}
        style={{
          background: "rgba(4,13,36,0.92)",
          backdropFilter: "blur(24px)",
          minHeight: "100dvh",
        }}
      >
        <div className="flex items-center gap-2.5 px-5 h-16 border-b border-white/[0.06] flex-shrink-0">
          <div className="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center">
            <BarChart3 className="w-4 h-4 text-white" />
          </div>
          <span className="font-bold text-base tracking-tight select-none">
            Friday <span className="text-blue-400">Insights</span>
          </span>
        </div>

        <nav className="flex-1 px-3 py-5 space-y-1 overflow-y-auto">
          <p className="text-slate-600 text-[10px] font-bold uppercase tracking-[0.18em] px-3 mb-3">
            Main Menu
          </p>
          {NAV_ITEMS.map((item) => {
            const active = activeNav === item.id;
            return (
              <button
                key={item.id}
                onClick={() => { setActiveNav(item.id); setSidebarOpen(false); }}
                className={`
                  w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium
                  transition-all duration-200 text-left
                  ${active
                    ? "bg-blue-600/20 text-blue-300 border border-blue-500/25"
                    : "text-slate-400 hover:text-white hover:bg-white/[0.06]"
                  }
                `}
              >
                <span className={active ? "text-blue-400" : ""}>{item.icon}</span>
                {item.label}
                {active && (
                  <span className="ml-auto w-1.5 h-1.5 rounded-full bg-blue-400 flex-shrink-0" />
                )}
              </button>
            );
          })}
        </nav>

        <div className="px-3 py-4 border-t border-white/[0.06]">
          <div className="flex items-center gap-3 px-3 py-2.5 rounded-xl bg-white/[0.04] border border-white/[0.06]">
            <div className="w-8 h-8 rounded-full bg-gradient-to-br from-blue-500 to-violet-600 flex items-center justify-center text-xs font-bold flex-shrink-0">
              KC
            </div>
            <div className="min-w-0">
              <p className="text-xs font-semibold text-white truncate">Krish & Co.</p>
              <p className="text-[10px] text-slate-500 truncate">Admin</p>
            </div>
            <button className="ml-auto text-slate-500 hover:text-red-400 transition-colors">
              <LogOut className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>
      </aside>

      {/* ── Main panel ────────────────────────────────────────────────────── */}
      <div className="flex-1 flex flex-col min-w-0 relative z-10">

        {/* ── Top Header ──────────────────────────────────────────────────── */}
        <header
          className="sticky top-0 z-20 flex items-center gap-3 px-4 sm:px-6 h-16 border-b border-white/[0.06] flex-shrink-0"
          style={{ background: "rgba(2,8,23,0.85)", backdropFilter: "blur(20px)" }}
        >
          <button
            className="md:hidden p-1.5 text-slate-400 hover:text-white rounded-lg transition-colors"
            onClick={() => setSidebarOpen(true)}
          >
            <Menu className="w-5 h-5" />
          </button>

          <div className="relative flex-1 max-w-xs">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500 pointer-events-none" />
            <input
              type="text"
              placeholder="Search transactions, invoices…"
              className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg pl-9 pr-4 py-2 text-xs text-slate-300 placeholder-slate-600 focus:outline-none focus:border-blue-500/50 focus:bg-blue-500/5 transition-all duration-200"
            />
          </div>

          {/* ── CA Multi-Client Switcher ─────────────────────────────────── */}
          <div className="hidden md:block relative">
            <button
              onClick={() => { setClientDropdown(!clientDropdown); setProfileOpen(false); setNotifOpen(false); }}
              className="flex items-center gap-2 bg-white/[0.04] border border-white/[0.08] hover:border-blue-500/30 rounded-xl px-3 py-1.5 transition-all duration-200"
            >
              <div className="w-5 h-5 rounded-md bg-blue-500/20 flex items-center justify-center flex-shrink-0">
                <Building2 className="w-3 h-3 text-blue-400" />
              </div>
              <div className="text-left">
                <p className="text-[9px] text-slate-500 leading-none mb-0.5">Active Client</p>
                <p className="text-xs font-semibold text-white leading-none truncate max-w-[120px]">{activeClient.name}</p>
              </div>
              <div className="ml-1 hidden lg:block">
                <p className="text-[9px] text-slate-600 leading-none">Synced {activeClient.lastSync}</p>
              </div>
              <ChevronDown className="w-3 h-3 text-slate-500 ml-1 flex-shrink-0" />
            </button>
            {clientDropdown && (
              <div
                className="absolute left-0 top-full mt-2 w-64 rounded-2xl border border-white/10 shadow-2xl overflow-hidden z-50"
                style={{ background: "rgba(4,13,36,0.97)", backdropFilter: "blur(24px)" }}
              >
                <div className="px-4 py-2.5 border-b border-white/[0.06]">
                  <p className="text-[10px] font-bold uppercase tracking-[0.15em] text-slate-500">Switch Client</p>
                </div>
                {CA_CLIENTS.map((client) => (
                  <button
                    key={client.id}
                    onClick={() => { setActiveClient(client); setClientDropdown(false); }}
                    className={`w-full flex items-center gap-3 px-4 py-3 text-left transition-colors hover:bg-white/[0.05] ${
                      activeClient.id === client.id ? "bg-blue-600/10" : ""
                    }`}
                  >
                    <div className={`w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0 ${
                      activeClient.id === client.id ? "bg-blue-500/20" : "bg-white/[0.05]"
                    }`}>
                      <Building2 className={`w-3.5 h-3.5 ${activeClient.id === client.id ? "text-blue-400" : "text-slate-500"}`} />
                    </div>
                    <div className="min-w-0 flex-1">
                      <p className={`text-xs font-semibold truncate ${activeClient.id === client.id ? "text-white" : "text-slate-300"}`}>
                        {client.name}
                      </p>
                      <p className="text-[9px] text-slate-600 mt-0.5">Last sync: {client.lastSync}</p>
                    </div>
                    {activeClient.id === client.id && (
                      <span className="w-1.5 h-1.5 rounded-full bg-blue-400 flex-shrink-0" />
                    )}
                  </button>
                ))}
                <div className="px-4 py-2.5 border-t border-white/[0.06]">
                  <button className="w-full text-center text-[10px] text-blue-400 hover:text-blue-300 transition-colors font-medium">
                    + Add New Client
                  </button>
                </div>
              </div>
            )}
          </div>

          <div className="ml-auto flex items-center gap-2">
            <div className="relative">
              <button
                onClick={() => { setNotifOpen(!notifOpen); setProfileOpen(false); }}
                className="relative p-2 rounded-lg text-slate-400 hover:text-white hover:bg-white/[0.06] transition-all duration-200"
              >
                <Bell className="w-[18px] h-[18px]" />
                <span className="absolute top-1.5 right-1.5 w-1.5 h-1.5 rounded-full bg-red-500" />
              </button>
              {notifOpen && (
                <div
                  className="absolute right-0 top-full mt-2 w-72 rounded-2xl border border-white/10 shadow-2xl overflow-hidden"
                  style={{ background: "rgba(4,13,36,0.97)", backdropFilter: "blur(24px)" }}
                >
                  <div className="px-4 py-3 border-b border-white/[0.06] flex items-center justify-between">
                    <p className="text-xs font-semibold text-white">Notifications</p>
                    <span className="text-[10px] bg-red-500/20 text-red-400 px-2 py-0.5 rounded-full font-semibold">3 new</span>
                  </div>
                  {[
                    { msg: "GSTR-1 filing overdue",     time: "2h ago",  color: "text-red-400",   icon: <AlertCircle className="w-3.5 h-3.5" /> },
                    { msg: "Mehta Textiles payment received", time: "5h ago",  color: "text-green-400", icon: <CheckCircle2 className="w-3.5 h-3.5" /> },
                    { msg: "Weekly report generated",   time: "9h ago",  color: "text-blue-400",  icon: <BarChart3 className="w-3.5 h-3.5" /> },
                  ].map((n, i) => (
                    <div key={i} className="px-4 py-3 border-b border-white/[0.04] hover:bg-white/[0.03] transition-colors cursor-pointer flex items-start gap-3">
                      <span className={`mt-0.5 flex-shrink-0 ${n.color}`}>{n.icon}</span>
                      <div className="min-w-0">
                        <p className="text-xs text-slate-200 leading-snug">{n.msg}</p>
                        <p className="text-[10px] text-slate-500 mt-0.5">{n.time}</p>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className="relative">
              <button
                onClick={() => { setProfileOpen(!profileOpen); setNotifOpen(false); }}
                className="flex items-center gap-2.5 bg-white/[0.04] border border-white/[0.08] hover:border-blue-500/30 rounded-xl px-3 py-1.5 transition-all duration-200"
              >
                <div className="w-6 h-6 rounded-full bg-gradient-to-br from-blue-500 to-violet-600 flex items-center justify-center text-[10px] font-bold flex-shrink-0">
                  KC
                </div>
                <span className="hidden sm:block text-xs font-medium text-slate-300">Krish & Co.</span>
                <ChevronDown className="w-3 h-3 text-slate-500" />
              </button>
              {profileOpen && (
                <div
                  className="absolute right-0 top-full mt-2 w-48 rounded-2xl border border-white/10 shadow-2xl overflow-hidden py-1"
                  style={{ background: "rgba(4,13,36,0.97)", backdropFilter: "blur(24px)" }}
                >
                  {["Profile", "Account", "Billing", "Sign out"].map((label) => (
                    <button
                      key={label}
                      className={`w-full text-left px-4 py-2.5 text-xs transition-colors hover:bg-white/[0.05] ${label === "Sign out" ? "text-red-400 hover:text-red-300 border-t border-white/[0.06] mt-1 pt-3" : "text-slate-300 hover:text-white"}`}
                    >
                      {label}
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>
        </header>

        {/* ── Scrollable content ──────────────────────────────────────────── */}
        <main className="flex-1 overflow-y-auto px-4 sm:px-6 py-7 space-y-6">

          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
            <div>
              <h1 className="text-xl font-extrabold tracking-tight">Dashboard</h1>
              <p className="text-slate-400 text-xs mt-0.5">Monday, 16 March 2026 · Last synced 2 min ago</p>
            </div>
            <div className="flex items-center gap-2">
              <span className="inline-flex items-center gap-1.5 bg-emerald-500/10 border border-emerald-500/25 text-emerald-400 text-xs font-semibold px-3 py-1.5 rounded-full">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                Tally Connected
              </span>
            </div>
          </div>

          {/* ── Row 1: KPI Cards ──────────────────────────────────────────── */}
          <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
            {KPI_CARDS.map((card) => {
              const a = ACCENT_STYLES[card.accent];
              return (
                <div
                  key={card.id}
                  className="group relative rounded-2xl p-5 border transition-all duration-300 hover:-translate-y-1 cursor-default"
                  style={{
                    background: "linear-gradient(145deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.015) 100%)",
                    borderColor: a.border,
                    boxShadow: `0 0 0 0 transparent`,
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.boxShadow = `0 8px 40px ${a.glow}`; }}
                  onMouseLeave={(e) => { e.currentTarget.style.boxShadow = `0 0 0 0 transparent`; }}
                >
                  <div className="flex items-start justify-between mb-4">
                    <div className={`w-10 h-10 rounded-xl flex items-center justify-center flex-shrink-0 ${a.icon}`}>
                      {card.icon}
                    </div>
                    <span
                      className={`inline-flex items-center gap-1 text-xs font-semibold px-2.5 py-1 rounded-full ${
                        card.up
                          ? "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20"
                          : "bg-red-500/10 text-red-400 border border-red-500/20"
                      }`}
                    >
                      {card.up
                        ? <TrendingUp className="w-3 h-3" />
                        : <TrendingDown className="w-3 h-3" />
                      }
                      {card.change}
                    </span>
                  </div>
                  <p className="text-2xl font-extrabold tracking-tight">{card.value}</p>
                  <p className="text-slate-400 text-xs mt-1 font-medium">{card.label}</p>
                  <p className="text-slate-600 text-[10px] mt-1.5">{card.sub}</p>
                </div>
              );
            })}
          </div>

          {/* ── Row 2: Chart ──────────────────────────────────────────────── */}
          <div
            className="rounded-2xl border border-white/[0.08] p-5 sm:p-7"
            style={{
              background: "linear-gradient(145deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0.01) 100%)",
            }}
          >
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4 mb-8">
              <div>
                <h2 className="text-base font-bold text-white">Weekly Revenue Overview</h2>
                <p className="text-slate-500 text-xs mt-0.5">10 – 16 March 2026 · All figures in INR</p>
              </div>

              <div
                className="inline-flex items-center gap-2 px-4 py-2.5 rounded-xl border text-xs font-bold self-start sm:self-auto flex-shrink-0"
                style={{
                  background: "rgba(37,211,102,0.08)",
                  border: "1px solid rgba(37,211,102,0.3)",
                  boxShadow: "0 0 24px rgba(37,211,102,0.14), inset 0 1px 0 rgba(37,211,102,0.08)",
                  color: "#25D366",
                }}
              >
                <BadgeCheck className="w-4 h-4 flex-shrink-0" />
                Verified by Dual-Algorithm Validation
              </div>
            </div>

            <div className="flex items-center gap-5 mb-6">
              {[{ label: "Revenue", color: "#60a5fa" }, { label: "Expenses", color: "rgba(139,92,246,0.7)" }].map((l) => (
                <span key={l.label} className="flex items-center gap-2 text-xs text-slate-400">
                  <span className="w-3 h-0.5 rounded-full inline-block" style={{ background: l.color }} />
                  {l.label}
                </span>
              ))}
            </div>

            <div className="h-60 sm:h-72">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={WEEKLY_REVENUE} margin={{ top: 5, right: 4, left: 0, bottom: 0 }}>
                  <defs>
                    <linearGradient id="gradRevenue" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%"  stopColor="#3b82f6" stopOpacity={0.25} />
                      <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="gradExpenses" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%"  stopColor="#8b5cf6" stopOpacity={0.2} />
                      <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
                  <XAxis
                    dataKey="day"
                    tick={{ fill: "#64748b", fontSize: 11 }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis
                    tickFormatter={(v) => `₹${(v / 1000).toFixed(0)}k`}
                    tick={{ fill: "#64748b", fontSize: 10 }}
                    axisLine={false}
                    tickLine={false}
                    width={52}
                  />
                  <Tooltip content={<CustomTooltip />} cursor={{ stroke: "rgba(255,255,255,0.06)", strokeWidth: 1 }} />
                  <Area
                    type="monotone"
                    dataKey="revenue"
                    stroke="#60a5fa"
                    strokeWidth={2.5}
                    fill="url(#gradRevenue)"
                    dot={false}
                    activeDot={{ r: 5, fill: "#60a5fa", stroke: "#020817", strokeWidth: 2 }}
                  />
                  <Area
                    type="monotone"
                    dataKey="expenses"
                    stroke="rgba(139,92,246,0.75)"
                    strokeWidth={2}
                    fill="url(#gradExpenses)"
                    dot={false}
                    activeDot={{ r: 4, fill: "#8b5cf6", stroke: "#020817", strokeWidth: 2 }}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* ── Row 3: Transactions + Inventory ────────────────────────────── */}
          <div className="grid grid-cols-1 lg:grid-cols-5 gap-5">

            {/* Recent Transactions */}
            <div
              className="lg:col-span-3 rounded-2xl border border-white/[0.08] overflow-hidden"
              style={{ background: "linear-gradient(145deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0.01) 100%)" }}
            >
              <div className="flex items-center justify-between px-5 py-4 border-b border-white/[0.06]">
                <h3 className="text-sm font-bold text-white">Recent Transactions</h3>
                <button className="text-xs text-blue-400 hover:text-blue-300 transition-colors font-medium">
                  View all →
                </button>
              </div>

              <div className="grid grid-cols-12 px-5 py-2.5 border-b border-white/[0.04]">
                {["Txn ID", "Client", "Date", "Amount", "Status"].map((h, i) => (
                  <p
                    key={h}
                    className={`text-[10px] font-bold uppercase tracking-[0.15em] text-slate-600 ${
                      i === 0 ? "col-span-2" : i === 1 ? "col-span-4" : i === 2 ? "col-span-2" : i === 3 ? "col-span-2 text-right" : "col-span-2 text-right"
                    }`}
                  >
                    {h}
                  </p>
                ))}
              </div>

              <div className="divide-y divide-white/[0.04]">
                {RECENT_TRANSACTIONS.map((txn) => (
                  <div
                    key={txn.id}
                    className="grid grid-cols-12 px-5 py-3.5 items-center hover:bg-white/[0.025] transition-colors"
                  >
                    <p className="col-span-2 text-xs font-mono text-slate-500">{txn.id}</p>
                    <p className="col-span-4 text-xs font-medium text-slate-200 truncate pr-2">{txn.client}</p>
                    <p className="col-span-2 text-xs text-slate-500 flex items-center gap-1">
                      <Clock className="w-3 h-3 flex-shrink-0" />
                      <span className="truncate hidden sm:inline">{txn.date}</span>
                    </p>
                    <p className="col-span-2 text-xs font-bold text-white text-right">{fmt(txn.amount)}</p>
                    <div className="col-span-2 flex justify-end">
                      <span
                        className={`inline-flex items-center gap-1 text-[10px] font-bold px-2 py-1 rounded-full ${
                          txn.status === "Paid"
                            ? "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20"
                            : "bg-amber-500/10 text-amber-400 border border-amber-500/20"
                        }`}
                      >
                        {txn.status === "Paid" ? <CheckCircle2 className="w-2.5 h-2.5" /> : <Clock className="w-2.5 h-2.5" />}
                        {txn.status}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Inventory Velocity & Dead Stock Radar */}
            <div
              className="lg:col-span-2 rounded-2xl border overflow-hidden"
              style={{
                background: "linear-gradient(145deg, rgba(245,158,11,0.04) 0%, rgba(255,255,255,0.01) 100%)",
                borderColor: "rgba(245,158,11,0.18)",
              }}
            >
              <div className="flex items-center justify-between px-5 py-4 border-b border-white/[0.06]">
                <div className="flex items-center gap-2">
                  <Package className="w-4 h-4 text-amber-400" />
                  <h3 className="text-sm font-bold text-white">Inventory Velocity</h3>
                </div>
                <span className="inline-flex items-center gap-1 text-[10px] font-bold px-2 py-1 rounded-full bg-red-500/10 text-red-400 border border-red-500/20">
                  <AlertCircle className="w-2.5 h-2.5" />
                  3 Dead Stock
                </span>
              </div>

              <div className="px-5 py-4 space-y-4">
                {/* Fastest Moving */}
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-[0.15em] text-slate-600 mb-2">
                    🚀 Fastest Moving Item
                  </p>
                  <div
                    className="rounded-xl p-3 border border-emerald-500/20"
                    style={{ background: "rgba(52,211,153,0.06)" }}
                  >
                    <p className="text-xs font-semibold text-white">{INVENTORY_DATA.fastestMoving.name}</p>
                    <div className="flex items-center justify-between mt-1.5">
                      <span className="text-[10px] text-emerald-400 font-bold">
                        {INVENTORY_DATA.fastestMoving.turnover}
                      </span>
                      <span className="text-[10px] text-slate-400">
                        {INVENTORY_DATA.fastestMoving.value} stock
                      </span>
                    </div>
                  </div>
                </div>

                {/* Dead Stock Warnings */}
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-[0.15em] text-slate-600 mb-2">
                    ⚠️ Dead Stock (60+ days)
                  </p>
                  <div className="space-y-2">
                    {INVENTORY_DATA.deadStock.map((item) => (
                      <div
                        key={item.sku}
                        className="flex items-center justify-between rounded-lg px-3 py-2.5 border"
                        style={{
                          background: item.days > 90 ? "rgba(239,68,68,0.05)" : "rgba(245,158,11,0.05)",
                          borderColor: item.days > 90 ? "rgba(239,68,68,0.2)" : "rgba(245,158,11,0.2)",
                        }}
                      >
                        <div className="flex items-center gap-2 min-w-0">
                          <span
                            className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                              item.days > 90 ? "bg-red-400" : "bg-amber-400"
                            }`}
                          />
                          <div className="min-w-0">
                            <p className="text-xs text-white font-medium truncate">{item.name}</p>
                            <p className="text-[9px] text-slate-500">{item.sku}</p>
                          </div>
                        </div>
                        <div className="text-right flex-shrink-0 ml-2">
                          <span
                            className={`text-[10px] font-bold px-1.5 py-0.5 rounded-full ${
                              item.days > 90
                                ? "bg-red-500/10 text-red-400 border border-red-500/20"
                                : "bg-amber-500/10 text-amber-400 border border-amber-500/20"
                            }`}
                          >
                            {item.days}d
                          </span>
                          <p className="text-[10px] text-slate-400 mt-1">{item.value}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Total tied capital */}
                <div className="border-t border-white/[0.06] pt-3 flex items-center justify-between">
                  <p className="text-xs text-slate-400">Total Tied Capital</p>
                  <p className="text-sm font-extrabold text-red-400">{INVENTORY_DATA.totalTiedCapital}</p>
                </div>
              </div>
            </div>

          </div>
        </main>
      </div>
    </div>
  );
}