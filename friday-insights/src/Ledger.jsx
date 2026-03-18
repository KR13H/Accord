import { useMemo, useState } from "react";
import { Link, NavLink } from "react-router-dom";
import {
  BookOpen,
  CalendarDays,
  LayoutDashboard,
  Menu,
  Plus,
  ReceiptText,
  Scale,
  ShieldCheck,
  X,
} from "lucide-react";

const ACCOUNT_OPTIONS = [
  "Cash",
  "Bank",
  "Sales",
  "Inventory",
  "Accounts Receivable",
  "Accounts Payable",
  "Purchases",
  "GST Output",
  "GST Input",
  "Expenses",
];

const NAV_ITEMS = [
  { id: "dashboard", label: "Dashboard", to: "/dashboard", icon: LayoutDashboard },
  { id: "ledger", label: "Ledger", to: "/ledger", icon: BookOpen },
  { id: "insights", label: "AI Insights", to: "/insights", icon: ShieldCheck },
];

const INITIAL_LEDGER_ROWS = [
  {
    id: 1,
    date: "2026-03-15",
    voucher: "JV-1128",
    account: "Cash",
    description: "Retail sales collection",
    debit: 85000,
    credit: 0,
  },
  {
    id: 2,
    date: "2026-03-15",
    voucher: "JV-1128",
    account: "Sales",
    description: "Retail sales collection",
    debit: 0,
    credit: 85000,
  },
  {
    id: 3,
    date: "2026-03-16",
    voucher: "JV-1129",
    account: "Inventory",
    description: "Purchase of spare parts",
    debit: 42000,
    credit: 0,
  },
  {
    id: 4,
    date: "2026-03-16",
    voucher: "JV-1129",
    account: "Accounts Payable",
    description: "Purchase of spare parts",
    debit: 0,
    credit: 42000,
  },
  {
    id: 5,
    date: "2026-03-17",
    voucher: "JV-1130",
    account: "Bank",
    description: "Customer invoice received",
    debit: 67500,
    credit: 0,
  },
  {
    id: 6,
    date: "2026-03-17",
    voucher: "JV-1130",
    account: "Accounts Receivable",
    description: "Customer invoice received",
    debit: 0,
    credit: 67500,
  },
];

const createLine = (id) => ({
  id,
  account: "",
  debit: "",
  credit: "",
});

const formatInr = (value) =>
  Number(value || 0).toLocaleString("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2,
  });

export default function Ledger() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [entryDate, setEntryDate] = useState("2026-03-17");
  const [voucher, setVoucher] = useState("JV-1131");
  const [description, setDescription] = useState("");
  const [lineSeed, setLineSeed] = useState(3);
  const [lines, setLines] = useState([createLine(1), createLine(2)]);
  const [ledgerRows, setLedgerRows] = useState(INITIAL_LEDGER_ROWS);

  const totals = useMemo(() => {
    return lines.reduce(
      (acc, line) => {
        const debit = Number(line.debit) || 0;
        const credit = Number(line.credit) || 0;
        return {
          debit: acc.debit + debit,
          credit: acc.credit + credit,
        };
      },
      { debit: 0, credit: 0 }
    );
  }, [lines]);

  const hasAccountsOnPostedLines = lines
    .filter((line) => (Number(line.debit) || 0) > 0 || (Number(line.credit) || 0) > 0)
    .every((line) => line.account);

  const hasAnyAmount = totals.debit > 0 || totals.credit > 0;
  const isBalanced = Math.abs(totals.debit - totals.credit) < 0.0001;
  const canPost =
    isBalanced &&
    hasAnyAmount &&
    hasAccountsOnPostedLines &&
    Boolean(entryDate) &&
    Boolean(voucher.trim()) &&
    Boolean(description.trim());

  const updateLine = (id, field, value) => {
    setLines((current) =>
      current.map((line) => {
        if (line.id !== id) return line;
        return { ...line, [field]: value };
      })
    );
  };

  const addLine = () => {
    setLines((current) => [...current, createLine(lineSeed)]);
    setLineSeed((prev) => prev + 1);
  };

  const removeLine = (id) => {
    setLines((current) => {
      if (current.length <= 2) return current;
      return current.filter((line) => line.id !== id);
    });
  };

  const postTransaction = () => {
    if (!canPost) return;

    const postedRows = lines
      .filter((line) => (Number(line.debit) || 0) > 0 || (Number(line.credit) || 0) > 0)
      .map((line, idx) => ({
        id: Date.now() + idx,
        date: entryDate,
        voucher: voucher.trim(),
        account: line.account,
        description: description.trim(),
        debit: Number(line.debit) || 0,
        credit: Number(line.credit) || 0,
      }));

    setLedgerRows((current) => [
      ...postedRows,
      ...current,
    ]);

    setDescription("");
    setLines([createLine(1), createLine(2)]);
    setLineSeed(3);
  };

  return (
    <div className="min-h-screen text-white" style={{ background: "#020817" }}>
      <div className="fixed inset-0 pointer-events-none overflow-hidden" aria-hidden>
        <div
          className="absolute -top-40 -left-40 w-[560px] h-[560px] rounded-full opacity-20"
          style={{ background: "radial-gradient(circle, rgba(30,64,175,0.5) 0%, transparent 65%)" }}
        />
        <div
          className="absolute bottom-0 right-0 w-[520px] h-[520px] rounded-full opacity-10"
          style={{ background: "radial-gradient(circle, rgba(56,189,248,0.4) 0%, transparent 65%)" }}
        />
      </div>

      <div className="relative z-10 flex min-h-screen overflow-hidden">
        {sidebarOpen && (
          <button
            className="fixed inset-0 z-20 bg-black/60 md:hidden"
            onClick={() => setSidebarOpen(false)}
            aria-label="Close navigation"
          />
        )}

        <aside
          className={`
            fixed left-0 top-0 z-30 h-full w-[244px] border-r border-white/[0.08]
            bg-slate-950/85 backdrop-blur-xl transition-transform duration-300
            ${sidebarOpen ? "translate-x-0" : "-translate-x-full"}
            md:translate-x-0 md:static
          `}
        >
          <div className="flex h-16 items-center gap-2 border-b border-white/[0.08] px-5">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-blue-600">
              <Scale className="h-4 w-4" />
            </div>
            <div className="text-sm font-bold tracking-tight">
              Friday <span className="text-blue-400">Ledger</span>
            </div>
          </div>

          <nav className="space-y-1 px-3 py-5">
            {NAV_ITEMS.map((item) => {
              const Icon = item.icon;
              return (
                <NavLink
                  key={item.id}
                  to={item.to}
                  className={({ isActive }) =>
                    `flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm transition-colors ${
                      isActive
                        ? "border border-blue-500/30 bg-blue-600/15 text-blue-300"
                        : "text-slate-400 hover:bg-white/[0.05] hover:text-white"
                    }`
                  }
                  onClick={() => setSidebarOpen(false)}
                >
                  <Icon className="h-4 w-4" />
                  {item.label}
                </NavLink>
              );
            })}
          </nav>

          <div className="mt-auto border-t border-white/[0.08] p-4 text-xs text-slate-500">
            Double-entry mode enabled
          </div>
        </aside>

        <div className="flex min-w-0 flex-1 flex-col">
          <header className="sticky top-0 z-20 flex h-16 items-center border-b border-white/[0.08] bg-slate-950/75 px-4 backdrop-blur-xl sm:px-6">
            <button
              className="rounded-lg p-2 text-slate-400 transition-colors hover:bg-white/[0.06] hover:text-white md:hidden"
              onClick={() => setSidebarOpen(true)}
              aria-label="Open navigation"
            >
              <Menu className="h-5 w-5" />
            </button>

            <div className="ml-2 sm:ml-0">
              <p className="text-sm font-semibold">General Journal</p>
              <p className="text-xs text-slate-500">Double-entry engine foundation</p>
            </div>

            <div className="ml-auto flex items-center gap-2 text-xs">
              <Link
                to="/dashboard"
                className="rounded-lg border border-white/[0.1] bg-white/[0.03] px-3 py-1.5 text-slate-300 transition-colors hover:text-white"
              >
                Back to Dashboard
              </Link>
            </div>
          </header>

          <main className="flex-1 space-y-6 overflow-y-auto px-4 py-6 sm:px-6">
            <section
              className="rounded-2xl border border-white/[0.1] p-4 sm:p-6"
              style={{
                background: "linear-gradient(145deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.01) 100%)",
              }}
            >
              <div className="mb-5 flex items-center justify-between gap-3">
                <div>
                  <h1 className="text-base font-bold sm:text-lg">General Journal Entry</h1>
                  <p className="text-xs text-slate-500">Every transaction must balance: total debits = total credits.</p>
                </div>
                <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/25 bg-emerald-500/10 px-2.5 py-1 text-[11px] font-semibold text-emerald-400">
                  <ShieldCheck className="h-3.5 w-3.5" />
                  Validation Active
                </span>
              </div>

              <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                <label className="text-xs text-slate-400">
                  Date
                  <div className="relative mt-1.5">
                    <CalendarDays className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-500" />
                    <input
                      type="date"
                      value={entryDate}
                      onChange={(e) => setEntryDate(e.target.value)}
                      className="w-full rounded-lg border border-white/[0.1] bg-white/[0.03] py-2 pl-9 pr-3 text-sm text-white outline-none transition-colors focus:border-blue-500/50"
                    />
                  </div>
                </label>

                <label className="text-xs text-slate-400">
                  Reference / Voucher No.
                  <input
                    type="text"
                    value={voucher}
                    onChange={(e) => setVoucher(e.target.value)}
                    placeholder="JV-1131"
                    className="mt-1.5 w-full rounded-lg border border-white/[0.1] bg-white/[0.03] px-3 py-2 text-sm text-white outline-none transition-colors focus:border-blue-500/50"
                  />
                </label>

                <label className="text-xs text-slate-400 sm:col-span-1">
                  Description
                  <input
                    type="text"
                    value={description}
                    onChange={(e) => setDescription(e.target.value)}
                    placeholder="Narration for this voucher"
                    className="mt-1.5 w-full rounded-lg border border-white/[0.1] bg-white/[0.03] px-3 py-2 text-sm text-white outline-none transition-colors focus:border-blue-500/50"
                  />
                </label>
              </div>

              <div className="mt-5 space-y-3">
                <div className="hidden grid-cols-12 gap-2 px-2 text-[10px] font-bold uppercase tracking-[0.16em] text-slate-600 sm:grid">
                  <p className="col-span-5">Account</p>
                  <p className="col-span-3 text-right">Debit</p>
                  <p className="col-span-3 text-right">Credit</p>
                  <p className="col-span-1 text-right">Action</p>
                </div>

                {lines.map((line) => (
                  <div key={line.id} className="grid grid-cols-1 gap-2 rounded-xl border border-white/[0.08] bg-white/[0.02] p-3 sm:grid-cols-12 sm:items-center">
                    <div className="sm:col-span-5">
                      <select
                        value={line.account}
                        onChange={(e) => updateLine(line.id, "account", e.target.value)}
                        className="w-full rounded-lg border border-white/[0.1] bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-blue-500/50"
                      >
                        <option value="">Select account</option>
                        {ACCOUNT_OPTIONS.map((account) => (
                          <option key={account} value={account}>
                            {account}
                          </option>
                        ))}
                      </select>
                    </div>

                    <div className="sm:col-span-3">
                      <input
                        type="number"
                        min="0"
                        step="0.01"
                        value={line.debit}
                        onChange={(e) => updateLine(line.id, "debit", e.target.value)}
                        placeholder="0.00"
                        className="w-full rounded-lg border border-white/[0.1] bg-slate-950 px-3 py-2 text-right text-sm text-emerald-300 outline-none focus:border-emerald-500/50"
                      />
                    </div>

                    <div className="sm:col-span-3">
                      <input
                        type="number"
                        min="0"
                        step="0.01"
                        value={line.credit}
                        onChange={(e) => updateLine(line.id, "credit", e.target.value)}
                        placeholder="0.00"
                        className="w-full rounded-lg border border-white/[0.1] bg-slate-950 px-3 py-2 text-right text-sm text-blue-300 outline-none focus:border-blue-500/50"
                      />
                    </div>

                    <div className="flex justify-end sm:col-span-1">
                      <button
                        type="button"
                        onClick={() => removeLine(line.id)}
                        className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/[0.1] text-slate-500 transition-colors hover:text-red-400 disabled:cursor-not-allowed disabled:opacity-40"
                        disabled={lines.length <= 2}
                        aria-label="Remove line"
                      >
                        <X className="h-4 w-4" />
                      </button>
                    </div>
                  </div>
                ))}
              </div>

              <div className="mt-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <button
                  type="button"
                  onClick={addLine}
                  className="inline-flex items-center justify-center gap-1.5 rounded-lg border border-white/[0.12] bg-white/[0.03] px-3 py-2 text-xs font-semibold text-slate-300 transition-colors hover:text-white sm:justify-start"
                >
                  <Plus className="h-3.5 w-3.5" />
                  Add line item
                </button>

                <div className="grid grid-cols-2 gap-2 sm:flex sm:items-center sm:gap-6">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.14em] text-slate-600">Total Debits</p>
                    <p className="text-sm font-bold text-emerald-400">{formatInr(totals.debit)}</p>
                  </div>
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.14em] text-slate-600">Total Credits</p>
                    <p className="text-sm font-bold text-blue-400">{formatInr(totals.credit)}</p>
                  </div>
                  <div className="col-span-2">
                    <p
                      className={`text-xs font-semibold ${
                        isBalanced ? "text-emerald-400" : "text-red-400"
                      }`}
                    >
                      {isBalanced ? "Balanced: ready to post" : "Out of balance: debits must equal credits"}
                    </p>
                  </div>
                </div>
              </div>

              <div className="mt-4">
                <button
                  type="button"
                  onClick={postTransaction}
                  disabled={!canPost}
                  className={`inline-flex w-full items-center justify-center gap-2 rounded-xl px-4 py-2.5 text-sm font-bold transition-all sm:w-auto ${
                    canPost
                      ? "bg-blue-600 text-white hover:bg-blue-500 hover:shadow-lg hover:shadow-blue-500/25"
                      : "cursor-not-allowed border border-white/[0.1] bg-white/[0.04] text-slate-500"
                  }`}
                >
                  <ReceiptText className="h-4 w-4" />
                  Post Transaction
                </button>
              </div>
            </section>

            <section
              className="overflow-hidden rounded-2xl border border-white/[0.1]"
              style={{
                background: "linear-gradient(145deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.01) 100%)",
              }}
            >
              <div className="border-b border-white/[0.08] px-4 py-4 sm:px-6">
                <h2 className="text-base font-bold">Chart of Accounts and Ledger View</h2>
                <p className="text-xs text-slate-500">Recent posted entries (mock and live in-session).</p>
              </div>

              <div className="overflow-x-auto">
                <table className="min-w-full text-left">
                  <thead>
                    <tr className="border-b border-white/[0.08] text-[10px] uppercase tracking-[0.14em] text-slate-600">
                      <th className="px-4 py-3 sm:px-6">Date</th>
                      <th className="px-4 py-3">Voucher</th>
                      <th className="px-4 py-3">Account</th>
                      <th className="px-4 py-3">Description</th>
                      <th className="px-4 py-3 text-right">Debit</th>
                      <th className="px-4 py-3 text-right sm:px-6">Credit</th>
                    </tr>
                  </thead>
                  <tbody>
                    {ledgerRows.map((row) => (
                      <tr key={row.id} className="border-b border-white/[0.04] text-xs text-slate-300">
                        <td className="whitespace-nowrap px-4 py-3 sm:px-6">{row.date}</td>
                        <td className="whitespace-nowrap px-4 py-3 font-medium text-slate-200">{row.voucher}</td>
                        <td className="whitespace-nowrap px-4 py-3">{row.account}</td>
                        <td className="px-4 py-3">{row.description}</td>
                        <td className="whitespace-nowrap px-4 py-3 text-right font-semibold text-emerald-300">
                          {row.debit ? formatInr(row.debit) : "-"}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-right font-semibold text-blue-300 sm:px-6">
                          {row.credit ? formatInr(row.credit) : "-"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          </main>
        </div>
      </div>
    </div>
  );
}