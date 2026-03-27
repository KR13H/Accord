import { useEffect, useMemo, useState } from "react";

const KEYS = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "0", ".", "Clear"];

function formatCurrency(value) {
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2,
  }).format(value);
}

export default function QuickSaleTerminal() {
  const [displayValue, setDisplayValue] = useState("0");
  const [todaysTotal, setTodaysTotal] = useState(0);
  const [submitting, setSubmitting] = useState(false);
  const [toast, setToast] = useState("");

  const amount = useMemo(() => Number.parseFloat(displayValue || "0") || 0, [displayValue]);

  useEffect(() => {
    let active = true;
    async function loadSummary() {
      try {
        const res = await fetch("/api/v1/sme/summary");
        const data = await res.json();
        if (!active) return;
        if (res.ok && data?.summary?.income_total) {
          setTodaysTotal(Number.parseFloat(data.summary.income_total) || 0);
        }
      } catch {
        if (active) {
          setTodaysTotal(0);
        }
      }
    }
    loadSummary();
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!toast) return;
    const id = window.setTimeout(() => setToast(""), 1500);
    return () => window.clearTimeout(id);
  }, [toast]);

  const onKeyTap = (key) => {
    if (submitting) return;
    if (key === "Clear") {
      setDisplayValue("0");
      return;
    }
    if (key === ".") {
      if (displayValue.includes(".")) return;
      setDisplayValue(`${displayValue}.`);
      return;
    }
    if (displayValue === "0") {
      setDisplayValue(key);
    } else {
      setDisplayValue(`${displayValue}${key}`);
    }
  };

  const recordSale = async (paymentMethod) => {
    if (submitting || amount <= 0) return;
    setSubmitting(true);
    try {
      const category = paymentMethod === "Cash" ? "Cash Sale" : "UPI Payment";
      const res = await fetch("/api/v1/sme/transactions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          type: "INCOME",
          amount,
          category,
          payment_method: paymentMethod,
        }),
      });

      const payload = await res.json();
      if (!res.ok) {
        throw new Error(payload?.detail || "Unable to record sale");
      }

      setTodaysTotal((prev) => prev + amount);
      setDisplayValue("0");
      setToast("Sale recorded");
    } catch (error) {
      setToast(error?.message || "Request failed");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-white px-4 py-6 md:px-8 md:py-10">
      <div className="mx-auto w-full max-w-4xl">
        <div className="rounded-3xl border border-emerald-300/25 bg-emerald-500/10 px-5 py-4 md:px-8 md:py-6 mb-6">
          <p className="text-sm md:text-base text-emerald-100/90">Today&apos;s Total</p>
          <p className="text-4xl md:text-6xl font-black tracking-tight mt-2">{formatCurrency(todaysTotal)}</p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-[1fr_340px] gap-6">
          <div className="rounded-3xl border border-slate-700 bg-slate-900/80 p-5 md:p-8">
            <div className="rounded-2xl border border-cyan-300/25 bg-black/60 px-5 py-5 md:px-7 md:py-7 mb-5">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Amount</p>
              <p className="text-5xl md:text-7xl font-black mt-2 text-cyan-200 leading-none">{displayValue}</p>
            </div>

            <div className="grid grid-cols-3 gap-3 md:gap-4">
              {KEYS.map((key) => (
                <button
                  key={key}
                  type="button"
                  onClick={() => onKeyTap(key)}
                  className={`h-20 md:h-24 rounded-2xl text-2xl md:text-3xl font-bold transition active:scale-[0.98] ${
                    key === "Clear"
                      ? "bg-rose-500/20 text-rose-100 border border-rose-400/50"
                      : "bg-slate-800 text-cyan-100 border border-cyan-400/35"
                  }`}
                >
                  {key}
                </button>
              ))}
            </div>
          </div>

          <div className="flex flex-col gap-4">
            <button
              type="button"
              disabled={submitting || amount <= 0}
              onClick={() => recordSale("Cash")}
              className="h-24 md:h-28 rounded-3xl border border-amber-300/45 bg-amber-500/25 text-amber-50 text-2xl md:text-3xl font-black disabled:opacity-60"
            >
              Record Cash Sale
            </button>
            <button
              type="button"
              disabled={submitting || amount <= 0}
              onClick={() => recordSale("UPI")}
              className="h-24 md:h-28 rounded-3xl border border-sky-300/45 bg-sky-500/25 text-sky-50 text-2xl md:text-3xl font-black disabled:opacity-60"
            >
              Record UPI Sale
            </button>
            <div className="rounded-2xl border border-slate-700 bg-slate-900/80 px-4 py-3 text-sm text-slate-300">
              Designed for fast tablet entry. Tap amount, choose payment, done.
            </div>
          </div>
        </div>
      </div>

      {toast ? (
        <div className="fixed bottom-6 right-6 rounded-xl bg-emerald-500 text-emerald-950 px-4 py-3 font-semibold shadow-xl">
          {toast}
        </div>
      ) : null}
    </div>
  );
}
