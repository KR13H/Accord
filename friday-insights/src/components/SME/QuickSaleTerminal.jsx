import { useEffect, useMemo, useRef, useState } from "react";
import { QRCodeCanvas } from "qrcode.react";

import { queueSale } from "../../api/offlineQueue";
import { buildSmeHeaders, getStoredSmeRole } from "../../api/smeAuth";
import { useBarcodeScanner } from "../../utils/barcodeListener";
import { browserPrintReceipt, printReceipt } from "../../utils/thermalPrinter";

const KEYS = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "0", ".", "Clear"];

function formatCurrency(value) {
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2,
  }).format(value);
}

export default function QuickSaleTerminal({ smeRole: smeRoleProp }) {
  const [displayValue, setDisplayValue] = useState("0");
  const [todaysTotal, setTodaysTotal] = useState(0);
  const [submitting, setSubmitting] = useState(false);
  const [toast, setToast] = useState({ message: "", variant: "success" });
  const [showUpiModal, setShowUpiModal] = useState(false);
  const [soundboxMuted, setSoundboxMuted] = useState(false);
  const [soundboxVolume, setSoundboxVolume] = useState(1);
  const [cartItems, setCartItems] = useState([]);
  const [lastSale, setLastSale] = useState(null);
  const amountInputRef = useRef(null);

  const amount = useMemo(() => Number.parseFloat(displayValue || "0") || 0, [displayValue]);
  const cartTotal = useMemo(
    () => cartItems.reduce((sum, item) => sum + Number(item.unit_price || 0) * Number(item.quantity || 1), 0),
    [cartItems]
  );
  const smeRole = useMemo(() => {
    const role = smeRoleProp || getStoredSmeRole();
    return role && role.trim() ? role.trim().toLowerCase() : "owner";
  }, [smeRoleProp]);
  const upiUri = useMemo(() => {
    const fixedAmount = amount.toFixed(2);
    return `upi://pay?pa=yourmerchant@upi&pn=AccordSME&am=${fixedAmount}&cu=INR`;
  }, [amount]);

  useEffect(() => {
    let active = true;
    async function loadSummary() {
      try {
        const res = await fetch("/api/v1/sme/summary", {
          headers: buildSmeHeaders({}, { role: smeRole }),
        });
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
  }, [smeRole]);

  useEffect(() => {
    if (!toast.message) return;
    const id = window.setTimeout(() => setToast({ message: "", variant: "success" }), 2000);
    return () => window.clearTimeout(id);
  }, [toast]);

  useEffect(() => {
    const handlePosShortcut = (event) => {
      const action = event?.detail?.action;
      if (action === "focus-amount") {
        amountInputRef.current?.focus();
        amountInputRef.current?.select?.();
        return;
      }
      if (action === "exact-cash") {
        void recordSale("Cash");
        return;
      }
      if (action === "clear") {
        setDisplayValue("0");
        setCartItems([]);
      }
    };

    window.addEventListener("accord:pos-shortcut", handlePosShortcut);
    return () => window.removeEventListener("accord:pos-shortcut", handlePosShortcut);
  }, [amount, cartTotal, submitting]);

  useBarcodeScanner(async (scannedCode) => {
    try {
      const query = new URLSearchParams({ factory_serial: scannedCode }).toString();
      const response = await fetch(`/api/v1/sme/inventory/items?${query}`, {
        headers: buildSmeHeaders({}, { role: smeRole }),
      });
      const body = await response.json();
      if (!response.ok || !Array.isArray(body?.items) || body.items.length === 0) {
        setToast({ message: `No inventory match for ${scannedCode}`, variant: "warning" });
        return;
      }

      const match = body.items[0];
      setCartItems((prev) => {
        const idx = prev.findIndex((item) => item.system_serial === match.system_serial);
        if (idx >= 0) {
          const next = [...prev];
          next[idx] = { ...next[idx], quantity: Number(next[idx].quantity || 1) + 1 };
          return next;
        }
        return [
          ...prev,
          {
            id: match.id,
            item_name: match.item_name,
            localized_name: match.localized_name || match.item_name,
            factory_serial: match.factory_serial,
            system_serial: match.system_serial,
            unit_price: Number(match.unit_price || 0),
            quantity: 1,
          },
        ];
      });
      setDisplayValue(String((cartTotal + Number(match.unit_price || 0)).toFixed(2)));
      setToast({ message: `Scanned: ${match.item_name}`, variant: "success" });
    } catch (error) {
      setToast({ message: error?.message || "Barcode lookup failed", variant: "error" });
    }
  });

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

  const onAmountInputChange = (event) => {
    const raw = String(event.target.value || "").replace(/[^0-9.]/g, "");
    const firstDot = raw.indexOf(".");
    const normalized = firstDot >= 0 ? `${raw.slice(0, firstDot + 1)}${raw.slice(firstDot + 1).replace(/\./g, "")}` : raw;
    setDisplayValue(normalized || "0");
  };

  const speakPaymentReceived = () => {
    if (soundboxMuted || typeof window === "undefined" || !("speechSynthesis" in window)) {
      return;
    }
    const utterance = new window.SpeechSynthesisUtterance(
      `Accord received payment of ${amount.toFixed(2)} rupees`
    );
    utterance.rate = 1;
    utterance.pitch = 1;
    utterance.volume = soundboxVolume;
    window.speechSynthesis.cancel();
    window.speechSynthesis.speak(utterance);
  };

  const recordSale = async (paymentMethod) => {
    const payableAmount = cartTotal > 0 ? Number(cartTotal.toFixed(2)) : amount;
    if (submitting || payableAmount <= 0) return;
    setSubmitting(true);
    try {
      const category = paymentMethod === "Cash" ? "Cash Sale" : "UPI Payment";
      const payload = {
        type: "INCOME",
        amount: payableAmount,
        category,
        payment_method: paymentMethod,
      };

      if (!navigator.onLine) {
        await queueSale(payload);
        if (paymentMethod === "UPI") {
          speakPaymentReceived();
        }
        setTodaysTotal((prev) => prev + payableAmount);
        setLastSale({
          transaction: {
            id: `offline-${Date.now()}`,
            ...payload,
            created_at: new Date().toISOString(),
          },
          items: cartItems,
        });
        setDisplayValue("0");
        setCartItems([]);
        setShowUpiModal(false);
        setToast({ message: "Offline: Sale Saved Locally.", variant: "warning" });
        return;
      }

      const res = await fetch("/api/v1/sme/transactions", {
        method: "POST",
        headers: buildSmeHeaders({
          "Content-Type": "application/json",
        }, { role: smeRole }),
        body: JSON.stringify(payload),
      });

      const responseBody = await res.json();
      if (!res.ok) {
        throw new Error(responseBody?.detail || "Unable to record sale");
      }

      if (paymentMethod === "UPI") {
        speakPaymentReceived();
      }

      setTodaysTotal((prev) => prev + payableAmount);
      setLastSale({
        transaction: responseBody?.transaction || {
          ...payload,
          id: responseBody?.id || `sale-${Date.now()}`,
          created_at: new Date().toISOString(),
        },
        items: cartItems,
      });
      setDisplayValue("0");
      setCartItems([]);
      setShowUpiModal(false);
      setToast({ message: "Sale recorded", variant: "success" });
    } catch (error) {
      setToast({ message: error?.message || "Request failed", variant: "error" });
    } finally {
      setSubmitting(false);
    }
  };

  const handlePrintReceipt = async () => {
    if (!lastSale?.transaction) {
      setToast({ message: "No completed sale to print", variant: "warning" });
      return;
    }

    try {
      if (typeof navigator !== "undefined" && navigator.usb) {
        await printReceipt(lastSale.transaction, lastSale.items || []);
        setToast({ message: "Receipt printed via thermal printer", variant: "success" });
      } else {
        browserPrintReceipt(lastSale.transaction, lastSale.items || []);
        setToast({ message: "WebUSB unavailable, opened browser print", variant: "warning" });
      }
    } catch {
      try {
        browserPrintReceipt(lastSale.transaction, lastSale.items || []);
        setToast({ message: "Thermal print failed, used browser print fallback", variant: "warning" });
      } catch (error) {
        setToast({ message: error?.message || "Receipt print failed", variant: "error" });
      }
    }
  };

  return (
    <div className="min-h-screen bg-slate-100 text-slate-900 dark:bg-slate-950 dark:text-white px-4 py-6 md:px-8 md:py-10">
      <div className="mx-auto w-full max-w-4xl">
        <div className="rounded-3xl border border-emerald-300 bg-emerald-100/80 dark:border-emerald-300/25 dark:bg-emerald-500/10 px-5 py-4 md:px-8 md:py-6 mb-6">
          <p className="text-sm md:text-base text-emerald-800 dark:text-emerald-100/90">Today&apos;s Total</p>
          <p className="text-4xl md:text-6xl font-black tracking-tight mt-2">{formatCurrency(todaysTotal)}</p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-[1fr_340px] gap-6">
          <div className="rounded-3xl border border-slate-300 bg-white dark:border-slate-700 dark:bg-slate-900/80 p-5 md:p-8">
            <div className="rounded-2xl border border-cyan-300/40 bg-slate-100 dark:border-cyan-300/25 dark:bg-black/60 px-5 py-5 md:px-7 md:py-7 mb-5">
              <p className="text-xs uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">Amount</p>
              <input
                ref={amountInputRef}
                value={displayValue}
                onChange={onAmountInputChange}
                className="w-full bg-transparent border-none outline-none text-5xl md:text-7xl font-black mt-2 text-cyan-700 dark:text-cyan-200 leading-none"
                inputMode="decimal"
                aria-label="POS amount input"
              />
            </div>

            {cartItems.length > 0 ? (
              <div className="mb-5 rounded-2xl border border-slate-300 bg-slate-50 dark:border-slate-700 dark:bg-slate-950/70 px-4 py-4">
                <p className="text-xs uppercase tracking-[0.16em] text-slate-500 dark:text-slate-400 mb-2">Scanned Cart</p>
                <div className="space-y-1 text-sm text-slate-700 dark:text-slate-200">
                  {cartItems.map((item) => (
                    <div key={item.system_serial} className="flex justify-between">
                      <span>{item.item_name} x {item.quantity}</span>
                      <span>{formatCurrency(Number(item.unit_price || 0) * Number(item.quantity || 1))}</span>
                    </div>
                  ))}
                </div>
                <div className="mt-2 pt-2 border-t border-slate-700 flex justify-between text-cyan-200 font-bold">
                  <span>Cart Total</span>
                  <span>{formatCurrency(cartTotal)}</span>
                </div>
              </div>
            ) : null}

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
              disabled={submitting || (cartTotal <= 0 && amount <= 0)}
              onClick={() => recordSale("Cash")}
              className="h-24 md:h-28 rounded-3xl border border-amber-300/45 bg-amber-500/25 text-amber-50 text-2xl md:text-3xl font-black disabled:opacity-60"
            >
              Record Cash Sale
            </button>
            <button
              type="button"
              disabled={submitting || (cartTotal <= 0 && amount <= 0)}
              onClick={() => setShowUpiModal(true)}
              className="h-24 md:h-28 rounded-3xl border border-sky-300/45 bg-sky-500/25 text-sky-50 text-2xl md:text-3xl font-black disabled:opacity-60"
            >
              Record UPI Sale
            </button>
            <button
              type="button"
              disabled={!lastSale?.transaction}
              onClick={handlePrintReceipt}
              className="h-16 rounded-3xl border border-fuchsia-300/45 bg-fuchsia-500/20 text-fuchsia-50 text-lg font-bold disabled:opacity-60"
            >
              Print Receipt
            </button>
            <div className="rounded-2xl border border-slate-700 bg-slate-900/80 px-4 py-3 text-sm text-slate-300">
              F2: focus amount, Space: exact cash, Esc: clear, scan barcode to auto-add.
            </div>
            <div className="rounded-2xl border border-slate-700 bg-slate-900/80 px-4 py-3">
              <div className="flex items-center justify-between gap-3">
                <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Soundbox</p>
                <button
                  type="button"
                  onClick={() => setSoundboxMuted((prev) => !prev)}
                  className="rounded-lg border border-cyan-400/40 bg-cyan-500/15 px-3 py-1.5 text-xs font-semibold text-cyan-100"
                >
                  {soundboxMuted ? "Muted" : "Unmuted"}
                </button>
              </div>
              <label className="mt-3 block text-xs text-slate-300" htmlFor="soundbox-volume">
                Volume: {Math.round(soundboxVolume * 100)}%
              </label>
              <input
                id="soundbox-volume"
                type="range"
                min="0"
                max="1"
                step="0.05"
                value={soundboxVolume}
                onChange={(event) => setSoundboxVolume(Number.parseFloat(event.target.value) || 0)}
                className="mt-2 w-full accent-cyan-300"
              />
            </div>
          </div>
        </div>
      </div>

      {showUpiModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4">
          <div className="w-full max-w-md rounded-3xl border border-slate-700 bg-slate-900 p-6 md:p-8 shadow-2xl">
            <p className="text-xs uppercase tracking-[0.18em] text-slate-400">UPI Payment</p>
            <p className="mt-2 text-3xl font-black text-cyan-100">{formatCurrency(amount)}</p>
            <p className="mt-1 text-sm text-slate-300">Scan this QR from any UPI app and confirm once paid.</p>

            <div className="mt-5 rounded-2xl bg-white p-4 flex items-center justify-center">
              <QRCodeCanvas value={upiUri} size={240} includeMargin level="M" />
            </div>

            <div className="mt-5 grid grid-cols-1 sm:grid-cols-2 gap-3">
              <button
                type="button"
                onClick={() => setShowUpiModal(false)}
                className="h-12 rounded-2xl border border-slate-600 bg-slate-800 text-slate-200 font-semibold"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={submitting || (cartTotal <= 0 && amount <= 0)}
                onClick={() => recordSale("UPI")}
                className="h-12 rounded-2xl border border-emerald-300/45 bg-emerald-500/25 text-emerald-50 font-black disabled:opacity-60"
              >
                Payment Received
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {toast.message ? (
        <div
          className={`fixed bottom-6 right-6 rounded-xl px-4 py-3 font-semibold shadow-xl ${
            toast.variant === "warning"
              ? "bg-yellow-400 text-yellow-950"
              : toast.variant === "error"
                ? "bg-rose-500 text-rose-50"
                : "bg-emerald-500 text-emerald-950"
          }`}
        >
          {toast.message}
        </div>
      ) : null}
    </div>
  );
}
