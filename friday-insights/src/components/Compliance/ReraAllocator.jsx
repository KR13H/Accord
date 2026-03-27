import { useEffect, useMemo, useState } from "react";
import apiClient from "../../api/client";

export default function ReraAllocator() {
  const [bookings, setBookings] = useState([]);
  const [bookingId, setBookingId] = useState("");
  const [receiptAmount, setReceiptAmount] = useState("0");
  const [loading, setLoading] = useState(false);
  const [toast, setToast] = useState({ type: "", message: "" });

  const amount = Number(receiptAmount || 0);
  const reraSplit = useMemo(() => (amount * 0.7).toFixed(2), [amount]);
  const opsSplit = useMemo(() => (amount * 0.3).toFixed(2), [amount]);

  useEffect(() => {
    (async () => {
      try {
        const res = await apiClient.get("/bookings", { params: { status: "ACTIVE", limit: 200 } });
        const rows = res.data.items || [];
        setBookings(rows);
        if (rows.length) setBookingId(rows[0].booking_id);
      } catch (err) {
        setToast({ type: "error", message: err?.response?.data?.detail || "Failed to load active bookings" });
      }
    })();
  }, []);

  const executeAllocation = async () => {
    setLoading(true);
    setToast({ type: "", message: "" });
    try {
      const paymentRef = `WEB-${Date.now()}`;
      await apiClient.post("/rera/allocations", {
        booking_id: bookingId,
        payment_reference: paymentRef,
        event_type: "PAYMENT",
        receipt_amount: String(amount.toFixed(2)),
      });
      setToast({ type: "success", message: `Allocation executed for ${bookingId}` });
    } catch (err) {
      setToast({ type: "error", message: err?.response?.data?.detail || err.message || "Allocation failed" });
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="rounded-2xl border border-cyan-400/20 bg-slate-950/70 p-4 sm:p-5 space-y-4">
      <h2 className="text-lg font-semibold text-cyan-100">RERA 70/30 Allocator</h2>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <select className="rounded-lg bg-slate-900 border border-slate-700 px-3 py-2" value={bookingId} onChange={(e) => setBookingId(e.target.value)}>
          {bookings.map((b) => (
            <option key={b.booking_id} value={b.booking_id}>{b.booking_id} - {b.customer_name}</option>
          ))}
        </select>
        <input className="rounded-lg bg-slate-900 border border-slate-700 px-3 py-2" type="number" min="0" step="0.01" value={receiptAmount} onChange={(e) => setReceiptAmount(e.target.value)} />
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/10 p-3">
          <div className="text-xs text-emerald-200">70% Escrow / Construction</div>
          <div className="text-xl font-bold text-emerald-100">₹{Number(reraSplit).toLocaleString("en-IN")}</div>
        </div>
        <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3">
          <div className="text-xs text-amber-200">30% Operations</div>
          <div className="text-xl font-bold text-amber-100">₹{Number(opsSplit).toLocaleString("en-IN")}</div>
        </div>
      </div>

      <button
        type="button"
        disabled={loading || !bookingId || amount <= 0}
        onClick={executeAllocation}
        className="rounded-lg bg-cyan-600 hover:bg-cyan-500 disabled:opacity-50 px-4 py-2 font-semibold"
      >
        {loading ? "Executing..." : "Execute Allocation"}
      </button>

      {toast.message ? (
        <div className={`text-sm ${toast.type === "error" ? "text-red-300" : "text-emerald-300"}`}>
          {toast.message}
        </div>
      ) : null}
    </section>
  );
}
