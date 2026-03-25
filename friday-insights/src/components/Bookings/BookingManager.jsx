import { useEffect, useMemo, useState } from "react";
import apiClient from "../../api/client";

function makeBookingId() {
  return `BK-WEB-${Math.random().toString(36).slice(2, 10).toUpperCase()}`;
}

export default function BookingManager() {
  const [bookings, setBookings] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [form, setForm] = useState({
    project_id: "PRJ-NORTH-01",
    customer_name: "",
    unit_code: "",
    total_consideration: "",
  });

  const totalConsideration = useMemo(
    () => bookings.reduce((sum, row) => sum + Number(row.total_consideration || 0), 0),
    [bookings]
  );

  const fetchBookings = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await apiClient.get("/bookings", { params: { limit: 200 } });
      setBookings(res.data.items || []);
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || "Failed to load bookings");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchBookings();
  }, []);

  const onChange = (key, value) => setForm((prev) => ({ ...prev, [key]: value }));

  const onSubmit = async (event) => {
    event.preventDefault();
    setError("");
    try {
      const payload = {
        booking_id: makeBookingId(),
        project_id: form.project_id.trim(),
        spv_id: "SPV-DEFAULT",
        customer_name: form.customer_name.trim(),
        unit_code: form.unit_code.trim(),
        total_consideration: form.total_consideration || "0",
        booking_date: new Date().toISOString().slice(0, 10),
        status: "ACTIVE",
      };
      const res = await apiClient.post("/bookings", payload);
      const booking = res.data.booking;
      setBookings((prev) => [booking, ...prev]);
      setForm((prev) => ({ ...prev, customer_name: "", unit_code: "", total_consideration: "" }));
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || "Failed to create booking");
    }
  };

  return (
    <section className="rounded-2xl border border-cyan-400/20 bg-slate-950/70 p-4 sm:p-5 space-y-4">
      <div className="flex items-center justify-between gap-3">
        <h2 className="text-lg font-semibold text-cyan-100">Booking Management</h2>
        <button className="text-xs px-3 py-1.5 rounded-lg border border-cyan-500/40" onClick={fetchBookings} type="button">
          Refresh
        </button>
      </div>

      <form onSubmit={onSubmit} className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <input data-cy="booking-project-id" className="rounded-lg bg-slate-900 border border-slate-700 px-3 py-2" placeholder="Project ID" value={form.project_id} onChange={(e) => onChange("project_id", e.target.value)} required />
        <input data-cy="booking-customer-name" className="rounded-lg bg-slate-900 border border-slate-700 px-3 py-2" placeholder="Customer Name" value={form.customer_name} onChange={(e) => onChange("customer_name", e.target.value)} required />
        <input data-cy="booking-unit-code" className="rounded-lg bg-slate-900 border border-slate-700 px-3 py-2" placeholder="Unit Code" value={form.unit_code} onChange={(e) => onChange("unit_code", e.target.value)} required />
        <input data-cy="booking-total-consideration" className="rounded-lg bg-slate-900 border border-slate-700 px-3 py-2" placeholder="Total Consideration" type="number" min="0" step="0.01" value={form.total_consideration} onChange={(e) => onChange("total_consideration", e.target.value)} required />
        <button data-cy="booking-submit" className="md:col-span-2 rounded-lg bg-cyan-600 hover:bg-cyan-500 px-4 py-2 font-semibold" type="submit">Create Booking</button>
      </form>

      {error ? <div className="text-red-300 text-sm">{error}</div> : null}
      <div className="text-xs text-slate-400">Total Consideration In View: {totalConsideration.toLocaleString("en-IN", { style: "currency", currency: "INR" })}</div>

      <div className="overflow-auto rounded-xl border border-slate-800">
        <table className="min-w-full text-sm">
          <thead className="bg-slate-900 text-slate-300">
            <tr>
              <th className="text-left p-2">Booking</th>
              <th className="text-left p-2">Project</th>
              <th className="text-left p-2">Customer</th>
              <th className="text-left p-2">Unit</th>
              <th className="text-left p-2">Status</th>
              <th className="text-right p-2">Consideration</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={6} className="p-3 text-slate-400">Loading...</td></tr>
            ) : bookings.length === 0 ? (
              <tr><td colSpan={6} className="p-3 text-slate-400">No bookings available</td></tr>
            ) : (
              bookings.map((row) => (
                <tr key={row.booking_id} className="border-t border-slate-800">
                  <td className="p-2 font-mono text-xs">{row.booking_id}</td>
                  <td className="p-2">{row.project_id}</td>
                  <td className="p-2">{row.customer_name}</td>
                  <td className="p-2">{row.unit_code}</td>
                  <td className="p-2">{row.status}</td>
                  <td className="p-2 text-right">{Number(row.total_consideration || 0).toLocaleString("en-IN", { style: "currency", currency: "INR" })}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
