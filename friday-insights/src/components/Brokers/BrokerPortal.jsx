import { useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";

export default function BrokerPortal() {
  const [form, setForm] = useState({
    rera_registration_number: "",
    customer_name: "",
    project_id: "",
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);

  const onChange = (key, value) => {
    setForm((prev) => ({ ...prev, [key]: value }));
  };

  const onSubmit = async (event) => {
    event.preventDefault();
    setLoading(true);
    setError("");
    setResult(null);

    try {
      const response = await fetch(`${API_BASE}/api/v1/brokers/register`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.detail || "Lead registration failed");
      }
      setResult(data);
      setForm({ rera_registration_number: "", customer_name: "", project_id: "" });
    } catch (err) {
      setError(String(err.message || err));
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className="min-h-screen bg-slate-950 text-white px-4 py-12">
      <section className="max-w-2xl mx-auto rounded-3xl border border-cyan-400/30 bg-slate-900/80 p-8 shadow-2xl">
        <p className="text-cyan-300 uppercase tracking-[0.2em] text-xs">Accord Broker Network</p>
        <h1 className="text-3xl font-bold mt-2">Register Buyer Lead</h1>
        <p className="text-slate-300 mt-3 text-sm">
          Submit your lead details to secure commission attribution when the buyer books a unit.
        </p>

        <form className="mt-8 space-y-4" onSubmit={onSubmit}>
          <label className="block">
            <span className="text-sm text-slate-200">RERA Registration Number</span>
            <input
              className="mt-1 w-full rounded-xl bg-slate-950 border border-slate-700 px-3 py-2"
              value={form.rera_registration_number}
              onChange={(e) => onChange("rera_registration_number", e.target.value)}
              placeholder="DLRERA/AGT/2026/01234"
              required
            />
          </label>

          <label className="block">
            <span className="text-sm text-slate-200">Customer Name</span>
            <input
              className="mt-1 w-full rounded-xl bg-slate-950 border border-slate-700 px-3 py-2"
              value={form.customer_name}
              onChange={(e) => onChange("customer_name", e.target.value)}
              placeholder="Amanpreet Kaur"
              required
            />
          </label>

          <label className="block">
            <span className="text-sm text-slate-200">Project</span>
            <input
              className="mt-1 w-full rounded-xl bg-slate-950 border border-slate-700 px-3 py-2"
              value={form.project_id}
              onChange={(e) => onChange("project_id", e.target.value)}
              placeholder="NOIDA-TOWER-A"
              required
            />
          </label>

          <button
            type="submit"
            className="w-full rounded-xl bg-cyan-500 hover:bg-cyan-400 text-slate-950 font-semibold py-2.5 disabled:opacity-60"
            disabled={loading}
          >
            {loading ? "Submitting..." : "Register Lead"}
          </button>
        </form>

        {error ? <p className="mt-4 text-rose-300 text-sm">{error}</p> : null}
        {result ? (
          <div className="mt-4 rounded-xl border border-emerald-500/40 bg-emerald-500/10 p-4 text-sm">
            <p className="font-semibold">Lead registered successfully.</p>
            <p>Lead ID: {result.lead_id}</p>
            <p>Broker ID: {result.broker_id}</p>
          </div>
        ) : null}
      </section>
    </main>
  );
}
