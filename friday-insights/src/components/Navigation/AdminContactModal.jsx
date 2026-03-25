import { useEffect, useMemo, useState } from "react";
import apiClient from "../../api/client";

const URGENCY_OPTIONS = ["Low", "Medium", "High"];

const INITIAL_FORM = {
  subject: "",
  urgency: "Medium",
  message: "",
};

export default function AdminContactModal({ open, onClose }) {
  const [form, setForm] = useState(INITIAL_FORM);
  const [loading, setLoading] = useState(false);
  const [toast, setToast] = useState({ type: "", message: "" });

  useEffect(() => {
    if (!open) {
      return undefined;
    }

    const handleEsc = (event) => {
      if (event.key === "Escape" && !loading) {
        onClose();
      }
    };

    window.addEventListener("keydown", handleEsc);
    return () => window.removeEventListener("keydown", handleEsc);
  }, [loading, onClose, open]);

  const canSubmit = useMemo(() => {
    return form.subject.trim().length >= 3 && form.message.trim().length >= 10;
  }, [form]);

  if (!open) {
    return null;
  }

  const updateField = (key, value) => {
    setForm((prev) => ({ ...prev, [key]: value }));
  };

  const submitSupportRequest = async (event) => {
    event.preventDefault();
    if (!canSubmit) {
      setToast({ type: "error", message: "Please enter a valid subject and message." });
      return;
    }

    setLoading(true);
    setToast({ type: "", message: "" });

    try {
      await apiClient.post("/support/contact", {
        subject: form.subject.trim(),
        urgency: form.urgency,
        message: form.message.trim(),
      });
      setToast({ type: "success", message: "Support request sent. Admin has been notified." });
      setForm(INITIAL_FORM);
      window.setTimeout(() => {
        onClose();
      }, 900);
    } catch (err) {
      setToast({
        type: "error",
        message: err?.response?.data?.detail || err?.message || "Unable to send support request",
      });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
      <div className="w-full max-w-lg rounded-2xl border border-cyan-400/25 bg-slate-900 p-5 shadow-2xl shadow-cyan-900/30">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-cyan-100">Contact Support</h2>
            <p className="mt-1 text-sm text-slate-400">Route urgent platform issues directly to the system admin.</p>
          </div>
          <button
            type="button"
            onClick={onClose}
            disabled={loading}
            className="rounded-md border border-slate-700 px-2 py-1 text-sm text-slate-300 hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
          >
            Close
          </button>
        </div>

        <form onSubmit={submitSupportRequest} className="mt-4 space-y-3">
          <div className="space-y-1">
            <label className="text-sm text-slate-300" htmlFor="support-subject">Subject</label>
            <input
              id="support-subject"
              type="text"
              value={form.subject}
              onChange={(event) => updateField("subject", event.target.value)}
              placeholder="Eg: RERA allocation failing for booking BK-1021"
              className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none focus:border-cyan-500"
              maxLength={160}
              required
            />
          </div>

          <div className="space-y-1">
            <label className="text-sm text-slate-300" htmlFor="support-urgency">Urgency</label>
            <select
              id="support-urgency"
              value={form.urgency}
              onChange={(event) => updateField("urgency", event.target.value)}
              className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none focus:border-cyan-500"
            >
              {URGENCY_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </div>

          <div className="space-y-1">
            <label className="text-sm text-slate-300" htmlFor="support-message">Message</label>
            <textarea
              id="support-message"
              rows={5}
              value={form.message}
              onChange={(event) => updateField("message", event.target.value)}
              placeholder="Describe the issue, exact workflow, and expected behavior."
              className="w-full resize-y rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none focus:border-cyan-500"
              maxLength={2000}
              required
            />
          </div>

          {toast.message ? (
            <div className={`rounded-lg border px-3 py-2 text-sm ${toast.type === "error" ? "border-red-500/40 bg-red-500/10 text-red-200" : "border-emerald-500/40 bg-emerald-500/10 text-emerald-200"}`}>
              {toast.message}
            </div>
          ) : null}

          <div className="flex items-center justify-end gap-2 pt-1">
            <button
              type="button"
              onClick={onClose}
              disabled={loading}
              className="rounded-lg border border-slate-700 px-3 py-2 text-sm text-slate-200 hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading || !canSubmit}
              className="rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-500 disabled:cursor-not-allowed disabled:bg-cyan-900"
            >
              {loading ? "Sending..." : "Send to Support"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
