import { useEffect, useMemo, useState } from "react";
import {
  AlertTriangle,
  ArrowRight,
  BadgeAlert,
  Bot,
  BrainCircuit,
  Camera,
  CheckCircle2,
  FileCode2,
  Fingerprint,
  Loader2,
  PanelLeft,
  Radar,
  Sparkles,
  ShieldAlert,
  Zap,
} from "lucide-react";
import { AnimatePresence, motion } from "framer-motion";
import "./AiInsights.css";

function formatINR(value) {
  const num = Number(value ?? 0);
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2,
  }).format(Number.isFinite(num) ? num : 0);
}

function formatDateTime(value) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString("en-IN", {
    dateStyle: "medium",
    timeStyle: "short",
  });
}

function adviceBadge(advice) {
  if (advice === "WITHHOLD_GST_PORTION") {
    return {
      className: "bg-red-500/15 text-red-300 border-red-500/40",
      label: "Net of GST",
      icon: <ShieldAlert className="w-3.5 h-3.5" />,
    };
  }
  if (advice === "REVIEW_BEFORE_PAYMENT") {
    return {
      className: "bg-amber-500/15 text-amber-300 border-amber-500/40",
      label: "Review Before Payment",
      icon: <BadgeAlert className="w-3.5 h-3.5" />,
    };
  }
  return {
    className: "bg-emerald-500/15 text-emerald-300 border-emerald-500/40",
    label: "Standard Payment",
    icon: <CheckCircle2 className="w-3.5 h-3.5" />,
  };
}

function ComplianceGauge({ score }) {
  const clamped = Math.max(0, Math.min(100, Number(score || 0)));
  const radius = 62;
  const circumference = 2 * Math.PI * radius;
  const dash = circumference - (clamped / 100) * circumference;
  const tone = clamped >= 75 ? "#10b981" : clamped >= 45 ? "#22d3ee" : "#ef4444";

  return (
    <div className="relative w-40 h-40">
      <svg viewBox="0 0 160 160" className="w-full h-full" style={{ willChange: "transform", transform: "translateZ(0)" }}>
        <circle cx="80" cy="80" r={radius} stroke="rgba(148,163,184,0.2)" strokeWidth="11" fill="none" />
        <motion.circle
          cx="80"
          cy="80"
          r={radius}
          stroke={tone}
          strokeWidth="11"
          fill="none"
          strokeLinecap="round"
          transform="rotate(-90 80 80)"
          initial={{ strokeDashoffset: circumference }}
          animate={{ strokeDashoffset: dash }}
          transition={{ duration: 1, ease: "easeOut" }}
          strokeDasharray={circumference}
        />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-3xl font-black tracking-tight">{Math.round(clamped)}</span>
        <span className="text-[10px] uppercase tracking-[0.22em] text-slate-400">Health Score</span>
      </div>
    </div>
  );
}

export default function AiInsights() {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [isRunningWizard, setIsRunningWizard] = useState(false);
  const [isDownloadingAudit, setIsDownloadingAudit] = useState(false);
  const [isArchivingBatch, setIsArchivingBatch] = useState(false);
  const [isApprovingBatch, setIsApprovingBatch] = useState(false);
  const [wizardResult, setWizardResult] = useState("");
  const [lastExportEntryIds, setLastExportEntryIds] = useState([]);
  const [lastExportHash, setLastExportHash] = useState("");
  const [includeFiled, setIncludeFiled] = useState(false);
  const [reversalSummary, setReversalSummary] = useState(null);
  const [isVerifyingExport, setIsVerifyingExport] = useState(false);
  const [verifyResult, setVerifyResult] = useState(null);
  const [adminRole, setAdminRole] = useState("admin");
  const [adminId, setAdminId] = useState("101");
  const [minMonthlyBalance, setMinMonthlyBalance] = useState("");
  const [isCertifyingSafeHarbor, setIsCertifyingSafeHarbor] = useState(false);

  const [gstin, setGstin] = useState("");
  const [invoiceAmount, setInvoiceAmount] = useState("");
  const [adviceData, setAdviceData] = useState(null);
  const [adviceLoading, setAdviceLoading] = useState(false);
  const [adviceError, setAdviceError] = useState("");
  const [biometricToken, setBiometricToken] = useState("");
  const [biometricAction, setBiometricAction] = useState("");
  const [biometricExpiresAt, setBiometricExpiresAt] = useState("");
  const [isMintingBiometric, setIsMintingBiometric] = useState(false);
  const [fridayQuestion, setFridayQuestion] = useState("What are my top 3 cash-flow risks today?");
  const [fridayAnswer, setFridayAnswer] = useState("");
  const [fridayError, setFridayError] = useState("");
  const [isAskingFriday, setIsAskingFriday] = useState(false);
  const [isFridayBubbleOpen, setIsFridayBubbleOpen] = useState(false);
  const [fridayStreamText, setFridayStreamText] = useState("");
  const [isUploadingReceipt, setIsUploadingReceipt] = useState(false);
  const [receiptError, setReceiptError] = useState("");
  const [receiptResult, setReceiptResult] = useState(null);
  const [isExportingTally, setIsExportingTally] = useState(false);
  const [isBatchUploading, setIsBatchUploading] = useState(false);
  const [batchUploadError, setBatchUploadError] = useState("");
  const [batchUploadResults, setBatchUploadResults] = useState(null);

  useEffect(() => {
    if (!fridayAnswer) {
      setFridayStreamText("");
      return;
    }
    setFridayStreamText("");
    let index = 0;
    const timer = setInterval(() => {
      index += 5;
      setFridayStreamText(fridayAnswer.slice(0, index));
      if (index >= fridayAnswer.length) {
        clearInterval(timer);
      }
    }, 14);
    return () => clearInterval(timer);
  }, [fridayAnswer]);

  const buildNudgeMessage = (vendor) => {
    const monthLabel = new Date().toLocaleString("en-IN", { month: "short", year: "numeric" });
    return (
      `Hello ${vendor.legal_name || "Team"},\n\n` +
      `Your GSTR-1 for ${monthLabel} appears pending on our side for GSTIN ${vendor.gstin}. ` +
      "Automatically flagged by Accord Compliance Engine. Please file by the 11th to avoid downstream reversals.\n\n" +
      "Regards,\nAccord Compliance Desk"
    );
  };

  const openEmailNudge = (vendor) => {
    const subject = encodeURIComponent(`GSTR-1 filing reminder for ${vendor.gstin}`);
    const body = encodeURIComponent(buildNudgeMessage(vendor));
    window.open(`mailto:?subject=${subject}&body=${body}`, "_blank");
  };

  const openWhatsAppNudge = (vendor) => {
    const text = encodeURIComponent(buildNudgeMessage(vendor));
    window.open(`https://wa.me/?text=${text}`, "_blank");
  };

  const biometricValid = useMemo(() => {
    if (!biometricToken || !biometricExpiresAt) {
      return false;
    }
    const expiry = new Date(biometricExpiresAt).getTime();
    return Number.isFinite(expiry) && expiry > Date.now() + 5000;
  }, [biometricToken, biometricExpiresAt]);

  const ensureBiometricToken = async (action) => {
    if (biometricValid && biometricAction === action) {
      return biometricToken;
    }

    setIsMintingBiometric(true);
    try {
      const res = await fetch(`/api/v1/auth/biometric-token?action=${encodeURIComponent(action)}`, {
        method: "POST",
        headers: {
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
        },
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to mint biometric token (${res.status})`);
      }
      setBiometricToken(data.token || "");
      setBiometricAction(data.action || action);
      setBiometricExpiresAt(data.expires_at || "");
      return data.token;
    } finally {
      setIsMintingBiometric(false);
    }
  };

  const askFriday = async () => {
    if (!fridayQuestion.trim()) {
      setFridayError("Enter a question for Friday Insights.");
      return;
    }
    setIsAskingFriday(true);
    setFridayError("");
    setFridayAnswer("");
    try {
      const cleanedMMB = minMonthlyBalance.trim();
      const hasMMB = cleanedMMB.length > 0 && /^\d+(\.\d+)?$/.test(cleanedMMB);
      const res = await fetch("/api/v1/insights/ask-friday", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          question: fridayQuestion,
          model: "llama3.2",
          ...(hasMMB ? { min_credit_balance: cleanedMMB } : {}),
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to ask Friday (${res.status})`);
      }
      setFridayAnswer(data.answer || "No response returned by Friday.");
    } catch (err) {
      setFridayError(err instanceof Error ? err.message : "Failed to ask Friday");
    } finally {
      setIsAskingFriday(false);
    }
  };

  const fetchSummary = async () => {
    setLoading(true);
    setError("");
    try {
      const cleanedMMB = minMonthlyBalance.trim();
      const hasMMB = cleanedMMB.length > 0 && /^\d+(\.\d+)?$/.test(cleanedMMB);
      const mmbQuery = hasMMB ? `&min_credit_balance=${encodeURIComponent(cleanedMMB)}` : "";
      const [summaryRes, reversalRes] = await Promise.all([
        fetch(`/api/v1/insights/friday-summary?${hasMMB ? `min_credit_balance=${encodeURIComponent(cleanedMMB)}` : ""}`),
        fetch(`/api/v1/journal/reversal-summary/recent?hours=72&include_filed=${includeFiled ? "true" : "false"}${mmbQuery}`),
      ]);
      if (!summaryRes.ok) {
        throw new Error(`Unable to fetch Friday summary (${summaryRes.status})`);
      }
      if (!reversalRes.ok) {
        throw new Error(`Unable to fetch reversal summary (${reversalRes.status})`);
      }
      const [summaryData, reversalData] = await Promise.all([summaryRes.json(), reversalRes.json()]);
      setSummary(summaryData);
      setReversalSummary(reversalData);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load insights");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void fetchSummary();
  }, [includeFiled, minMonthlyBalance]);

  const runReversalWizard = async () => {
    if (!window.confirm("Generate Rule 37A reversal journals now?")) {
      return;
    }
    setIsRunningWizard(true);
    setWizardResult("");
    try {
      const cleanedMMB = minMonthlyBalance.trim();
      const hasMMB = cleanedMMB.length > 0 && /^\d+(\.\d+)?$/.test(cleanedMMB);
      const res = await fetch("/api/v1/journal/generate-reversal-37a", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
        },
        body: JSON.stringify({
          ...(hasMMB ? { min_credit_balance: cleanedMMB } : {}),
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Wizard failed (${res.status})`);
      }
      setWizardResult(data.message || "Rule 37A reversal journals generated.");
      await fetchSummary();
    } catch (err) {
      setWizardResult(err instanceof Error ? err.message : "Failed to run wizard");
    } finally {
      setIsRunningWizard(false);
    }
  };

  const downloadAuditTrail = async () => {
    setIsDownloadingAudit(true);
    try {
      const cleanedMMB = minMonthlyBalance.trim();
      const hasMMB = cleanedMMB.length > 0 && /^\d+(\.\d+)?$/.test(cleanedMMB);
      const query = hasMMB
        ? `/api/v1/journal/reversal-summary/recent/export?hours=72&min_credit_balance=${encodeURIComponent(cleanedMMB)}`
        : "/api/v1/journal/reversal-summary/recent/export?hours=72";
      const res = await fetch(query, {
        headers: {
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
        },
      });
      if (!res.ok) {
        throw new Error(`Unable to download audit trail (${res.status})`);
      }

      const entryIdsHeader = res.headers.get("X-Accord-Entry-Ids") || "";
      const exportHashHeader = res.headers.get("X-Accord-Export-Hash") || "";
      const parsedEntryIds = entryIdsHeader
        .split(",")
        .map((value) => Number(value.trim()))
        .filter((value) => Number.isInteger(value) && value > 0);

      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = `Accord_Rule37A_Audit_${new Date().toISOString().slice(0, 10)}.csv`;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.URL.revokeObjectURL(url);

      setLastExportEntryIds(parsedEntryIds);
      setLastExportHash(exportHashHeader);
      setVerifyResult(null);
      setWizardResult(
        parsedEntryIds.length > 0
          ? `Audit trail downloaded. ${parsedEntryIds.length} reversal entries are ready for filing review.`
          : "Audit trail downloaded. No pending reversal entries were found in the selected window."
      );
    } catch (err) {
      setWizardResult(err instanceof Error ? err.message : "Failed to download audit trail");
    } finally {
      setIsDownloadingAudit(false);
    }
  };

  const markBatchAsFiled = async () => {
    if (lastExportEntryIds.length === 0) {
      setWizardResult("Download the latest audit trail before marking a batch as filed.");
      return;
    }
    if (verifyResult?.status !== "MATCHED") {
      setWizardResult("Run a successful fingerprint verification (MATCHED) before marking this batch as filed.");
      return;
    }
    if (!window.confirm("Mark this exported Rule 37A batch as filed and hide it from the 72-hour queue?")) {
      return;
    }

    setIsArchivingBatch(true);
    try {
      const biometric = await ensureBiometricToken("REVERSAL_ARCHIVE");
      const res = await fetch("/api/v1/journal/reversal-summary/archive", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
          "X-Biometric-Token": biometric,
        },
        body: JSON.stringify({
          entry_ids: lastExportEntryIds,
          export_hash: lastExportHash || null,
          note: "Marked as filed from Friday Control Room",
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail?.message || data?.detail || `Unable to mark batch as filed (${res.status})`);
      }

      const archivedCount = Number(data?.archived_count ?? 0);
      const skippedCount = Array.isArray(data?.skipped_already_filed) ? data.skipped_already_filed.length : 0;
      setWizardResult(
        `Batch filed successfully. Archived ${archivedCount} entries${skippedCount > 0 ? `, skipped ${skippedCount} already-filed` : ""}.`
      );
      setLastExportEntryIds([]);
      setLastExportHash("");
      await fetchSummary();
    } catch (err) {
      setWizardResult(err instanceof Error ? err.message : "Failed to mark batch as filed");
    } finally {
      setIsArchivingBatch(false);
    }
  };

  const approveBatch = async () => {
    if (lastExportEntryIds.length === 0) {
      setWizardResult("Download the latest audit trail before recording approvals.");
      return;
    }
    setIsApprovingBatch(true);
    try {
      const biometric = await ensureBiometricToken("REVERSAL_APPROVE");
      const res = await fetch("/api/v1/journal/reversal-summary/approve", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
          "X-Biometric-Token": biometric,
        },
        body: JSON.stringify({
          entry_ids: lastExportEntryIds,
          export_hash: lastExportHash || null,
          note: "Approval from Friday Control Room",
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail?.message || data?.detail || `Unable to approve batch (${res.status})`);
      }
      if (data.waiting_second_admin) {
        setWizardResult("Primary approval recorded. Waiting for 2nd admin on high-value batch.");
      } else {
        setWizardResult("Approval recorded. Batch is now approval-complete.");
      }
      await fetchSummary();
    } catch (err) {
      setWizardResult(err instanceof Error ? err.message : "Failed to approve batch");
    } finally {
      setIsApprovingBatch(false);
    }
  };

  const certifySafeHarbor = async () => {
    const cleanedMMB = minMonthlyBalance.trim();
    if (!(cleanedMMB.length > 0 && /^\d+(\.\d+)?$/.test(cleanedMMB))) {
      setWizardResult("Enter a valid Minimum Monthly Balance before CA certification.");
      return;
    }
    if (adminRole !== "ca") {
      setWizardResult("Only CA role can certify Safe Harbor claims.");
      return;
    }
    setIsCertifyingSafeHarbor(true);
    try {
      const biometric = await ensureBiometricToken("SAFE_HARBOR_CERTIFY");
      const res = await fetch("/api/v1/insights/safe-harbor/certify", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
          "X-Biometric-Token": biometric,
        },
        body: JSON.stringify({
          as_of_date: summary?.as_of_date,
          min_credit_balance: cleanedMMB,
          note: "MMB attested from Friday Control Room",
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to certify Safe Harbor (${res.status})`);
      }
      setWizardResult(`Safe Harbor certified by CA. Attestation #${data.attestation_id}.`);
      await fetchSummary();
    } catch (err) {
      setWizardResult(err instanceof Error ? err.message : "Failed to certify Safe Harbor");
    } finally {
      setIsCertifyingSafeHarbor(false);
    }
  };

  const downloadSafeHarborCertificate = async () => {
    const targetBatchId = lastExportEntryIds[0] || reversalSummary?.entries?.[0]?.entry_id;
    if (!targetBatchId) {
      setWizardResult("Generate or select a reversal batch before downloading the Safe Harbor certificate.");
      return;
    }
    const cleanedMMB = minMonthlyBalance.trim();
    if (!(cleanedMMB.length > 0 && /^\d+(\.\d+)?$/.test(cleanedMMB))) {
      setWizardResult("Enter a valid Minimum Monthly Balance before generating the certificate.");
      return;
    }

    try {
      const biometric = await ensureBiometricToken("SAFE_HARBOR_CERTIFICATE");
      const query = new URLSearchParams({ min_credit_balance: cleanedMMB });
      const res = await fetch(`/api/v1/journal/safe-harbor-certificate/${targetBatchId}?${query.toString()}`, {
        headers: {
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
          "X-Biometric-Token": biometric,
        },
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `Unable to generate certificate (${res.status})`);
      }
      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = `Accord_SafeHarbor_Certificate_${targetBatchId}.pdf`;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.URL.revokeObjectURL(url);
      setWizardResult(`Safe Harbor certificate downloaded for batch #${targetBatchId}.`);
    } catch (err) {
      setWizardResult(err instanceof Error ? err.message : "Failed to download Safe Harbor certificate");
    }
  };

  const verifyAuditExport = async (event) => {
    const selectedFile = event.target.files?.[0];
    if (!selectedFile) {
      return;
    }
    setIsVerifyingExport(true);
    setVerifyResult(null);
    try {
      const formData = new FormData();
      formData.append("file", selectedFile);
      if (lastExportHash) {
        formData.append("expected_export_hash", lastExportHash);
      }
      const res = await fetch("/api/v1/reports/verify-export", {
        method: "POST",
        headers: {
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
        },
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to verify export (${res.status})`);
      }
      setVerifyResult(data);
    } catch (err) {
      setVerifyResult({
        status: "ERROR",
        reason: err instanceof Error ? err.message : "Unable to verify export",
      });
    } finally {
      setIsVerifyingExport(false);
      event.target.value = "";
    }
  };

  const getPaymentAdvice = async () => {
    const cleaned = gstin.trim().toUpperCase();
    if (!cleaned) {
      setAdviceError("Enter a GSTIN to evaluate payment safety.");
      return;
    }
    setAdviceLoading(true);
    setAdviceError("");
    setAdviceData(null);
    try {
      const query = invoiceAmount.trim() ? `?invoice_amount=${encodeURIComponent(invoiceAmount.trim())}` : "";
      const res = await fetch(`/api/v1/insights/vendor/${encodeURIComponent(cleaned)}/payment-advice${query}`);
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to compute advice (${res.status})`);
      }
      setAdviceData(data);
    } catch (err) {
      setAdviceError(err instanceof Error ? err.message : "Unable to compute payment advice");
    } finally {
      setAdviceLoading(false);
    }
  };

  const uploadReceiptPhoto = async (file) => {
    if (!file) {
      return;
    }
    setIsUploadingReceipt(true);
    setReceiptError("");
    setReceiptResult(null);
    try {
      const formData = new FormData();
      formData.append("file", file);
      const res = await fetch("/api/v1/ledger/upload-photo", {
        method: "POST",
        headers: {
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
        },
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to process receipt (${res.status})`);
      }
      setReceiptResult(data);
      setWizardResult(`Receipt posted as journal ${data.reference} (#${data.entry_id}).`);
      await fetchSummary();
    } catch (err) {
      setReceiptError(err instanceof Error ? err.message : "Failed to process receipt");
    } finally {
      setIsUploadingReceipt(false);
    }
  };

  const exportTallyXml = async (entryId) => {
    if (!entryId) {
      return;
    }
    setIsExportingTally(true);
    try {
      const res = await fetch(`/api/v1/ledger/export-tally/${entryId}`, {
        headers: {
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
        },
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `Unable to export Tally XML (${res.status})`);
      }
      const blob = await res.blob();
      const cd = res.headers.get("Content-Disposition") || "";
      const matched = cd.match(/filename=([^;]+)/i);
      const fileName = matched?.[1]?.replace(/"/g, "") || `Accord_Tally_Export_${entryId}.xml`;
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = fileName;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      setReceiptError(err instanceof Error ? err.message : "Failed to export Tally XML");
    } finally {
      setIsExportingTally(false);
    }
  };

  const uploadBatchReceipts = async (files) => {
    if (!files || files.length === 0) {
      setBatchUploadError("No files selected");
      return;
    }

    setIsBatchUploading(true);
    setBatchUploadError("");
    setBatchUploadResults(null);
    
    try {
      const formData = new FormData();
      Array.from(files).forEach((file) => {
        formData.append("files", file);
      });

      const res = await fetch("/api/v1/ledger/upload-photo-batch", {
        method: "POST",
        headers: {
          "X-Role": adminRole,
          "X-Admin-Id": adminId,
        },
        body: formData,
      });

      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to process batch (${res.status})`);
      }

      setBatchUploadResults(data);
      setWizardResult(`Batch processing complete: ${data.total_processed} entries posted, ${data.total_failed} failed.`);
      await fetchSummary();
    } catch (err) {
      setBatchUploadError(err instanceof Error ? err.message : "Failed to process batch");
    } finally {
      setIsBatchUploading(false);
    }
  };

  const topVendors = useMemo(() => summary?.vendor_risk_ranking ?? [], [summary]);

  if (loading) {
    return (
      <div className="min-h-screen bg-slate-950 text-slate-200 flex items-center justify-center gap-3">
        <Loader2 className="w-5 h-5 animate-spin" />
        <span className="text-sm tracking-wide">Loading Accord Intelligence...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-slate-950 text-slate-200 flex items-center justify-center p-6">
        <div className="max-w-lg w-full rounded-2xl border border-red-500/40 bg-red-500/10 p-6">
          <p className="text-red-200 font-semibold">Failed to load insights</p>
          <p className="text-sm text-red-100/90 mt-1">{error}</p>
          <button
            onClick={fetchSummary}
            className="mt-4 px-4 py-2 rounded-lg bg-red-600 hover:bg-red-700 text-white text-sm font-semibold"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  const reversal = summary?.reversal_risks;
  const critical = Boolean(summary?.critical_action_required);
  const shieldMode = adviceData?.payment_advice?.advice === "WITHHOLD_GST_PORTION";
  const reversalEntries = reversalSummary?.entries ?? [];
  const safeHarbor = reversal?.safe_harbor;
  const selectedEntries = reversalEntries.filter((entry) => lastExportEntryIds.includes(entry.entry_id));
  const selectedNeedsSecond = selectedEntries.some((entry) => entry.secondary_approval_required);
  const selectedWaitingSecond = selectedEntries.some((entry) => entry.waiting_second_admin);
  const complianceHealthScore = useMemo(() => {
    const itcRisk = Number(summary?.summary?.total_itc_at_risk ?? 0);
    const pending = Number(summary?.ims_actionables?.PENDING ?? 0);
    const riskPenalty = Math.min(35, itcRisk > 0 ? Math.log10(itcRisk + 10) * 10 : 0);
    const pendingPenalty = Math.min(35, pending * 4.5);
    const hardStopPenalty = critical ? 25 : 0;
    return Math.max(5, 100 - riskPenalty - pendingPenalty - hardStopPenalty);
  }, [summary, critical]);

  const radarSignals = [
    {
      key: "rule37a",
      label: "Rule 37A Pressure",
      value: Math.min(100, Number(reversal?.at_risk_invoice_count ?? 0) * 10 + (critical ? 35 : 0)),
      tone: "from-red-500/30 to-rose-400/10",
    },
    {
      key: "ims",
      label: "IMS Backlog",
      value: Math.min(100, Number(summary?.ims_actionables?.PENDING ?? 0) * 12),
      tone: "from-cyan-500/30 to-sky-400/10",
    },
    {
      key: "vendor",
      label: "Vendor Trust Drag",
      value: Math.min(100, Math.max(0, 100 - Number(topVendors?.[0]?.filing_consistency_score ?? 100))),
      tone: "from-emerald-500/30 to-teal-400/10",
    },
  ];

  return (
    <div className="ai-shell min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-[96rem] px-4 sm:px-6 lg:px-8 py-8">
        <div className="grid grid-cols-1 xl:grid-cols-[320px_minmax(0,1fr)] gap-6">
          <aside className="ai-sidebar ai-reveal rounded-3xl border border-white/10 bg-white/5 backdrop-blur-xl p-4 sm:p-5 space-y-5 h-fit xl:sticky xl:top-6" data-delay="1">
            <div>
              <p className="text-[11px] tracking-[0.2em] text-cyan-300/80 uppercase">Command Rail</p>
              <div className="mt-2 flex items-center gap-2 text-slate-100">
                <PanelLeft className="w-4 h-4 text-cyan-300" />
                <h2 className="font-semibold">Approvals Timeline</h2>
              </div>
              <p className="text-xs text-slate-400 mt-1">Live chain-of-control for selected reversal batches.</p>
            </div>

            <div className="space-y-2">
              {(selectedEntries.length > 0 ? selectedEntries : reversalEntries.slice(0, 3)).map((entry) => (
                <div key={`rail-${entry.entry_id}`} className="rounded-xl border border-slate-700/80 bg-slate-950/70 p-3 space-y-1">
                  <p className="text-xs text-slate-300 font-semibold">{entry.reference}</p>
                  <p className="text-[11px] text-slate-500">Created: {formatDateTime(entry.approval_timeline?.timestamps?.created_at)}</p>
                  <p className="text-[11px] text-slate-500">Exported: {formatDateTime(entry.approval_timeline?.timestamps?.exported_at)}</p>
                  <p className="text-[11px] text-slate-500">Verified: {formatDateTime(entry.approval_timeline?.timestamps?.verified_at)}</p>
                  <p className="text-[11px] text-slate-500">2nd Approval: {formatDateTime(entry.approval_timeline?.timestamps?.approved_at)}</p>
                </div>
              ))}
            </div>

            <div className="rounded-xl border border-slate-700/80 bg-slate-950/70 p-3 space-y-2">
              <div className="flex items-center gap-2">
                <Fingerprint className="w-4 h-4 text-indigo-300" />
                <p className="text-xs font-semibold text-slate-200">Dual-Admin Biometric Gate</p>
              </div>
              <p className="text-[11px] text-slate-400">
                {biometricValid
                  ? `Biometric token active for ${biometricAction} until ${formatDateTime(biometricExpiresAt)}`
                  : "No active biometric token. Approval actions will auto-request one."}
              </p>
              <button
                onClick={() => ensureBiometricToken("REVERSAL_APPROVE")}
                disabled={isMintingBiometric}
                className="w-full inline-flex items-center justify-center gap-2 rounded-lg border border-indigo-700/70 bg-indigo-900/40 hover:bg-indigo-800/50 px-3 py-2 text-xs font-semibold disabled:opacity-60"
              >
                {isMintingBiometric ? "Minting..." : "Mint Biometric Token"}
              </button>
            </div>

            <div className="rounded-xl border border-slate-700/80 bg-slate-950/70 p-3 space-y-2">
              <p className="text-xs font-semibold text-slate-200">Vendor Nudge Queue</p>
              {(topVendors.slice(0, 3)).map((vendor) => (
                <div key={`nudge-${vendor.gstin}`} className="flex items-center justify-between gap-2">
                  <span className="text-[11px] text-slate-400 truncate">{vendor.legal_name || vendor.gstin}</span>
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => openEmailNudge(vendor)}
                      className="text-[10px] px-2 py-1 rounded border border-slate-700 bg-slate-900 hover:bg-slate-800"
                    >
                      Email
                    </button>
                    <button
                      onClick={() => openWhatsAppNudge(vendor)}
                      className="text-[10px] px-2 py-1 rounded border border-slate-700 bg-slate-900 hover:bg-slate-800"
                    >
                      WA
                    </button>
                  </div>
                </div>
              ))}
            </div>

            <div className="rounded-xl border border-cyan-700/50 bg-cyan-900/20 p-3 space-y-2">
              <div className="flex items-center gap-2">
                <BrainCircuit className="w-4 h-4 text-cyan-300" />
                <p className="text-xs font-semibold text-cyan-100">Ask Friday (Local Ollama)</p>
              </div>
              <textarea
                value={fridayQuestion}
                onChange={(e) => setFridayQuestion(e.target.value)}
                className="w-full min-h-24 rounded-lg border border-cyan-800/60 bg-slate-950/80 px-3 py-2 text-xs"
              />
              <button
                onClick={askFriday}
                disabled={isAskingFriday}
                className="w-full rounded-lg bg-cyan-600 hover:bg-cyan-700 px-3 py-2 text-xs font-semibold disabled:opacity-60"
              >
                {isAskingFriday ? "Thinking..." : "Ask Friday"}
              </button>
              {fridayError ? <p className="text-[11px] text-red-300">{fridayError}</p> : null}
              {fridayAnswer ? <p className="text-[11px] text-slate-200 whitespace-pre-wrap">{fridayAnswer}</p> : null}
            </div>

            <div className="rounded-xl border border-emerald-700/60 bg-emerald-950/20 p-3 space-y-2">
              <div className="flex items-center gap-2">
                <Camera className="w-4 h-4 text-emerald-300" />
                <p className="text-xs font-semibold text-emerald-100">Vision Ledger Scan</p>
              </div>
              <p className="text-[11px] text-slate-400">Capture or upload receipt image, auto-post journal, then export Tally XML.</p>
              <label className="w-full inline-flex items-center justify-center gap-2 rounded-lg border border-emerald-700/70 bg-emerald-900/35 hover:bg-emerald-800/50 px-3 py-2 text-xs font-semibold cursor-pointer">
                {isUploadingReceipt ? "Processing..." : "Scan Receipt"}
                <input
                  type="file"
                  accept="image/*"
                  capture="environment"
                  disabled={isUploadingReceipt}
                  className="hidden"
                  onChange={(event) => {
                    const picked = event.target.files?.[0];
                    void uploadReceiptPhoto(picked);
                    event.target.value = "";
                  }}
                />
              </label>
              <label className="w-full inline-flex items-center justify-center gap-2 rounded-lg border border-cyan-700/70 bg-cyan-900/35 hover:bg-cyan-800/50 px-3 py-2 text-xs font-semibold cursor-pointer">
                {isBatchUploading ? "Processing Batch..." : "Batch Upload (10-20 images)"}
                <input
                  type="file"
                  accept="image/*"
                  multiple
                  disabled={isBatchUploading}
                  className="hidden"
                  onChange={(event) => {
                    void uploadBatchReceipts(event.target.files);
                    event.target.value = "";
                  }}
                />
              </label>
              {batchUploadResults ? (
                <div className="rounded-lg border border-cyan-700/50 bg-slate-950/70 p-2 space-y-1">
                  <p className="text-[11px] text-cyan-200 font-semibold">
                    Batch: {batchUploadResults.total_processed} ✓ | {batchUploadResults.total_failed} ✗
                  </p>
                  <p className="text-[11px] text-slate-400">
                    {batchUploadResults.processor}
                  </p>
                  {batchUploadResults.results && batchUploadResults.results.length > 0 && (
                    <div className="mt-1 space-y-1 max-h-32 overflow-y-auto">
                      {batchUploadResults.results.slice(0, 3).map((r) => (
                        <p key={r.entry_id} className="text-[10px] text-emerald-300">
                          {r.reference} | INR {r.amount}
                        </p>
                      ))}
                      {batchUploadResults.results.length > 3 && (
                        <p className="text-[10px] text-slate-400">+{batchUploadResults.results.length - 3} more entries</p>
                      )}
                    </div>
                  )}
                </div>
              ) : null}
              {receiptResult && !batchUploadResults ? (
                <div className="rounded-lg border border-emerald-700/50 bg-slate-950/70 p-2 space-y-1">
                  <p className="text-[11px] text-emerald-200 font-semibold">
                    {receiptResult?.reference} | INR {receiptResult?.extracted?.total_amount}
                  </p>
                  <p className="text-[11px] text-slate-400 truncate">
                    {receiptResult?.extracted?.vendor || "Vendor unknown"}
                    {receiptResult?.extracted?.gstin ? ` | ${receiptResult.extracted.gstin}` : ""}
                  </p>
                  <button
                    onClick={() => exportTallyXml(receiptResult?.entry_id)}
                    disabled={isExportingTally}
                    className="w-full mt-1 inline-flex items-center justify-center gap-2 rounded-lg border border-cyan-700/70 bg-cyan-900/35 hover:bg-cyan-800/50 px-3 py-2 text-xs font-semibold disabled:opacity-60"
                  >
                    {isExportingTally ? "Exporting..." : "Export Tally XML"}
                    <FileCode2 className="w-3.5 h-3.5" />
                  </button>
                </div>
              ) : null}
              {receiptError ? <p className="text-[11px] text-red-300">{receiptError}</p> : null}
              {batchUploadError ? <p className="text-[11px] text-red-300">{batchUploadError}</p> : null}
            </div>
          </aside>

          <div className="space-y-6 ai-reveal" data-delay="2">
        <header className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <p className="text-xs tracking-[0.2em] text-cyan-300/80 uppercase">Friday Control Room</p>
            <h1 className="text-2xl sm:text-3xl font-bold tracking-tight">Cash-Flow Protection Command Center</h1>
            <p className="text-sm text-slate-400 mt-1">As of {summary?.as_of_date}</p>
          </div>
          <div className="rounded-xl border border-slate-800 bg-slate-900/70 px-4 py-3">
            <p className="text-xs text-slate-400">Potential Interest Savings</p>
            <p className="text-xl font-mono font-bold text-emerald-300">
              {formatINR(summary?.summary?.total_potential_interest_savings)}
            </p>
          </div>
        </header>

        <section className="grid grid-cols-1 xl:grid-cols-[320px_minmax(0,1fr)] gap-6 ai-panel ai-reveal" data-delay="2">
          <div className="rounded-2xl border border-cyan-500/25 bg-slate-900/50 p-5 flex flex-col items-center justify-center gap-3">
            <p className="text-xs uppercase tracking-[0.2em] text-cyan-300">Compliance Health Score</p>
            <ComplianceGauge score={complianceHealthScore} />
            <p className="text-xs text-slate-400 text-center">
              Composite signal from Rule 37A, IMS backlog, and vendor filing behavior.
            </p>
          </div>
          <div className="rounded-2xl border border-slate-700/80 bg-slate-900/55 p-4 sm:p-5">
            <div className="flex items-center gap-2 mb-3">
              <Radar className="w-4 h-4 text-cyan-300" />
              <h3 className="text-sm font-semibold tracking-wide uppercase text-slate-200">Animated Compliance Radar</h3>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              {radarSignals.map((signal, idx) => (
                <motion.div
                  key={signal.key}
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: idx * 0.08, duration: 0.35 }}
                  className={`rounded-xl border border-slate-700/70 bg-gradient-to-br ${signal.tone} p-3`}
                  style={{ willChange: "transform", transform: "translateZ(0)" }}
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-xs text-slate-200">{signal.label}</span>
                    <span className="text-xs text-slate-300 font-mono">{Math.round(signal.value)}%</span>
                  </div>
                  <div className="h-2 rounded-full bg-slate-800 overflow-hidden">
                    <motion.div
                      className="h-full bg-cyan-300/90"
                      initial={{ width: 0 }}
                      animate={{ width: `${Math.max(4, signal.value)}%` }}
                      transition={{ duration: 0.9, ease: "easeOut" }}
                    />
                  </div>
                </motion.div>
              ))}
            </div>
          </div>
        </section>

        <section
          data-delay="2"
          className={`rounded-2xl border p-5 sm:p-6 ${
            critical ? "border-red-500/50 bg-red-500/10" : "border-amber-500/40 bg-amber-500/10"
          } ai-panel ai-reveal`}
        >
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div className="flex items-start gap-3">
              {critical ? <ShieldAlert className="w-7 h-7 text-red-300 mt-0.5" /> : <AlertTriangle className="w-7 h-7 text-amber-300 mt-0.5" />}
              <div>
                <h2 className="font-bold text-lg">
                  {critical ? "Critical Action Required: Rule 37A" : "Rule 37A Watchlist"}
                </h2>
                <p className="text-sm text-slate-200/90 mt-1">
                  Immediate reversal risk: <span className="font-semibold">{formatINR(reversal?.immediate_reversal_risk)}</span>
                </p>
                <p className="text-sm text-slate-300/90">
                  At-risk invoices: <span className="font-semibold">{reversal?.at_risk_invoice_count ?? 0}</span>
                </p>
                <p className="text-sm text-slate-300/90">
                  Safe Harbor: <span className="font-semibold">{safeHarbor?.status || "STANDARD_INTEREST_APPLIES"}</span>
                </p>
                <p className="text-sm text-slate-300/90">
                  Liability Offset: <span className="font-semibold">{formatINR(safeHarbor?.liability_offset || 0)}</span>
                </p>
              </div>
            </div>
            <button
              onClick={runReversalWizard}
              disabled={isRunningWizard}
              className={`inline-flex items-center justify-center gap-2 px-5 py-2.5 rounded-lg font-semibold text-white disabled:opacity-60 disabled:cursor-not-allowed ${
                critical
                  ? "bg-red-600 hover:bg-red-700 ring-2 ring-red-300/70 shadow-lg shadow-red-900/50 animate-pulse"
                  : "bg-red-600 hover:bg-red-700"
              }`}
            >
              {isRunningWizard ? "Processing..." : "Launch Reversal Wizard"}
              <Zap className="w-4 h-4" />
            </button>
            <button
              onClick={downloadAuditTrail}
              disabled={isDownloadingAudit}
              className="inline-flex items-center justify-center gap-2 px-5 py-2.5 rounded-lg font-semibold text-slate-100 border border-slate-600 bg-slate-900 hover:bg-slate-800 disabled:opacity-60 disabled:cursor-not-allowed"
            >
              {isDownloadingAudit ? "Downloading..." : "Download Audit Trail (CSV)"}
            </button>
            <button
              onClick={approveBatch}
              disabled={isApprovingBatch || lastExportEntryIds.length === 0}
              className="inline-flex items-center justify-center gap-2 px-5 py-2.5 rounded-lg font-semibold text-cyan-100 border border-cyan-700/70 bg-cyan-900/40 hover:bg-cyan-800/50 disabled:opacity-60 disabled:cursor-not-allowed"
            >
              {isApprovingBatch ? "Approving..." : "Secondary Approval"}
            </button>
            <button
              onClick={markBatchAsFiled}
              disabled={
                isArchivingBatch ||
                lastExportEntryIds.length === 0 ||
                verifyResult?.status !== "MATCHED" ||
                (selectedNeedsSecond && selectedWaitingSecond)
              }
              className="inline-flex items-center justify-center gap-2 px-5 py-2.5 rounded-lg font-semibold text-emerald-100 border border-emerald-700/70 bg-emerald-900/40 hover:bg-emerald-800/50 disabled:opacity-60 disabled:cursor-not-allowed"
            >
              {isArchivingBatch ? "Marking..." : "Mark as Filed"}
            </button>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mt-3">
            <input
              value={minMonthlyBalance}
              onChange={(e) => setMinMonthlyBalance(e.target.value)}
              placeholder="Minimum Monthly Credit Balance"
              inputMode="decimal"
              className="rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
            />
            <input
              value={adminId}
              onChange={(e) => setAdminId(e.target.value)}
              placeholder="Admin ID"
              className="rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
            />
            <select
              value={adminRole}
              onChange={(e) => setAdminRole(e.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
            >
              <option value="admin">admin</option>
              <option value="ca">ca</option>
            </select>
            <div className="text-xs text-slate-400 rounded-lg border border-slate-800 bg-slate-950 px-3 py-2 flex items-center">
              {selectedNeedsSecond
                ? selectedWaitingSecond
                  ? "High-value batch: waiting for 2nd admin"
                  : "High-value batch: dual approval complete"
                : "Standard batch: single approval sufficient"}
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-3 mt-2">
            <button
              onClick={certifySafeHarbor}
              disabled={isCertifyingSafeHarbor || adminRole !== "ca"}
              className="inline-flex items-center justify-center gap-2 px-4 py-2 rounded-lg font-semibold text-indigo-100 border border-indigo-700/70 bg-indigo-900/40 hover:bg-indigo-800/50 disabled:opacity-60 disabled:cursor-not-allowed"
            >
              {isCertifyingSafeHarbor ? "Certifying..." : "CA Certify MMB"}
            </button>
            <button
              onClick={downloadSafeHarborCertificate}
              className="inline-flex items-center justify-center gap-2 px-4 py-2 rounded-lg font-semibold text-emerald-100 border border-emerald-700/70 bg-emerald-900/40 hover:bg-emerald-800/50"
            >
              Download Safe Harbor PDF
            </button>
            <span className="text-xs text-slate-400">
              Interest Outcome: {safeHarbor?.interest_outcome || "INR_18PCT_STANDARD"}
            </span>
            <span className="text-xs text-slate-500">
              Biometric: {biometricValid ? `ACTIVE (${biometricAction})` : "REQUIRED"}
            </span>
          </div>
          {wizardResult ? <p className="mt-3 text-sm text-slate-100">{wizardResult}</p> : null}
        </section>

        <section className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5 space-y-4 ai-panel ai-reveal" data-delay="3">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <h3 className="text-lg font-semibold">Reversal Batch Queue</h3>
              <p className="text-sm text-slate-400">Switch between pending actions and filed historical batches.</p>
            </div>
            <div className="inline-flex rounded-lg border border-slate-700 bg-slate-950 p-1">
              <button
                onClick={() => setIncludeFiled(false)}
                className={`px-3 py-1.5 rounded-md text-xs font-semibold ${
                  includeFiled ? "text-slate-400" : "bg-red-600/30 text-red-100"
                }`}
              >
                Action Required
              </button>
              <button
                onClick={() => setIncludeFiled(true)}
                className={`px-3 py-1.5 rounded-md text-xs font-semibold ${
                  includeFiled ? "bg-emerald-600/30 text-emerald-100" : "text-slate-400"
                }`}
              >
                Historical / Filed
              </button>
            </div>
          </div>

          <div className="text-xs text-slate-400 flex flex-wrap gap-4">
            <span>Window: {reversalSummary?.window_hours ?? 72}h</span>
            <span>Count: {reversalSummary?.count ?? 0}</span>
            <span>Total: {formatINR(reversalSummary?.total_reversal_amount ?? 0)}</span>
          </div>

          <div className="space-y-2">
            {reversalEntries.length === 0 ? (
              <p className="text-sm text-slate-400">No reversal batches found for this view.</p>
            ) : (
              reversalEntries.map((entry) => (
                <div
                  key={entry.entry_id}
                  className={`rounded-xl border px-4 py-3 flex flex-col gap-2 md:flex-row md:items-center md:justify-between ${
                    entry.is_filed
                      ? "border-slate-700 bg-slate-950/40 opacity-60 grayscale"
                      : "border-slate-800 bg-slate-950/80"
                  }`}
                >
                  <div className="space-y-2">
                    <p className="font-medium">{entry.reference}</p>
                    <p className="text-xs text-slate-500">Created: {formatDateTime(entry.reversal_created_at)}</p>
                    <p className="text-xs text-slate-500">Amount: {formatINR(entry.reversal_amount)}</p>
                    <div className="rounded-lg border border-slate-800 bg-slate-950/70 px-3 py-2">
                      <p className="text-[11px] text-slate-400 mb-1">Chain of Control</p>
                      <p className="text-[11px] text-slate-300">
                        Created: {entry.approval_timeline?.created_by ? `Admin-${entry.approval_timeline.created_by}` : "-"} @ {formatDateTime(entry.approval_timeline?.timestamps?.created_at)}
                      </p>
                      <p className="text-[11px] text-slate-300">
                        Exported: {entry.approval_timeline?.exported_by ? `Admin-${entry.approval_timeline.exported_by}` : "-"} @ {formatDateTime(entry.approval_timeline?.timestamps?.exported_at)}
                      </p>
                      <p className="text-[11px] text-slate-300">
                        Verified: {entry.approval_timeline?.verified_by ? `Admin-${entry.approval_timeline.verified_by}` : "-"} @ {formatDateTime(entry.approval_timeline?.timestamps?.verified_at)}
                      </p>
                      <p className="text-[11px] text-slate-300">
                        2nd Approval: {entry.approval_timeline?.approved_by_2 ? `Admin-${entry.approval_timeline.approved_by_2}` : "-"} @ {formatDateTime(entry.approval_timeline?.timestamps?.approved_at)}
                      </p>
                    </div>
                  </div>
                  <div className="text-right">
                    <span
                      className={`inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-semibold ${
                        entry.is_filed
                          ? "border-emerald-500/40 bg-emerald-500/15 text-emerald-200"
                          : "border-amber-500/40 bg-amber-500/15 text-amber-200"
                      }`}
                    >
                      {entry.is_filed ? "FILED" : "PENDING_REVIEW"}
                    </span>
                    {entry.waiting_second_admin ? (
                      <p className="text-[11px] text-amber-300 mt-1">Waiting for 2nd Admin</p>
                    ) : null}
                    {entry.filed_at ? <p className="text-xs text-slate-500 mt-1">Filed: {formatDateTime(entry.filed_at)}</p> : null}
                  </div>
                </div>
              ))
            )}
          </div>
        </section>

        <section className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5 space-y-3 ai-panel ai-reveal" data-delay="3">
          <h3 className="text-lg font-semibold">Export Fingerprint Verifier</h3>
          <p className="text-sm text-slate-400">
            Upload a CSV export from Accord to validate integrity against stored payload fingerprint and export hash.
          </p>
          <div className="flex flex-col sm:flex-row sm:items-center gap-3">
            <label className="inline-flex items-center justify-center px-4 py-2 rounded-lg border border-slate-700 bg-slate-950 hover:bg-slate-900 text-sm font-semibold cursor-pointer">
              {isVerifyingExport ? "Verifying..." : "Upload CSV to Verify"}
              <input
                type="file"
                accept=".csv,text/csv"
                onChange={verifyAuditExport}
                disabled={isVerifyingExport}
                className="hidden"
              />
            </label>
            {lastExportHash ? <span className="text-xs text-slate-500">Expected Hash: {lastExportHash.slice(0, 16)}...</span> : null}
          </div>
          {verifyResult ? (
            <div
              className={`rounded-lg border p-3 text-sm ${
                verifyResult.status === "MATCHED"
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-100"
                  : "border-red-500/40 bg-red-500/10 text-red-100"
              }`}
            >
              <p className="font-semibold">Verification: {verifyResult.status}</p>
              {verifyResult.reason ? <p className="mt-1">{verifyResult.reason}</p> : null}
              {verifyResult.export?.export_id ? (
                <p className="mt-1 text-xs opacity-90">
                  Export #{verifyResult.export.export_id} | Generated {formatDateTime(verifyResult.export.generated_at)}
                </p>
              ) : null}
            </div>
          ) : null}
        </section>

        <section className="grid grid-cols-1 xl:grid-cols-5 gap-6 ai-reveal" data-delay="3">
          <div className="xl:col-span-2 rounded-2xl border border-slate-800 bg-slate-900/60 p-5 space-y-4">
            <h3 className="text-lg font-semibold">Withholding Advisor</h3>
            <p className="text-sm text-slate-400">
              Enter vendor GSTIN and optional invoice amount to calculate safe payment and withholding buffer.
            </p>
            <div className="space-y-3">
              <input
                value={gstin}
                onChange={(e) => setGstin(e.target.value)}
                placeholder="Vendor GSTIN"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-cyan-500/50"
              />
              <input
                value={invoiceAmount}
                onChange={(e) => setInvoiceAmount(e.target.value)}
                placeholder="Invoice amount (optional)"
                inputMode="decimal"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-cyan-500/50"
              />
              <input
                value={minMonthlyBalance}
                onChange={(e) => setMinMonthlyBalance(e.target.value)}
                placeholder="Minimum Monthly Credit Balance (MMB)"
                inputMode="decimal"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-cyan-500/50"
              />
              <button
                onClick={getPaymentAdvice}
                disabled={adviceLoading}
                className="w-full inline-flex items-center justify-center gap-2 rounded-lg bg-cyan-600 hover:bg-cyan-700 px-4 py-2.5 text-sm font-semibold disabled:opacity-60"
              >
                {adviceLoading ? "Calculating..." : "Get Payment Advice"}
                <ArrowRight className="w-4 h-4" />
              </button>
            </div>
            {adviceError ? <p className="text-sm text-red-300">{adviceError}</p> : null}
            {adviceData ? (
              <div className="rounded-xl border border-slate-700 bg-slate-950 p-4 space-y-2">
                <p className="text-sm text-slate-300">{adviceData.legal_name || adviceData.gstin}</p>
                <p className="text-xs text-slate-500">Trust Score: {adviceData.trust_score}</p>
                <p className="text-sm">
                  ITC at Risk:{" "}
                  <span className={`font-semibold ${shieldMode ? "text-red-300" : "text-slate-200"}`}>
                    {formatINR(adviceData.total_itc_at_risk)}
                  </span>
                </p>
                <p className="text-sm">Suggested Withholding: <span className="font-semibold text-amber-300">{formatINR(adviceData.suggested_withholding)}</span></p>
                {adviceData.safe_payment_amount ? (
                  <p className="text-sm">
                    Safe Payment:{" "}
                    <span className={`font-semibold ${shieldMode ? "text-emerald-300 text-lg" : "text-emerald-300"}`}>
                      {formatINR(adviceData.safe_payment_amount)}
                    </span>
                  </p>
                ) : null}
              </div>
            ) : null}
          </div>

          <div className="xl:col-span-3 rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold">Vendor Risk Ranking</h3>
              <span className="text-xs text-slate-400">Payment Shield active</span>
            </div>
            <div className="space-y-2">
              {topVendors.length === 0 ? (
                <p className="text-sm text-slate-400">No risky vendors detected.</p>
              ) : (
                topVendors.map((vendor) => {
                  const advice = vendor?.payment_advice?.advice || "STANDARD_PAYMENT";
                  const badge = adviceBadge(advice);
                  return (
                    <div
                      key={vendor.gstin}
                      className="rounded-xl border border-slate-800 bg-slate-950/80 px-4 py-3 flex items-center justify-between gap-3"
                    >
                      <div>
                        <p className="font-medium">{vendor.legal_name || "Unknown Vendor"}</p>
                        <p className="text-xs text-slate-500">{vendor.gstin}</p>
                        <div className="mt-2 flex items-center gap-2">
                          <button
                            onClick={() => openEmailNudge(vendor)}
                            className="text-[11px] px-2.5 py-1 rounded-md border border-slate-700 bg-slate-900 hover:bg-slate-800 text-slate-200"
                          >
                            Email Nudge
                          </button>
                          <button
                            onClick={() => openWhatsAppNudge(vendor)}
                            className="text-[11px] px-2.5 py-1 rounded-md border border-slate-700 bg-slate-900 hover:bg-slate-800 text-slate-200"
                          >
                            WhatsApp Nudge
                          </button>
                        </div>
                      </div>
                      <div className="text-right">
                        <div className={`inline-flex items-center gap-1 border rounded-full px-2.5 py-1 text-[11px] font-semibold ${badge.className}`}>
                          {badge.icon}
                          {badge.label}
                        </div>
                        <p className="text-sm mt-1">Score: <span className="font-mono font-semibold">{vendor.filing_consistency_score}</span></p>
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        </section>
          </div>
        </div>
      </div>

      <div className="fixed right-5 bottom-5 z-50">
        <motion.button
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.96 }}
          onClick={() => setIsFridayBubbleOpen((prev) => !prev)}
          className="rounded-full border border-cyan-400/40 bg-slate-900/90 text-cyan-100 shadow-lg shadow-cyan-900/40 p-4"
          style={{ willChange: "transform", transform: "translateZ(0)" }}
        >
          <Bot className="w-5 h-5" />
        </motion.button>
      </div>

      <AnimatePresence>
        {isFridayBubbleOpen ? (
          <motion.div
            initial={{ opacity: 0, y: 24, scale: 0.95 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 16, scale: 0.95 }}
            transition={{ duration: 0.2 }}
            className="fixed right-5 bottom-24 z-50 w-[92vw] max-w-md rounded-2xl border border-cyan-500/35 bg-slate-950/95 backdrop-blur-xl p-4"
            style={{ willChange: "transform", transform: "translateZ(0)" }}
          >
            <div className="flex items-center gap-2 mb-2">
              <Sparkles className="w-4 h-4 text-cyan-300" />
              <p className="text-sm font-semibold text-cyan-100">Friday Terminal</p>
            </div>
            <p className="text-[11px] text-slate-400 mb-2">Matrix-style local CA reasoning stream (Ollama on-device).</p>
            <textarea
              value={fridayQuestion}
              onChange={(e) => setFridayQuestion(e.target.value)}
              className="w-full min-h-20 rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-xs"
            />
            <button
              onClick={askFriday}
              disabled={isAskingFriday}
              className="mt-2 w-full rounded-lg bg-cyan-600 hover:bg-cyan-700 px-3 py-2 text-xs font-semibold disabled:opacity-60"
            >
              {isAskingFriday ? "Streaming..." : "Run Friday Analysis"}
            </button>
            <pre className="mt-3 rounded-lg border border-slate-800 bg-black/55 p-3 text-[11px] leading-relaxed text-emerald-300 whitespace-pre-wrap font-mono max-h-48 overflow-auto">
              {fridayError ? `ERROR> ${fridayError}` : fridayStreamText || "READY> Awaiting prompt..."}
            </pre>
          </motion.div>
        ) : null}
      </AnimatePresence>
    </div>
  );
}
