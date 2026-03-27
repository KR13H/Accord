import React, { useEffect, useMemo, useState } from "react";
import { FlatList, StyleSheet, Text, TouchableOpacity, View } from "react-native";
import { useQuery } from "@tanstack/react-query";
import mobileApi, { subscribeOfflineBanner } from "../api/axios";

type Language = "en" | "hi" | "pa" | "ur";

type ReportItem = {
  id: string;
  key: string;
};

const REPORTS: ReportItem[] = [
  { id: "cash_flow", key: "cash_flow" },
  { id: "rera_wip", key: "rera_wip" },
  { id: "rent_roll", key: "rent_roll" },
  { id: "gst_summary", key: "gst_summary" },
  { id: "itc_recon", key: "itc_recon" },
  { id: "spv_pl", key: "spv_pl" },
  { id: "receivables", key: "receivables" },
  { id: "payables", key: "payables" },
  { id: "vendor_risk", key: "vendor_risk" },
  { id: "sales_velocity", key: "sales_velocity" },
  { id: "unit_inventory", key: "unit_inventory" },
  { id: "project_cost", key: "project_cost" },
  { id: "late_fees", key: "late_fees" },
  { id: "allocation_audit", key: "allocation_audit" },
  { id: "journal_exceptions", key: "journal_exceptions" },
  { id: "tax_outstanding", key: "tax_outstanding" },
  { id: "compliance_calendar", key: "compliance_calendar" },
  { id: "tenant_aging", key: "tenant_aging" },
  { id: "invoice_ai_accuracy", key: "invoice_ai_accuracy" },
  { id: "weekly_exec", key: "weekly_exec" },
];

const TITLES: Record<Language, Record<string, string>> = {
  en: {
    cash_flow: "Cash Flow",
    rera_wip: "RERA WIP",
    rent_roll: "Rent Roll",
    gst_summary: "GST Summary",
    itc_recon: "ITC Reconciliation",
    spv_pl: "SPV P&L",
    receivables: "Receivables",
    payables: "Payables",
    vendor_risk: "Vendor Risk",
    sales_velocity: "Sales Velocity",
    unit_inventory: "Unit Inventory",
    project_cost: "Project Cost",
    late_fees: "Late Fee Summary",
    allocation_audit: "Allocation Audit",
    journal_exceptions: "Journal Exceptions",
    tax_outstanding: "Tax Outstanding",
    compliance_calendar: "Compliance Calendar",
    tenant_aging: "Tenant Aging",
    invoice_ai_accuracy: "AI Invoice Accuracy",
    weekly_exec: "Weekly Executive Brief",
  },
  hi: {
    cash_flow: "कैश फ्लो",
    rera_wip: "RERA WIP",
    rent_roll: "किराया रोल",
    gst_summary: "जीएसटी सारांश",
    itc_recon: "आईटीसी मिलान",
    spv_pl: "एसपीवी लाभ-हानि",
    receivables: "प्राप्य",
    payables: "देय",
    vendor_risk: "विक्रेता जोखिम",
    sales_velocity: "बिक्री गति",
    unit_inventory: "यूनिट इन्वेंटरी",
    project_cost: "प्रोजेक्ट लागत",
    late_fees: "लेट फीस सारांश",
    allocation_audit: "आवंटन ऑडिट",
    journal_exceptions: "जर्नल अपवाद",
    tax_outstanding: "कर बकाया",
    compliance_calendar: "अनुपालन कैलेंडर",
    tenant_aging: "किरायेदार एजिंग",
    invoice_ai_accuracy: "एआई इनवॉइस सटीकता",
    weekly_exec: "साप्ताहिक कार्यकारी सार",
  },
  pa: {
    cash_flow: "ਕੈਸ਼ ਫਲੋ",
    rera_wip: "RERA WIP",
    rent_roll: "ਰੈਂਟ ਰੋਲ",
    gst_summary: "GST ਸੰਖੇਪ",
    itc_recon: "ITC ਮਿਲਾਣ",
    spv_pl: "SPV ਮੁਨਾਫਾ-ਨੁਕਸਾਨ",
    receivables: "ਰਸੀਦਯੋਗ",
    payables: "ਦੇਣਯੋਗ",
    vendor_risk: "ਵਿਕਰੇਤਾ ਜੋਖਮ",
    sales_velocity: "ਸੇਲਜ਼ ਵੇਗ",
    unit_inventory: "ਯੂਨਿਟ ਇਨਵੈਂਟਰੀ",
    project_cost: "ਪ੍ਰੋਜੈਕਟ ਲਾਗਤ",
    late_fees: "ਦੇਰੀ ਫੀਸ ਸੰਖੇਪ",
    allocation_audit: "ਅਲੋਕੇਸ਼ਨ ਆਡਿਟ",
    journal_exceptions: "ਜਰਨਲ ਅਪਵਾਦ",
    tax_outstanding: "ਟੈਕਸ ਬਕਾਇਆ",
    compliance_calendar: "ਕੰਪਲਾਇੰਸ ਕੈਲੰਡਰ",
    tenant_aging: "ਕਿਰਾਏਦਾਰ ਏਜਿੰਗ",
    invoice_ai_accuracy: "ਏਆਈ ਇਨਵੌਇਸ ਸ਼ੁੱਧਤਾ",
    weekly_exec: "ਹਫਤਾਵਾਰੀ ਏਗਜ਼ੈਕਟਿਵ ਬ੍ਰੀਫ",
  },
  ur: {
    cash_flow: "کیش فلو",
    rera_wip: "RERA WIP",
    rent_roll: "رینٹ رول",
    gst_summary: "جی ایس ٹی خلاصہ",
    itc_recon: "آئی ٹی سی مفاہمت",
    spv_pl: "ایس پی وی منافع و نقصان",
    receivables: "وصولیاں",
    payables: "ادائگیاں",
    vendor_risk: "وینڈر رسک",
    sales_velocity: "سیلز رفتار",
    unit_inventory: "یونٹ انوینٹری",
    project_cost: "پروجیکٹ لاگت",
    late_fees: "تاخیر فیس خلاصہ",
    allocation_audit: "مختص آڈٹ",
    journal_exceptions: "جرنل استثنائات",
    tax_outstanding: "ٹیکس بقایا",
    compliance_calendar: "کمپلائنس کیلنڈر",
    tenant_aging: "کرایہ دار ایجنگ",
    invoice_ai_accuracy: "اے آئی انوائس درستگی",
    weekly_exec: "ہفتہ وار ایگزیکٹو بریف",
  },
};

type Props = {
  language: Language;
  onSelectReport: (reportId: string) => void;
};

export default function ReportsScreen({ language, onSelectReport }: Props) {
  const labels = useMemo(() => TITLES[language] || TITLES.en, [language]);
  const [offlineBanner, setOfflineBanner] = useState<{ visible: boolean; message: string }>({
    visible: false,
    message: "",
  });

  useEffect(() => {
    return subscribeOfflineBanner((state) => {
      setOfflineBanner(state);
    });
  }, []);

  const reportSnapshot = useQuery({
    queryKey: ["mobile-compliance-reports"],
    queryFn: async () => {
      const response = await mobileApi.get("/reports/ca/monthly", {
        params: { limit: 200 },
        offlineCacheKey: "mobile_compliance_reports_v1",
        offlineCacheTtlMs: 24 * 60 * 60 * 1000,
      });

      return {
        fromCache: String(response.headers?.["x-offline-cache"] || "") === "hit",
        period: String(response.data?.period || "current period"),
      };
    },
    staleTime: 30 * 1000,
  });

  const subtitleSuffix = reportSnapshot.data?.fromCache ? "(cached)" : `(period: ${reportSnapshot.data?.period || "current"})`;

  return (
    <View style={styles.container}>
      {offlineBanner.visible ? (
        <View style={styles.offlineBanner}>
          <Text style={styles.offlineBannerText}>{offlineBanner.message}</Text>
        </View>
      ) : null}

      {reportSnapshot.isLoading ? <Text style={styles.stateText}>Loading compliance reports...</Text> : null}
      {reportSnapshot.isError ? <Text style={styles.errorText}>Unable to load reports. Please retry when online.</Text> : null}

      <FlatList
        data={REPORTS}
        keyExtractor={(item) => item.id}
        renderItem={({ item }) => (
          <TouchableOpacity style={styles.card} onPress={() => onSelectReport(item.id)}>
            <Text style={styles.title}>{labels[item.key] || item.id}</Text>
            <Text style={styles.subtitle}>{item.id} {subtitleSuffix}</Text>
          </TouchableOpacity>
        )}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#020817",
    paddingHorizontal: 14,
    paddingTop: 12,
  },
  card: {
    borderWidth: 1,
    borderColor: "rgba(34,211,238,0.25)",
    backgroundColor: "rgba(15,23,42,0.92)",
    borderRadius: 12,
    paddingVertical: 12,
    paddingHorizontal: 14,
    marginBottom: 10,
  },
  title: {
    color: "#e2f8ff",
    fontWeight: "700",
    fontSize: 14,
  },
  subtitle: {
    color: "#8fa7b5",
    marginTop: 4,
    fontSize: 11,
  },
  offlineBanner: {
    borderWidth: 1,
    borderColor: "#f59e0b",
    backgroundColor: "rgba(245, 158, 11, 0.16)",
    borderRadius: 10,
    paddingVertical: 8,
    paddingHorizontal: 10,
    marginBottom: 10,
  },
  offlineBannerText: {
    color: "#fef3c7",
    fontSize: 12,
    fontWeight: "700",
  },
  stateText: {
    color: "#8fa7b5",
    fontSize: 12,
    marginBottom: 10,
  },
  errorText: {
    color: "#fca5a5",
    fontSize: 12,
    marginBottom: 10,
  },
});
