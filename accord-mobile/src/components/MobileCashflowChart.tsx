import React, { useMemo } from "react";
import { StyleSheet, Text, View, useWindowDimensions } from "react-native";
import { LineChart } from "react-native-chart-kit";
import { useQuery } from "@tanstack/react-query";

const API_BASE = "http://127.0.0.1:8000";

type SummaryResponse = {
  total_bookings?: number;
  funds_awaiting_rera_allocation?: string;
  pending_rent_due?: string;
  inflows_series?: number[];
  outflows_series?: number[];
};

export default function MobileCashflowChart() {
  const { width } = useWindowDimensions();
  const chartWidth = Math.max(280, width - 40);

  const summaryQuery = useQuery<SummaryResponse>({
    queryKey: ["mobile-dashboard-summary"],
    queryFn: async () => {
      const response = await fetch(`${API_BASE}/api/v1/dashboard/summary`, {
        headers: {
          "X-Role": "admin",
          "X-Admin-Id": "1001",
        },
      });
      if (!response.ok) {
        throw new Error(`Summary fetch failed (${response.status})`);
      }
      return response.json();
    },
    staleTime: 30_000,
  });

  const chartData = useMemo(() => {
    const inflows = summaryQuery.data?.inflows_series ?? [12, 14, 16, 18, 20, 19, 22];
    const outflows = summaryQuery.data?.outflows_series ?? [9, 10, 11, 13, 12, 14, 13];

    return {
      labels: ["M", "T", "W", "T", "F", "S", "S"],
      datasets: [
        {
          data: inflows,
          color: () => "rgba(34, 197, 94, 0.95)",
          strokeWidth: 2,
        },
        {
          data: outflows,
          color: () => "rgba(248, 113, 113, 0.95)",
          strokeWidth: 2,
        },
      ],
      legend: ["Inflows", "Outflows"],
    };
  }, [summaryQuery.data]);

  if (summaryQuery.isLoading) {
    return <Text style={styles.stateText}>Loading cashflow chart...</Text>;
  }

  if (summaryQuery.isError) {
    return <Text style={styles.errorText}>Unable to load mobile cashflow trend.</Text>;
  }

  return (
    <View style={styles.wrapper}>
      <Text style={styles.title}>Cashflow Trend</Text>
      <Text style={styles.subtitle}>Inflows vs Outflows (7-day smooth projection)</Text>
      <LineChart
        data={chartData}
        width={chartWidth}
        height={220}
        withDots={false}
        withShadow={false}
        withInnerLines
        withOuterLines
        bezier
        chartConfig={{
          backgroundGradientFrom: "#0f172a",
          backgroundGradientTo: "#111827",
          decimalPlaces: 0,
          color: (opacity = 1) => `rgba(148, 163, 184, ${opacity})`,
          labelColor: (opacity = 1) => `rgba(226, 232, 240, ${opacity})`,
          propsForBackgroundLines: {
            strokeDasharray: "6 6",
            stroke: "rgba(71, 85, 105, 0.45)",
          },
        }}
        style={styles.chart}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    borderWidth: 1,
    borderColor: "rgba(34, 211, 238, 0.26)",
    borderRadius: 14,
    backgroundColor: "rgba(15, 23, 42, 0.92)",
    padding: 12,
    marginBottom: 12,
  },
  title: {
    color: "#e2f8ff",
    fontWeight: "700",
    fontSize: 15,
  },
  subtitle: {
    color: "#8fa7b5",
    fontSize: 11,
    marginTop: 4,
    marginBottom: 8,
  },
  chart: {
    borderRadius: 12,
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
