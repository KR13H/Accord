import { useMemo, useState } from "react";

function formatInr(value) {
  return new Intl.NumberFormat("en-IN", { style: "currency", currency: "INR" }).format(Number(value || 0));
}

export default function DynamicReportTable({ columns = [], data = [] }) {
  const [sort, setSort] = useState({ key: "", direction: "asc" });

  const sortedRows = useMemo(() => {
    if (!sort.key) return data;
    const clone = [...data];
    clone.sort((a, b) => {
      const av = a?.[sort.key];
      const bv = b?.[sort.key];
      if (typeof av === "number" || typeof bv === "number") {
        return sort.direction === "asc" ? Number(av || 0) - Number(bv || 0) : Number(bv || 0) - Number(av || 0);
      }
      return sort.direction === "asc"
        ? String(av ?? "").localeCompare(String(bv ?? ""))
        : String(bv ?? "").localeCompare(String(av ?? ""));
    });
    return clone;
  }, [data, sort]);

  const toggleSort = (key) => {
    setSort((prev) => {
      if (prev.key === key) {
        return { key, direction: prev.direction === "asc" ? "desc" : "asc" };
      }
      return { key, direction: "asc" };
    });
  };

  return (
    <div className="overflow-auto rounded-xl border border-slate-800 max-h-[420px]">
      <table className="min-w-full text-sm">
        <thead className="sticky top-0 bg-slate-900 text-slate-300 z-10">
          <tr>
            {columns.map((col) => (
              <th key={col.key} className="p-2 text-left cursor-pointer" onClick={() => toggleSort(col.key)}>
                {col.label} {sort.key === col.key ? (sort.direction === "asc" ? "▲" : "▼") : ""}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sortedRows.length === 0 ? (
            <tr><td colSpan={Math.max(columns.length, 1)} className="p-3 text-slate-400">No rows</td></tr>
          ) : (
            sortedRows.map((row, idx) => (
              <tr key={idx} className="border-t border-slate-800">
                {columns.map((col) => {
                  const value = row?.[col.key];
                  const rendered = col.type === "currency" ? formatInr(value) : String(value ?? "");
                  return <td key={col.key} className="p-2">{rendered}</td>;
                })}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
