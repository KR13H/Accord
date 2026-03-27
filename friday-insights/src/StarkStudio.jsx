import { useEffect, useMemo, useRef, useState } from "react";
import { motion } from "framer-motion";
import GridLayout from "react-grid-layout";
import { Fingerprint, LayoutGrid, Mic, Plus, Radio, Save, Trash2 } from "lucide-react";
import GlobalMarketsBlock from "./components/StarkBlocks/GlobalMarketsBlock";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

const BLOCK_PRESETS = {
  kpi: { title: "KPI Beacon", content: "Revenue, margin, and working-capital pulse." },
  table: { title: "Ledger Lattice", content: "Invoice matrix with tax buckets and due aging." },
  text: { title: "Narrative Node", content: "Executive insights and compliance annotations." },
  chart: { title: "Flux Chart", content: "Weekly variance and trend momentum visualization." },
  voice: { title: "Voice Command", content: "Speak natural language and auto-map double-entry vouchers." },
  markets: { title: "Global Pulse", content: "Live FX rates and revaluation heartbeat for global ledgers." },
};

function nextGridPosition(count) {
  return {
    x: (count * 2) % 12,
    y: Infinity,
    w: 4,
    h: 3,
  };
}

function blockColor(type) {
  if (type === "kpi") return "from-cyan-400/20 to-blue-500/20 border-cyan-300/40";
  if (type === "table") return "from-emerald-400/20 to-teal-500/20 border-emerald-300/40";
  if (type === "chart") return "from-amber-400/20 to-orange-500/20 border-amber-300/40";
  if (type === "voice") return "from-cyan-400/25 to-sky-500/20 border-cyan-300/55";
  if (type === "markets") return "from-emerald-400/20 to-cyan-500/20 border-emerald-300/45";
  return "from-fuchsia-400/20 to-rose-500/20 border-fuchsia-300/40";
}

export default function StarkStudio() {
  const [templateName, setTemplateName] = useState("Stark Ledger Prime");
  const [templateType, setTemplateType] = useState("dashboard");
  const [blocks, setBlocks] = useState([
    { id: "blk-kpi-1", type: "kpi", title: "KPI Beacon", content: "Revenue and GST liability pulse." },
    { id: "blk-table-1", type: "table", title: "Ledger Lattice", content: "Reversal-ready invoice lattice." },
  ]);
  const [layout, setLayout] = useState([
    { i: "blk-kpi-1", x: 0, y: 0, w: 5, h: 3 },
    { i: "blk-table-1", x: 5, y: 0, w: 7, h: 4 },
  ]);
  const gridContainerRef = useRef(null);
  const [gridWidth, setGridWidth] = useState(1120);
  const [saveState, setSaveState] = useState({ loading: false, message: "", fingerprint: "" });
  const [listeningByBlock, setListeningByBlock] = useState({});
  const [fxRates, setFxRates] = useState({ USD: "83.1500", AED: "22.6400", GBP: "105.4200", EUR: "90.1200" });

  const blockMap = useMemo(() => new Map(blocks.map((block) => [block.id, block])), [blocks]);

  useEffect(() => {
    const node = gridContainerRef.current;
    if (!node) return;

    const update = () => {
      const measured = node.getBoundingClientRect().width;
      setGridWidth(Math.max(320, Math.floor(measured)));
    };

    update();
    if (typeof ResizeObserver !== "undefined") {
      const observer = new ResizeObserver(update);
      observer.observe(node);
      return () => observer.disconnect();
    }

    window.addEventListener("resize", update);
    return () => window.removeEventListener("resize", update);
  }, []);

  useEffect(() => {
    const loadRates = async () => {
      try {
        const res = await fetch("/api/v1/ledger/currencies");
        const data = await res.json();
        if (res.ok && data?.rates) {
          setFxRates(data.rates);
        }
      } catch {
        // Keep fallback rates when backend endpoint is unavailable.
      }
    };
    void loadRates();
  }, []);

  const addBlock = (type) => {
    const preset = BLOCK_PRESETS[type] || BLOCK_PRESETS.text;
    const id = `blk-${type}-${Date.now()}`;
    const next = { id, type, title: preset.title, content: preset.content };
    setBlocks((prev) => [...prev, next]);

    setLayout((prev) => {
      const pos = nextGridPosition(prev.length);
      return [...prev, { i: id, ...pos }];
    });
  };

  const removeBlock = (id) => {
    setBlocks((prev) => prev.filter((block) => block.id !== id));
    setLayout((prev) => prev.filter((item) => item.i !== id));
    setListeningByBlock((prev) => {
      const next = { ...prev };
      delete next[id];
      return next;
    });
  };

  const toggleListening = (id) => {
    setListeningByBlock((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  const saveTemplate = async () => {
    setSaveState({ loading: true, message: "Saving Stark template...", fingerprint: "" });
    try {
      const res = await fetch("/api/v1/studio/save-template", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Role": "admin",
          "X-Admin-Id": "1001",
        },
        body: JSON.stringify({
          name: templateName,
          template_type: templateType,
          layout,
          blocks,
        }),
      });

      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || `Unable to save template (${res.status})`);
      }

      setSaveState({
        loading: false,
        message: `Saved as ${data.name} (${data.storage_backend})`,
        fingerprint: data.pdf_fingerprint || "",
      });
    } catch (error) {
      setSaveState({
        loading: false,
        message: error instanceof Error ? error.message : "Template save failed",
        fingerprint: "",
      });
    }
  };

  return (
    <div className="min-h-screen bg-[#050b1e] text-slate-100 px-4 sm:px-6 py-8">
      <div className="mx-auto max-w-7xl">
        <div className="rounded-3xl border border-cyan-400/20 bg-slate-950/70 backdrop-blur-xl p-5 sm:p-6 mb-6">
          <p className="text-[11px] tracking-[0.24em] uppercase text-cyan-300">Stark Studio</p>
          <h1 className="text-2xl sm:text-3xl font-bold mt-2">Drag, Forge, and Save Stark Blocks</h1>
          <p className="text-sm text-slate-400 mt-2">
            Build accounting surfaces with block-level control, then export a fingerprinted PDF template for chain integrity.
          </p>

          <div className="mt-5 grid grid-cols-1 md:grid-cols-3 gap-3">
            <input
              value={templateName}
              onChange={(event) => setTemplateName(event.target.value)}
              className="md:col-span-2 rounded-xl bg-slate-900/80 border border-slate-700 px-3 py-2 text-sm"
              placeholder="Template name"
            />
            <select
              value={templateType}
              onChange={(event) => setTemplateType(event.target.value)}
              className="rounded-xl bg-slate-900/80 border border-slate-700 px-3 py-2 text-sm"
            >
              <option value="dashboard">Dashboard</option>
              <option value="invoice">Invoice</option>
              <option value="report">Report</option>
            </select>
          </div>

          <div className="mt-4 flex flex-wrap gap-2">
            {Object.keys(BLOCK_PRESETS).map((type) => (
              <button
                key={type}
                onClick={() => addBlock(type)}
                className="inline-flex items-center gap-1.5 rounded-lg border border-cyan-500/40 bg-cyan-900/20 px-3 py-1.5 text-xs font-semibold text-cyan-100 hover:bg-cyan-800/30"
              >
                <Plus className="w-3.5 h-3.5" /> Add {type}
              </button>
            ))}
            <button
              onClick={() => {
                void saveTemplate();
              }}
              disabled={saveState.loading || blocks.length === 0}
              className="ml-auto inline-flex items-center gap-2 rounded-lg border border-emerald-500/50 bg-emerald-900/25 px-4 py-2 text-xs font-semibold text-emerald-100 hover:bg-emerald-800/35 disabled:opacity-60"
            >
              <Save className="w-3.5 h-3.5" /> {saveState.loading ? "Saving..." : "Save Template"}
            </button>
          </div>

          {saveState.message ? (
            <div className="mt-4 rounded-xl border border-slate-700 bg-slate-900/75 px-3 py-2 text-xs">
              <p>{saveState.message}</p>
              {saveState.fingerprint ? (
                <p className="mt-1 text-cyan-200 flex items-center gap-1.5 break-all">
                  <Fingerprint className="w-3.5 h-3.5" /> {saveState.fingerprint}
                </p>
              ) : null}
            </div>
          ) : null}
        </div>

        <div ref={gridContainerRef} className="rounded-3xl border border-slate-800 bg-black/40 p-3 sm:p-4 overflow-hidden">
          <GridLayout
            className="layout"
            layout={layout}
            width={gridWidth}
            rowHeight={46}
            cols={12}
            margin={[12, 12]}
            onLayoutChange={(nextLayout) => setLayout(nextLayout)}
            draggableHandle=".stark-card-handle"
            compactType="vertical"
          >
            {blocks.map((block) => (
              <div key={block.id}>
                <motion.article
                  layout
                  initial={{ opacity: 0.8, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  className={`h-full rounded-2xl border bg-gradient-to-br ${blockColor(block.type)} backdrop-blur-sm`}
                >
                  <div className="stark-card-handle cursor-move px-3 pt-3 pb-2 flex items-center gap-2 border-b border-white/10">
                    <LayoutGrid className="w-3.5 h-3.5 text-cyan-100" />
                    <p className="text-xs font-semibold uppercase tracking-[0.14em] text-cyan-100">{block.type}</p>
                    <button
                      onClick={() => removeBlock(block.id)}
                      className="ml-auto inline-flex items-center rounded-md border border-rose-400/40 bg-rose-900/20 px-2 py-1 text-[10px] text-rose-200 hover:bg-rose-800/25"
                    >
                      <Trash2 className="w-3 h-3" />
                    </button>
                  </div>
                  <div className="px-3 py-3">
                    <h3 className="text-sm font-semibold text-white">{blockMap.get(block.id)?.title}</h3>
                    <p className="text-xs text-slate-200/90 mt-1">{blockMap.get(block.id)?.content}</p>
                    {block.type === "voice" ? (
                      <div className="mt-3 rounded-xl border border-cyan-500/35 bg-black/35 p-3">
                        <div className="flex items-center gap-3">
                          <div
                            className={`w-12 h-12 rounded-full border-2 flex items-center justify-center transition-all ${
                              listeningByBlock[block.id]
                                ? "border-cyan-300 shadow-[0_0_30px_rgba(34,211,238,0.7)] animate-pulse"
                                : "border-slate-600"
                            }`}
                          >
                            <Mic className={`w-5 h-5 ${listeningByBlock[block.id] ? "text-cyan-200" : "text-slate-400"}`} />
                          </div>
                          <div className="min-w-0">
                            <p className="text-[11px] uppercase tracking-[0.16em] text-cyan-200 inline-flex items-center gap-1.5">
                              <Radio className="w-3.5 h-3.5" /> Neural-Talk
                            </p>
                            <p className="text-xs text-slate-300 mt-1">
                              {listeningByBlock[block.id] ? "Listening for ledger command..." : "Standby: click to enable microphone state"}
                            </p>
                          </div>
                        </div>
                        <button
                          onClick={() => toggleListening(block.id)}
                          className="mt-3 w-full rounded-lg border border-cyan-500/45 bg-cyan-900/20 py-1.5 text-[11px] font-semibold uppercase tracking-[0.14em] text-cyan-100 hover:bg-cyan-800/30"
                        >
                          {listeningByBlock[block.id] ? "Stop Listening" : "Start Listening"}
                        </button>
                      </div>
                    ) : null}
                    {block.type === "markets" ? (
                      <div className="mt-3">
                        <GlobalMarketsBlock rates={fxRates} />
                      </div>
                    ) : null}
                  </div>
                </motion.article>
              </div>
            ))}
          </GridLayout>
        </div>
      </div>
    </div>
  );
}
