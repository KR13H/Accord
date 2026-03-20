import { useMemo, useState } from "react";
import GridLayout from "react-grid-layout";
import LedgerBlock from "./components/StarkBlocks/LedgerBlock";
import NexusBlock from "./components/StarkBlocks/NexusBlock";
import SupremacyChart from "./components/Charts/SupremacyChart";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

export default function DraggableDashboard() {
  const [layout, setLayout] = useState([
    { i: "nexus", x: 0, y: 0, w: 4, h: 4 },
    { i: "ledger", x: 4, y: 0, w: 4, h: 4 },
    { i: "chart", x: 8, y: 0, w: 4, h: 6 },
  ]);

  const ledgerMock = useMemo(
    () => [
      { reference: "ACC/25-26/000451", description: "Purchase - Castrol stock" },
      { reference: "ACC/25-26/000452", description: "Sale - B2B invoice" },
      { reference: "ACC/25-26/000453", description: "Inter-godown transfer" },
    ],
    []
  );

  return (
    <div className="min-h-screen bg-black text-white p-4 sm:p-6">
      <div className="mx-auto max-w-7xl rounded-3xl border border-cyan-400/25 bg-slate-950/70 p-4 sm:p-5">
        <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-300">Stark Blocks</p>
        <h1 className="text-2xl font-bold mt-2">Draggable Dashboard</h1>
        <p className="text-sm text-slate-400 mt-1">Move Nexus, Telemetry, and Ledger blocks like a financial page builder.</p>

        <div className="mt-4 overflow-x-auto">
          <GridLayout
            className="layout"
            layout={layout}
            cols={12}
            rowHeight={52}
            width={1100}
            margin={[12, 12]}
            onLayoutChange={(next) => setLayout(next)}
            draggableHandle=".stark-drag-handle"
          >
            <div key="nexus">
              <div className="stark-drag-handle cursor-move text-[10px] uppercase tracking-[0.15em] text-cyan-300 mb-1">drag</div>
              <NexusBlock nodes={128} edges={322} integrityScore={99.4} />
            </div>
            <div key="ledger">
              <div className="stark-drag-handle cursor-move text-[10px] uppercase tracking-[0.15em] text-cyan-300 mb-1">drag</div>
              <LedgerBlock entries={ledgerMock} />
            </div>
            <div key="chart">
              <div className="stark-drag-handle cursor-move text-[10px] uppercase tracking-[0.15em] text-cyan-300 mb-1">drag</div>
              <SupremacyChart />
            </div>
          </GridLayout>
        </div>
      </div>
    </div>
  );
}
