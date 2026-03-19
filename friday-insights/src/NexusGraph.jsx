import { useEffect, useMemo, useRef, useState } from "react";

function hashToNumber(input) {
  let hash = 0;
  for (let i = 0; i < input.length; i += 1) {
    hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
  }
  return hash;
}

export default function NexusGraph({ graph, onRiskNodeSelect }) {
  const canvasRef = useRef(null);
  const containerRef = useRef(null);
  const animationRef = useRef(0);
  const modelRef = useRef({ nodes: [], edges: [], width: 860, height: 360 });
  const [selectedId, setSelectedId] = useState("");

  const view = useMemo(() => {
    const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
    const edges = Array.isArray(graph?.edges) ? graph.edges : [];
    const riskVendors = new Set(
      (Array.isArray(graph?.risk_clusters) ? graph.risk_clusters : [])
        .flatMap((cluster) => (Array.isArray(cluster.vendors) ? cluster.vendors : []))
        .map((item) => String(item))
    );

    const width = 860;
    const height = 360;
    const centerX = width / 2;
    const centerY = height / 2;

    const indexed = nodes.map((node, idx) => {
      const id = String(node.id);
      const kind = String(node.kind || "vendor");
      const seed = hashToNumber(id + kind + String(idx));
      const angle = ((seed % 360) * Math.PI) / 180;
      const radius = kind === "anchor" ? 0 : kind === "amount_bucket" ? 150 : 120;
      const jitter = (seed % 29) - 14;
      return {
        id,
        kind,
        seed,
        x: centerX + Math.cos(angle) * (radius + jitter),
        y: centerY + Math.sin(angle) * (radius + jitter),
        vx: 0,
        vy: 0,
        risk: riskVendors.has(id),
        radius: kind === "anchor" ? 11 : kind === "amount_bucket" ? 7 : 8,
      };
    });

    const byId = new Map(indexed.map((item) => [item.id, item]));
    const safeEdges = edges
      .map((edge) => {
        const source = byId.get(String(edge.source));
        const target = byId.get(String(edge.target));
        if (!source || !target) {
          return null;
        }
        const risk = source.risk || target.risk;
        return { source, target, risk };
      })
      .filter(Boolean);

    return { width, height, nodes: indexed, edges: safeEdges, riskClusters: graph?.risk_clusters || [] };
  }, [graph]);

  useEffect(() => {
    modelRef.current = {
      width: view.width,
      height: view.height,
      nodes: view.nodes.map((node) => ({ ...node })),
      edges: view.edges.map((edge) => ({ source: edge.source.id, target: edge.target.id, risk: edge.risk })),
    };
  }, [view]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) {
      return undefined;
    }

    const context = canvas.getContext("2d");
    if (!context) {
      return undefined;
    }

    const resize = () => {
      const parent = containerRef.current;
      const cssWidth = parent ? Math.max(320, Math.floor(parent.clientWidth - 2)) : view.width;
      const cssHeight = 320;
      const ratio = window.devicePixelRatio || 1;
      canvas.style.width = `${cssWidth}px`;
      canvas.style.height = `${cssHeight}px`;
      canvas.width = Math.floor(cssWidth * ratio);
      canvas.height = Math.floor(cssHeight * ratio);
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
      modelRef.current.width = cssWidth;
      modelRef.current.height = cssHeight;
    };

    resize();
    window.addEventListener("resize", resize);

    let previous = performance.now();
    const tick = (now) => {
      const dt = Math.min((now - previous) / 1000, 0.025);
      previous = now;

      const model = modelRef.current;
      const nodes = model.nodes;
      const edges = model.edges;

      if (nodes.length > 0) {
        const byId = new Map(nodes.map((item) => [item.id, item]));
        const centerX = model.width / 2;
        const centerY = model.height / 2;

        for (let i = 0; i < nodes.length; i += 1) {
          const a = nodes[i];
          for (let j = i + 1; j < nodes.length; j += 1) {
            const b = nodes[j];
            const dx = a.x - b.x;
            const dy = a.y - b.y;
            const dist2 = Math.max(40, dx * dx + dy * dy);
            const force = 4200 / dist2;
            const invDist = 1 / Math.sqrt(dist2);
            const fx = dx * invDist * force;
            const fy = dy * invDist * force;
            a.vx += fx * dt;
            a.vy += fy * dt;
            b.vx -= fx * dt;
            b.vy -= fy * dt;
          }
        }

        for (const edge of edges) {
          const source = byId.get(edge.source);
          const target = byId.get(edge.target);
          if (!source || !target) {
            continue;
          }
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const distance = Math.max(0.001, Math.hypot(dx, dy));
          const targetLength = source.kind === "anchor" || target.kind === "anchor" ? 90 : 130;
          const stretch = distance - targetLength;
          const spring = edge.risk ? 0.11 : 0.08;
          const fx = (dx / distance) * stretch * spring;
          const fy = (dy / distance) * stretch * spring;
          source.vx += fx * dt;
          source.vy += fy * dt;
          target.vx -= fx * dt;
          target.vy -= fy * dt;
        }

        for (const node of nodes) {
          const towardCenterX = (centerX - node.x) * 0.012;
          const towardCenterY = (centerY - node.y) * 0.012;
          node.vx += towardCenterX * dt;
          node.vy += towardCenterY * dt;
          node.vx *= 0.94;
          node.vy *= 0.94;
          node.x += node.vx * 60 * dt;
          node.y += node.vy * 60 * dt;
          node.x = Math.max(20, Math.min(model.width - 20, node.x));
          node.y = Math.max(20, Math.min(model.height - 20, node.y));
        }
      }

      context.clearRect(0, 0, model.width, model.height);
      context.fillStyle = "rgba(2, 6, 23, 0.92)";
      context.fillRect(0, 0, model.width, model.height);

      const pulse = (Math.sin(now / 340) + 1) / 2;

      for (const edge of model.edges) {
        const source = model.nodes.find((node) => node.id === edge.source);
        const target = model.nodes.find((node) => node.id === edge.target);
        if (!source || !target) {
          continue;
        }
        context.beginPath();
        context.moveTo(source.x, source.y);
        context.lineTo(target.x, target.y);
        context.strokeStyle = edge.risk ? `rgba(248,113,113,${0.5 + pulse * 0.35})` : "rgba(34,211,238,0.28)";
        context.lineWidth = edge.risk ? 2.1 : 1.1;
        context.stroke();
      }

      for (const node of model.nodes) {
        context.beginPath();
        const isSelected = node.id === selectedId;
        const radius = node.risk ? node.radius + pulse * 1.7 : node.radius;
        context.arc(node.x, node.y, isSelected ? radius + 1.7 : radius, 0, Math.PI * 2);
        if (node.kind === "anchor") {
          context.fillStyle = "#22d3ee";
        } else if (node.risk) {
          context.fillStyle = "#ef4444";
        } else if (node.kind === "amount_bucket") {
          context.fillStyle = "#f59e0b";
        } else {
          context.fillStyle = "#e2e8f0";
        }
        context.fill();

        if (node.risk) {
          context.shadowColor = "rgba(239,68,68,0.85)";
          context.shadowBlur = 18;
          context.fill();
          context.shadowBlur = 0;
        }

        context.fillStyle = node.risk ? "#fecaca" : "#cbd5e1";
        context.font = "10px ui-monospace, SFMono-Regular, Menlo, monospace";
        context.fillText(node.id.slice(0, 14), node.x + 10, node.y + 3);
      }

      animationRef.current = window.requestAnimationFrame(tick);
    };

    animationRef.current = window.requestAnimationFrame(tick);

    return () => {
      window.cancelAnimationFrame(animationRef.current);
      window.removeEventListener("resize", resize);
    };
  }, [view.width, selectedId]);

  const onCanvasClick = (event) => {
    const canvas = canvasRef.current;
    if (!canvas) {
      return;
    }

    const rect = canvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    const nodes = modelRef.current.nodes;

    let nearest = null;
    let minDist = Number.POSITIVE_INFINITY;
    for (const node of nodes) {
      const dx = node.x - x;
      const dy = node.y - y;
      const dist = Math.hypot(dx, dy);
      if (dist < minDist) {
        minDist = dist;
        nearest = node;
      }
    }

    if (!nearest || minDist > 18) {
      return;
    }

    setSelectedId(nearest.id);
    if (nearest.risk && typeof onRiskNodeSelect === "function") {
      onRiskNodeSelect({ id: nearest.id, kind: nearest.kind, risk: true });
    }
  };

  if (!graph || !Array.isArray(graph.nodes) || graph.nodes.length === 0) {
    return (
      <div className="rounded-2xl border border-slate-800 bg-slate-950/70 p-4 text-sm text-slate-400">
        Nexus graph is empty. Run GSTR-2B reconciliation to generate network intelligence.
      </div>
    );
  }

  return (
    <div className="rounded-2xl border border-cyan-700/35 bg-black/85 p-4">
      <div className="mb-3 flex items-center justify-between gap-3">
        <p className="text-sm font-semibold text-cyan-100">Nexus Graph Visualizer</p>
        <span className="text-[11px] text-slate-400">
          Nodes: {view.nodes.length} | Edges: {view.edges.length}
        </span>
      </div>
      <div ref={containerRef} className="rounded-lg border border-slate-800 bg-slate-950/70 overflow-hidden">
        <canvas ref={canvasRef} className="block w-full h-[320px] cursor-crosshair" onClick={onCanvasClick} />
      </div>
      <p className="mt-2 text-[10px] uppercase tracking-[0.18em] text-slate-500">
        Force simulation active. Click a red-risk node to trigger targeted nudge controls.
      </p>

      {view.riskClusters.length > 0 ? (
        <div className="mt-3 space-y-2">
          {view.riskClusters.slice(0, 4).map((cluster, idx) => (
            <div key={`cluster-${idx}`} className="rounded-lg border border-red-500/35 bg-red-900/20 px-3 py-2 text-xs text-red-100">
              {cluster.severity}: {(cluster.vendors || []).join(", ")}
            </div>
          ))}
        </div>
      ) : (
        <p className="mt-3 text-xs text-slate-400">No circular-trading clusters detected in current graph snapshot.</p>
      )}
    </div>
  );
}
