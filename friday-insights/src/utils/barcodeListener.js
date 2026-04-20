import { useEffect, useRef } from "react";

const MAX_KEY_INTERVAL_MS = 30;
const MIN_BARCODE_LENGTH = 4;

function isTextEditable(element) {
  if (!element) return false;
  const tag = String(element.tagName || "").toLowerCase();
  return tag === "input" || tag === "textarea" || element.isContentEditable;
}

export function useBarcodeScanner(onScan) {
  const bufferRef = useRef("");
  const lastTsRef = useRef(0);

  useEffect(() => {
    const handleKeydown = (event) => {
      const now = performance.now();
      const diff = now - lastTsRef.current;
      lastTsRef.current = now;

      if (isTextEditable(document.activeElement)) {
        bufferRef.current = "";
        return;
      }

      if (event.key === "Enter") {
        const candidate = bufferRef.current;
        bufferRef.current = "";
        if (/^\d+$/.test(candidate) && candidate.length >= MIN_BARCODE_LENGTH) {
          onScan?.(candidate);
          event.preventDefault();
        }
        return;
      }

      if (!/^\d$/.test(event.key)) {
        bufferRef.current = "";
        return;
      }

      if (diff > MAX_KEY_INTERVAL_MS) {
        bufferRef.current = event.key;
      } else {
        bufferRef.current = `${bufferRef.current}${event.key}`;
      }
    };

    window.addEventListener("keydown", handleKeydown);
    return () => window.removeEventListener("keydown", handleKeydown);
  }, [onScan]);
}
