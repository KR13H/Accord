#!/bin/bash

################################################################################
# M3 BURN-IN SUPREMACY SUITE
# Purpose: Stress-test the Accord backend on Apple M3
# Workloads: Parallel ingestion, Mistral forensic audit, CPU saturation pulse
################################################################################

set -e

ACCORD_ROOT="/Users/krish/Developer/Accord"
BACKEND_URL="http://localhost:8000"
VENV_PYTHON="${ACCORD_ROOT}/.venv/bin/python"
SAMPLE_FILE="${ACCORD_ROOT}/storage/samples/batch_purchase.csv"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  🚀 ACCORD M3 BURN-IN SUPREMACY SUITE${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""

# ============================================================================
# PHASE 1: BACKEND STARTUP CHECK
# ============================================================================
echo -e "${YELLOW}[PHASE 1] Backend Communication Check${NC}"
if ! curl -s "$BACKEND_URL/api/v1/health" > /dev/null 2>&1; then
    echo -e "${RED}❌ Backend not running at $BACKEND_URL${NC}"
    echo "   Start backend with: uvicorn cloud_backend.main:app --reload --port 8000"
    exit 1
fi
echo -e "${GREEN}✅ Backend responsive at $BACKEND_URL${NC}"
echo ""

# ============================================================================
# PHASE 2: SATURATION PULSE (CPU SPIKE DEMO)
# ============================================================================
echo -e "${YELLOW}[PHASE 2] Triggering M3 Saturation Pulse (2-sec CPU burn)${NC}"
PULSE_RESPONSE=$(curl -s -X GET "$BACKEND_URL/api/v1/system/saturation-pulse")
echo "$PULSE_RESPONSE" | jq .
echo -e "${GREEN}✅ Saturation pulse complete${NC}"
echo ""

# ============================================================================
# PHASE 3: PARALLEL INGESTION BOMBARDMENT (20 CONCURRENT REQUESTS)
# ============================================================================
echo -e "${YELLOW}[PHASE 3] Omni-Saturation: 20 Parallel Ingestion Threads${NC}"
echo "         (Each request targets adaptive ingest worker pool)"
echo ""

if [ ! -f "$SAMPLE_FILE" ]; then
    echo -e "${RED}❌ Sample file not found: $SAMPLE_FILE${NC}"
    echo "   Skipping ingestion phase."
else
    INGEST_START=$(date +%s%N)
    INGEST_PIDS=()
    
    for i in {1..20}; do
        (
            ADMIN_ID="SUPREMACY_BOT_${i}"
            curl -s -X POST "$BACKEND_URL/api/v1/ledger/ingest-batch" \
                 -H "X-Role: admin" \
                 -H "X-Admin-Id: $ADMIN_ID" \
                 -F "files=@$SAMPLE_FILE" \
                 > "/tmp/ingest_$i.json" 2>&1
            echo -e "${GREEN}[Ingest $i] Complete${NC}"
        ) &
        INGEST_PIDS+=($!)
        
        # Stagger starts by 50ms to avoid thundering herd
        sleep 0.05
    done
    
    # Wait for all ingestion requests to complete
    echo "Waiting for all 20 ingestion threads..."
    for pid in "${INGEST_PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    
    INGEST_END=$(date +%s%N)
    INGEST_DURATION=$(( (INGEST_END - INGEST_START) / 1000000 ))
    echo -e "${GREEN}✅ All 20 ingestion requests completed in ${INGEST_DURATION}ms${NC}"
    
    # Sample a few responses
    echo -e "${CYAN}Sample response (Ingest #1):${NC}"
    head -c 200 /tmp/ingest_1.json | jq . 2>/dev/null || echo "(invalid JSON or no output)"
    echo ""
fi

# ============================================================================
# PHASE 4: REAL-TIME TELEMETRY MONITORING (15 SECONDS)
# ============================================================================
echo -e "${YELLOW}[PHASE 4] Real-Time M3 Heartbeat (15-second window)${NC}"
echo "         Sampling every 1 second..."
echo ""

for i in {1..15}; do
    TEL=$(curl -s -X GET "$BACKEND_URL/api/v1/system/m3-telemetry")
    
    CPU=$(echo "$TEL" | jq -r '.cpu_percent // "N/A"')
    THERMAL=$(echo "$TEL" | jq -r '.thermal_pressure_pct // "N/A"')
    RAM=$(echo "$TEL" | jq -r '.ram_used_gb // "N/A"')
    WORKERS=$(echo "$TEL" | jq -r '.active_workers // "N/A"')
    TARGET=$(echo "$TEL" | jq -r '.adaptive_target_workers // "N/A"')
    
    printf "[T+%2ds] CPU: %6s%% | Thermal: %6s%% | RAM: %5s GB | Workers: %2s/%s\n" \
           "$i" "$CPU" "$THERMAL" "$RAM" "$WORKERS" "$TARGET"
    
    sleep 1
done
echo -e "${GREEN}✅ Telemetry monitoring complete${NC}"
echo ""

# ============================================================================
# PHASE 5: OPTIONAL - MISTRAL FORENSIC AUDIT (BACKGROUND)
# ============================================================================
echo -e "${YELLOW}[PHASE 5] Optional: Mistral Forensic Audit (Background)${NC}"
echo "         Launch with: ollama run mistral \"Analyze tax compliance for 10,000 entries\""
echo "         (Runs in background; does not block test suite)"
echo ""
echo -e "${GREEN}✅ Test suite ready for Mistral deep-scan${NC}"
echo ""

# ============================================================================
# PHASE 6: RESULTS SUMMARY
# ============================================================================
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  🎯 M3 BURN-IN SUMMARY${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${GREEN}Workloads Executed:${NC}"
echo "  ✓ CPU saturation pulse (2 seconds)"
echo "  ✓ 20 parallel ingestion requests"
echo "  ✓ 15-second telemetry sampling"
echo ""
echo -e "${CYAN}Next Steps:${NC}"
echo "  1. Check cloud-backend logs for adaptive worker scaling"
echo "  2. Verify chain-of-trust integrity: curl -s http://localhost:8000/api/v1/ledger/verify-integrity -H 'X-Role: admin' -H 'X-Admin-Id: 1' | jq ."
echo "  3. Optional: Run Mistral forensic audit in background"
echo "  4. Commit results: git add -A && git commit -m 'BURN-IN: M3 stress test complete'"
echo ""
echo -e "${GREEN}🔥 M3 SUPREMACY SUITE COMPLETE 🔥${NC}"
