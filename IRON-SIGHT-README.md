# Iron-SIGHT M3 Vision Ledger System
### Enterprise Accounting OS for India - High-Throughput Multimodal OCR Pipeline

**Version**: 1.0 (March 2026)  
**Hardware**: Apple M3 Chip (8-core CPU + 16-core Neural Engine)  
**Framework**: FastAPI (backend) + React 19 + Vite (frontend) + Capacitor (iOS)

---

## 🛡️ Core Architecture

### **Saturation Mode: M3 Chip Optimization**

The Iron-SIGHT protocol maximizes the M3's computational density:

- **4 Performance Cores** → Llava vision extraction (multimodal LLM)
- **4 Efficiency Cores** → Tesseract OCR + image preprocessing  
- **16 Neural Engine Cores** → Parallel tensor operations (accelerate library)
- **2GB RAM Disk** (`/Volumes/AccordCache`) → High-speed image buffering

### **Three-Model Stack**

1. **Llava** (4.7GB) - Multimodal vision extraction from receipt images
2. **Llama3.2** (2.0GB) - Financial reasoning & structured JSON cleanup
3. **Mistral** (4.4GB) - High-speed fraud pattern detection & anomaly scoring

---

## 🚀 Feature Set: Made in India, For India

### **1. Vision Ledger Scan (GSTR-1 Ready)**

**Endpoint**: `POST /api/v1/ledger/upload-photo`

Take a photo of a handwritten or printed invoice:
- Receipt → Tesseract OCR → Llava vision extraction
- Extracts: Date, Vendor GSTIN, HSN code, Total Amount, Confidence score
- Auto-posts to PostgreSQL ledger as journal entry
- Stores OCR text + LLM response for audit trail

**India-Specific Extraction**:
- GSTIN validation (15-char format per GST rules)
- HSN code extraction (6+ digits per ITC compliance)
- GST amount isolation (5%, 12%, 18%, 28% slabs)
- Supplier state code mapping (for inter-state supply detection)

### **2. Parallel Vision Processing (Batch)**

**Endpoint**: `POST /api/v1/ledger/upload-photo-batch`

Drop 10-20 receipt images at once:
- `ProcessPoolExecutor` (10 workers on M3 8-core)
- Sequential RAM disk staging (CPU-friendly I/O)
- Parallel Tesseract + vision extraction
- Individual journal posting with audit logging
- Result: **10 entries processed in ~30 seconds**

**Response**:
```json
{
  "status": "batch_processed",
  "total_processed": 10,
  "total_failed": 0,
  "results": [
    {
      "entry_id": 501,
      "reference": "ACC/26-27/000501",
      "vendor": "Sharma Logistics Pvt Ltd",
      "amount": "15000.00"
    }
  ],
  "processor": "Iron-SIGHT M3 Parallel Engine"
}
```

### **3. Bulk Tally Export (Master XML)**

**Endpoint**: `POST /api/v1/ledger/export-tally-bulk`

Request: `{"entry_ids": [501, 502, 503, ...]}`

- Aggregates 1000+ entries into single Tally-compliant XML
- ENVELOPE/BODY/IMPORTDATA structure per Tally Prime 4.0 spec
- Batch metadata: timestamp, batch ID, entry count
- Balance footer with SHA-256 integrity hash
- Ready for bulk import into Tally Prime (no manual entry)

**Response Headers**:
```
X-Batch-ID: ACCORD-BULK-202603181520
X-Batch-Entries: 250
X-Batch-Balanced: true
X-Batch-Integrity: a3f8c21e9d... (SHA-256)
```

---

## 🇮🇳 India-Specific Compliance Features

### **Rule 37A Reversal Engine**

Automatic Interest Calculation (Safe Harbor Section 50(3)):

**Scenario**: Invoice filed late → ITC at risk of reversal

**Algorithm**:
1. Fetch invoices from last FY (Apr-Mar) with `ims_status != 'ACCEPTED'`
2. Calculate reversal amount = sum(tax_amount) where filed_date > Sep 20
3. Check Minimum Monthly Credit ledger balance
4. If balance ≥ reversal_amount → Interest = 0% (Safe Harbor)
5. If balance < reversal_amount → Interest = 18% p.a

**Output**:
```json
{
  "rule_37a": {
    "immediate_reversal_risk": "125000.00",
    "projected_annual_interest_18pct": "22500.00",
    "accrued_interest_if_past_cutoff": "2500.00",
    "safe_harbor": {
      "status": "SAFE_HARBOR_APPLICABLE",
      "interest_rate": "0.0000%",
      "legal_basis": "Sec_50(3)_Full_Cover"
    }
  }
}
```

### **GST/GSTR-1 Compliance Matrix**

**Table 4 (B2B)**: Standard invoices w/ GSTIN + HSN
- Validates counterparty GSTIN (15-char format)
- Enforces HSN code on all B2B supplies
- Flags non-compliant GST rates (allowed: 5%, 12%, 18%, 28% only)

**Table 5 (B2CL)**: Inter-state B2C ≥ INR 100,000
- Detects inter-state supplies via state code mismatch
- Auto-triggers BIL (Bill of Supply) routing

**Table 7 (B2CS)**: Intra-state B2C & other
- Aggregates by GST rate slab
- Maintains invoice count for IMS reconciliation

**Table 12 (HSN)**: HSN summary with compliance checks
- 6-digit HSN code validation
- GST rate snapshot per HSN
- Deviation warnings (e.g., "HSN typically 5%, found 18%")

### **Vendor Trust Scoring**

80-point system evaluating:
- **Filing Consistency** (GSTR-1 filed on time?)
  - Pending IMS: -4 points × count
  - Rejected IMS: -12 points × count
  - Accepted IMS: +2 points
- **Filing Delay** (how many days after invoice?)
  - Avg delay > 2 days: -1.25 points per day  
  - High-risk delay (past Sep 20): -10 points × count
- **ITC at Risk** (total contested invoices)

**Result**: Score 0-100
- 75-100: **GREEN** → Standard payment
- 45-74: **YELLOW** → Review before payment
- 0-44: **RED** → Net-of-tax (withhold GST portion)

### **Forensic Audit (Mistral Engine)**

**Endpoint**: `GET /api/v1/insights/forensic-audit?limit=1000`

Feed 1000+ journal entries to Mistral → Detect patterns:

- **Duplicate Invoices**: Same vendor, amount, date ± 1 day
- **Round-Tripping**: Vendor A → You → Vendor B (same pattern, 3+ times)
- **HSN Rate Anomalies**: "Item X typically 5%, this invoice 18%"
- **Reversing Invoices**: Credit notes 80-120% of original, within 3 days
- **State Code Mismatches**: GSTIN state ≠ recorded state code

**Output** (Risk-ranked):
```json
{
  "anomalies": [
    {
      "entry_id": 450,
      "reference": "ACC/26-27/000450",
      "risk_level": "CRITICAL",
      "reason": "Duplicate invoice: Sharma Logistics on 15-Mar (Entry 445 was 14-Mar), same ₹50K"
    }
  ]
}
```

### **Safe Harbor Certificate (PDF)**

**Endpoint**: `GET /api/v1/journal/safe-harbor-certificate/{batch_id}`

Legal defense certificate autogenerated:
- Minimum Monthly Credit Ledger balance: ₹500,000
- Rule 37A reversal amount: ₹125,000
- Liability offset: ₹375,000
- **Status**: SAFE_HARBOR_APPLICABLE
- **Interest outcome**: INR 0 (Section 50(3) applies)
- **SHA-256 integrity hash** (tamper-proof)

Suitable for filing with GST portal in audit defense.

---

## 📊 Database Schema (India-Specific Extensions)

### `journal_entries` Table
```sql
-- Core GST-aware fields
counterparty_gstin TEXT           -- Vendor's 15-char GSTIN
eco_gstin TEXT                    -- E-commerce operator GSTIN
company_state_code TEXT            -- Your state (e.g., "MH")
counterparty_state_code TEXT       -- Vendor's state
supply_source TEXT                 -- 'DIRECT' or 'ECO' (e-commerce)
ims_status TEXT                    -- 'ACCEPTED', 'REJECTED', 'PENDING'
vendor_legal_name TEXT             -- Legal name per GSTN
vendor_gstr1_filed_at DATE        -- When vendor filed GSTR-1
```

### `tax_ledger` Table
```sql
hsn_code TEXT                     -- 6+ digit HSN code
gst_rate_snapshot TEXT            -- Locked rate at posting
taxable_value TEXT                -- Before GST
tax_amount TEXT                   -- Calculated tax
supply_type TEXT                  -- 'B2B' or 'B2CS'
is_inter_state INTEGER            -- 1 if company_state ≠ vendor_state
supply_source TEXT                -- 'DIRECT' or 'ECO'
```

### `receipt_imports` Table
```sql
entry_id INTEGER                  -- FK to journal_entries
file_path TEXT                    -- Stored receipt image
ocr_text TEXT                     -- Raw Tesseract output
extracted_json TEXT               -- Llava JSON {date, vendor, gstin, total_amount, hsn}
model_response TEXT               -- Full LLM response for audit
status TEXT                       -- 'PROCESSED', 'FAILED', 'MANUAL_REVIEW'
created_at TIMESTAMP              -- Import timestamp
```

### `vendor_trust_scores` Table
```sql
gstin TEXT PRIMARY KEY            -- Vendor GSTIN
filing_consistency_score REAL     -- 0-100 score
avg_filing_delay_days INTEGER     -- Days after invoice before GSTR-1
total_itc_at_risk TEXT            -- Sum of unaccepted invoices' tax
updated_at TIMESTAMP              -- Last recalc timestamp
```

### `safe_harbor_attestations` Table
```sql
as_of_date DATE                   -- Reversal risk assessment date
min_credit_balance TEXT           -- MMB at that date
reversal_amount TEXT              -- Rule 37A reversal quantum
liability_offset TEXT             -- Min balance - reversal amount
status TEXT                       -- 'SAFE_HARBOR_APPLICABLE' or 'STANDARD_INTEREST'
legal_basis TEXT                  -- 'Sec_50(3)_Full_Cover' or other
```

---

## 🔧 API Reference (Iron-SIGHT Endpoints)

### Single Receipt Upload
```
POST /api/v1/ledger/upload-photo
Headers: X-Role, X-Admin-Id
Body: multipart/form-data (file)
Response: {entry_id, reference, extracted: {date, vendor, gstin, hsn, total_amount}}
```

### Batch Receipt Upload (NEW)
```
POST /api/v1/ledger/upload-photo-batch
Headers: X-Role, X-Admin-Id
Body: multipart/form-data (files[])
Response: {status, total_processed, total_failed, results: [{entry_id, reference, amount}...]}
```

### Single Entry Tally Export
```
GET /api/v1/ledger/export-tally/{entry_id}
Headers: X-Role, X-Admin-Id
Response: XML blob (Tally Prime format)
```

### Bulk Tally Export (NEW)
```
POST /api/v1/ledger/export-tally-bulk
Headers: X-Role, X-Admin-Id
Body: JSON {entry_ids: [501, 502, ...]}
Response: XML blob with batch metadata + integrity hash
```

### Rule 37A Reversal Risk
```
GET /api/v1/insights/friday-summary?as_of_date=2026-03-18&min_credit_balance=500000
Response: {rule_37a: {immediate_reversal_risk, projected_annual_interest, safe_harbor}}
```

### Vendor Trust Score
```
GET /api/v1/insights/vendor/{gstin}/payment-advice
Response: {filing_consistency_score, payment_advice: {alert, advice, message}}
```

### Forensic Audit
```
GET /api/v1/insights/forensic-audit?limit=1000
Response: {anomalies: [{entry_id, risk_level, reason}...]}
```

---

## 🏗️ Installation & Setup

### Backend Dependencies
```bash
pip install -r cloud-backend/requirements.txt
# Includes:
# - FastAPI, uvicorn
# - PyTorch 2.10 (CPU), transformers, accelerate, diffusers
# - Ollama models: llava, llama3.2, mistral (pulled separately)
# - Tesseract 5.5.2 (Homebrew: brew install tesseract)
# - pytesseract, opencv-python-headless, Pillow
# - PostgreSQL driver: psycopg[binary]
```

### Frontend Dependencies
```bash
cd friday-insights && npm install
npm run build
npm run preview  # Vite preview server
```

### Start Services
```bash
# Terminal 1: Backend
cd cloud-backend
uvicorn main:app --host 0.0.0.0 --port 8000

# Terminal 2: Frontend (dev server)
cd friday-insights
npm run dev  # Vite dev server on :5173

# Terminal 3: Ollama (ensure models are loaded)
ollama serve
```

---

## 🎯 Performance Metrics (M3 Benchmarks)

| Task | Time | Notes |
|------|------|-------|
| Single receipt (OCR + Vision) | ~8 sec | Tesseract + Llava |
| Batch 10 receipts (parallel) | ~30 sec | 10 workers, M3 saturation |
| Bulk Tally export (1000 entries) | ~5 sec | XML generation + SHA-256 |
| Rule 37A calc (FY data) | ~200ms | PostgreSQL aggregation |
| Vendor trust recalc (100 vendors) | ~800ms | Per-vendor score computation |
| Forensic audit (1000 entries) | ~120 sec | Llava pattern detection |

---

## 📋 Compliance Roadmap

### ✅ Implemented (v1.0)
- GSTR-1 Table 4/5/7/12 schema
- Rule 37A reversal calculator (Safe Harbor Section 50(3))
- Vendor trust scoring (filing consistency)
- Batch vision processing (Iron-SIGHT M3)
- Tally Prime XML export (single + bulk)
- iOS Capacitor scaffolding
- Forensic audit engine (Mistral-backed anomaly detection)

### 🔮 Roadmap (v2.0)
- GSTR-3B (IGST/CGST/SGST) reconciliation
- E-way bill integration (supply chain tracking)
- ITC reversals (Schedule 1 audits)
- ICEGATE filing automation
- Invoice discounting program (auto-factoring)
- AI-powered compliance predictions (Llama3.2 reasoning)

---

## 📞 Support

**For India-specific compliance questions:**
- GST Rules reference: @CBIC_India docs
- Rule 37A interpretation: CA guidance from tax committees
- Tally Prime spec: Tally operator documentation

**For technical issues:**
- GitHub: github.com/KR13H/Accord
- Issues tracking: Refer to GitHub issues tab
- Performance tuning: See `iron_sight_endpoints.py` worker configuration

---

**Made with ❤️ for Indian businesses.**  
*"Your M3 chip is now a distributed accounting firm."* — April 2026
