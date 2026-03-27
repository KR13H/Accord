# Accord Intelligence Phase - Field Directive (Japneet)

Date: 2026-03-18
Owner: Founding Team
Priority: P0
Objective: Generate decision-grade intelligence that creates durable product and GTM advantage over Vyapar, myBillBook, and BizGrow.

## Mission Output
Deliver 4 research reports in this exact format:
- Executive Summary (max 10 bullets)
- Evidence Table (claim, source URL, proof screenshot/file)
- Competitive Gap Matrix (Vyapar, myBillBook, BizGrow, Accord)
- Product Implication (what we build next)
- Risk Notes (legal, integration, execution)
- Recommended Action (ship now, validate, or park)

## Report 1 - IMS Regulatory Gap
Question:
How do Vyapar and myBillBook handle Invoice Management System (IMS) statuses ACCEPT/REJECT/PENDING for GSTR-2B workflows?

Required evidence:
- Product docs, release notes, support KB, or public demos.
- Screenshots of status workflows and user actions.
- Whether status updates are manual, semi-automated, or rules-driven.
- Whether anomaly handling is explainable and auditable.

Deliverables:
- Gap map: current behavior vs desired Accord behavior.
- Automation design notes for Mistral-based decision support.
- Compliance risk impact if user misses status actions.

Acceptance criteria:
- At least 8 primary sources.
- At least 2 sources per competitor.
- Clear recommendation for Accord IMS autopilot v1.

## Report 2 - Sharma Logistics Audit (Batch and Precision)
Question:
Do competitors support high-fidelity batch inventory for expiry and Decimal(12,4) precision in real trading scenarios (gold, chemicals, pharma)?

Required evidence:
- Batch-wise inventory features, expiry controls, lot-level valuation.
- Decimal precision limits in quantity, rate, tax, and ledger posting.
- Any rounding defects or user complaints at high volumes.

Deliverables:
- Stress-case matrix:
  - 0.0001 quantity movements
  - high-value invoice recalculation
  - batch split/merge edge cases
- Win statement: where Accord outperforms and why it matters commercially.

Acceptance criteria:
- Reproducible test sheet with sample transactions.
- At least 3 edge cases where competitor behavior fails or degrades.

## Report 3 - Tally Prime 4.0 Handshake (XSD and Direct Sync)
Question:
What is the exact XML contract for Tally Prime 4.0 direct sync including voucher payloads and image attachment support?

Required evidence:
- XSD/schema references or authoritative XML spec docs.
- Confirmed minimal valid voucher envelope.
- Constraints for attachments (pathing, encoding, size, accepted formats).
- Error response patterns from import failures.

Deliverables:
- Canonical schema map for:
  - ENVELOPE
  - HEADER
  - BODY/IMPORTDATA
  - REQUESTDESC/REQUESTDATA
  - VOUCHER and attachments
- Compatibility checklist for Accord exporter.
- Negative test cases to avoid failed imports.

Acceptance criteria:
- Working sample XML validated against documented structure.
- Evidence that attachment workflow is supported or explicitly unsupported.

## Report 4 - Hyper-Local GTM Clusters
Question:
Which 3 business clusters in India have the highest accounting friction that Accord can eliminate quickly?

Target examples:
- Surat textiles
- Ludhiana manufacturing
- Rajkot engineering
- Bhiwandi warehousing
- Jaipur gems/jewelry

Required evidence:
- Primary pain points in bookkeeping and GST workflows.
- Current software stack and workaround behavior.
- Typical ticket size and willingness to switch.

Deliverables:
- Top-3 cluster ranking with rationale.
- Pain-point scorecard:
  - handwritten ledger burden
  - reconciliation delay
  - invoice mismatch frequency
  - Tally handoff friction
- 30-day pilot plan per cluster.

Acceptance criteria:
- Minimum 5 credible sources per cluster.
- One field interview or quote per cluster where feasible.

## Delivery Protocol
- Deadline: T+48 hours for first draft, T+72 hours final.
- Storage path in repo: research/intelligence/
- File names:
  - IMS-Regulatory-Gap.md
  - Sharma-Logistics-Audit.md
  - Tally-Prime-Handshake.md
  - GTM-Cluster-Intel.md

## Quality Gate
A report is accepted only if:
- Claims are source-backed.
- Gaps are quantified.
- Product implication is explicit.
- Next action can be assigned to engineering immediately.

## Immediate Kickoff Message (Send As-Is)
Japneet, execute Intelligence Phase now. Deliver 4 reports per the directive in this file with source-backed evidence and concrete product implications. First draft in 48 hours, final in 72 hours.
