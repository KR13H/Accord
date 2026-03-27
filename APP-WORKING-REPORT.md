# Accord App Working Report

Date: 2026-03-26

## 1) What the app does

Accord is a multi-surface ERP platform with a FastAPI backend and web/mobile clients. In the current Phase 15 state, the repository includes:

- Core accounting and business workflows (ledger, compliance, reporting, inventory, etc.)
- Universal SME pivot capabilities:
  - Transaction capture (cash, UPI, etc.)
  - Daily summary computation
  - Tally XML export for accounting handoff
  - SME dashboard views
  - Udhaar-style customer credit management
  - SME inventory and low-stock alerting
- Stress and quality assets:
  - AI-generated backend tests
  - Concurrency tests
  - Fuzz/property-based tests

## 2) High-level architecture

- Backend: `cloud-backend/`
  - Entrypoint: `main.py`
  - API routes: `routes/`
  - Domain services: `services/`
  - SQL schema assets: `sql/`
  - Tests: `tests/`
- Web frontend: `friday-insights/`
  - React/Vite app
  - Includes SME terminal/dashboard screens mounted in app routing
- Mobile frontend: `accord-mobile/`
  - React Native / Expo codebase for mobile workflows

## 3) Key SME API endpoints

Base backend URL example: `http://127.0.0.1:8000`

- `POST /api/v1/sme/transactions`
  - Records an SME transaction.
- `GET /api/v1/sme/summary`
  - Returns day-level financial summary.
- `GET /api/v1/sme/export/tally`
  - Exports current range as Tally-compatible XML.
- `GET /api/v1/sme/export/tally?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`
  - Exports bounded date range as XML.
- `POST /api/v1/sme/customers`
  - Creates Udhaar customer profile.
- `POST /api/v1/sme/customers/{customer_id}/charge`
  - Adds receivable balance.
- `POST /api/v1/sme/customers/{customer_id}/settle`
  - Settles receivable balance.
- `GET /api/v1/sme/inventory/low-stock`
  - Lists low-stock items.

## 4) How to start using the app

### Backend

1. Create/activate venv (if not already active).
2. Install dependencies:
   - `cd cloud-backend`
   - `pip install -r requirements.txt`
3. Start API server:
   - `uvicorn main:app --reload --port 8000`
4. Open docs:
   - `http://127.0.0.1:8000/docs`

### Web frontend

1. In a second terminal:
   - `cd friday-insights`
2. Install dependencies:
   - `npm install`
3. Start dev server:
   - `npm run dev`
4. Open the URL shown by Vite (usually `http://127.0.0.1:5173`).

### Mobile app (optional)

1. `cd accord-mobile`
2. `npm install`
3. `npm start` (or `npx expo start`)
4. Launch in emulator/device via Expo controls.

## 5) First smoke flow after startup

1. Create two sales via `POST /api/v1/sme/transactions` (cash + UPI).
2. Read dashboard summary via `GET /api/v1/sme/summary`.
3. Export accounting XML via `GET /api/v1/sme/export/tally`.
4. Verify XML contains `<ENVELOPE>` root.

## 6) Validation run in this session

- Git publish:
  - `main` successfully pushed to `origin` (`git@github.com:KR13H/Accord.git`).
- Backend test battery:
  - Command: `python -m pytest tests/ai_generated/test_universal_accounting_ai.py tests/test_concurrency.py tests/test_fuzzing.py -q`
  - Result: `15 passed`.
- API smoke checks:
  - `POST /api/v1/sme/transactions` (cash): `201`
  - `POST /api/v1/sme/transactions` (upi): `201`
  - `GET /api/v1/sme/summary`: `200`
  - `GET /api/v1/sme/export/tally`: `200` and XML contains `<ENVELOPE>`
  - `GET /api/v1/sme/export/tally` with date range: `200` and XML contains `<ENVELOPE>`

## 7) Operational notes

- The backend currently logs deprecation warnings for `datetime.utcnow()` and FastAPI startup event style (`@app.on_event("startup")`). These are non-blocking but should be modernized in a maintenance pass.
- If you continue local feature work, check `git stash list` and restore your prior unrelated changes with `git stash pop` when ready.
