# Accord Real Estate SaaS

Accord is an AI-native financial and compliance operating system for Indian real estate developers. It combines accounting automation, compliance workflows, local-first AI, and multi-surface operations across web, mobile, and backend services.

## What Accord Solves

Indian real estate finance teams juggle high-stakes compliance requirements and fragmented systems. Accord centralizes this by providing:

- RERA-aware collection allocation and controls
- GST reconciliation and filing workflow support
- Maker/checker approvals for sensitive operations
- AI-assisted invoice and support workflows with local data handling
- Multi-tenant SPV workflows for project-level segregation

## Architecture Overview

Accord is a modular mono-repo with three major runtime surfaces:

- `cloud-backend/`: FastAPI APIs, compliance engines, services, Celery workers, SQLite/Postgres support
- `friday-insights/`: React web dashboard for finance, operations, compliance, and reporting
- `accord-mobile/`: React Native app for on-site execution, voice capture, and manager workflows

Primary data and AI execution are designed to run locally for privacy-sensitive financial operations.

## Tech Stack

### Backend

- FastAPI (Python)
- SQLite (default local), optional Postgres cutover path
- Celery workers (async jobs and schedules)
- Docker/Docker Compose runtime

### Frontend Web

- React + Vite
- Tailwind/CSS utility-driven UI styling

### Mobile

- React Native (TypeScript)
- Expo-compatible ecosystem integrations

### AI and Automation

- Ollama local inference (chat and extraction workflows)
- Local parsing pipelines for structured data extraction

## Core Features

- RERA 70/30 allocation and compliance tracking
- GST reconciliation and filing support workflows
- SPV-aware multi-tenancy paths
- Local AI parsing for invoices and support interactions
- Maker/checker approval controls for risk-bearing actions
- Rule 37A and audit trail support modules
- Vendor and portal integrations
- Mobile workflow support for distributed teams

## Repository Layout

- `cloud-backend/main.py`: primary API composition and route registration
- `cloud-backend/routes/`: domain-specific API routers
- `cloud-backend/services/`: business logic and compliance engines
- `cloud-backend/workers/`: Celery app and scheduled task modules
- `cloud-backend/sql/`: SQL bootstrap artifacts
- `friday-insights/src/`: web app modules and feature pages
- `accord-mobile/`: mobile app codebase and native integration layer

## Local Development Setup

### Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- Python 3.11+ for direct backend runs
- Node.js 18+ for local frontend/mobile builds
- Ollama (optional but recommended for local AI features)

### One-command stack start

```bash
docker compose up --build -d
```

### Health checks

- Backend health: `GET /api/v1/health`
- Friday AI health: `GET /api/v1/insights/friday-health`

### Optional direct backend run

```bash
cd cloud-backend
python -m uvicorn main:app --host 127.0.0.1 --port 8000
```

## Environment Notes

- Default backend persistence uses `cloud-backend/ledger.db`
- `DATABASE_URL` can be used for Postgres cutover mode
- `OLLAMA_HOST` controls local AI endpoint routing
- Celery broker/backend are configurable via `CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND`

## Security and Compliance Posture

- Local-first AI pipeline reduces third-party data egress
- Immutable-style audit and compliance logging flows
- Role-based access checks on privileged endpoints
- Workflow controls for approvals and filing operations

## Current Product Position

Accord is built for real estate teams that need compliance-grade controls with modern automation:

- Finance leadership and controllership
- Accounts and compliance operations
- Project-level field and mobile operators
- CA and external ecosystem collaborators

## License

Internal project repository. Add formal licensing terms before public distribution.
