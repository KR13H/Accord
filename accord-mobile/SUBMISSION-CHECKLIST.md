# Accord Mobile Submission Checklist (V2.8 Reliability Sprint)

Date: 2026-03-19
Scope: iOS/Android resilience hardening for App Review and Play review.

## Checklist

| Pillar | Result | Evidence | Notes |
|---|---|---|---|
| API failover + retry UX | PASS | Retry/timeout wrapper with exponential delays in App.tsx (`requestJson`, `fetchWithTimeout`) and offline banners in UI | Covers transient 5xx/429, timeout abort, and explicit offline message |
| Auth/session hardening | PASS | SecureStore token read/write/clear and forced session reset on 401/403 in App.tsx (`clearSession`) | Session expiry drives user-visible relogin banner |
| Voice interruption reliability | PASS | AppState pause handling, queue-based chunk buffering, flush and retry, commit flush before submit in App.tsx (`flushChunkQueue`, `commitVoiceToLedger`) | Handles OS background/interrupt scenarios without dropping queued chunks |
| SSE/live update resilience | PASS | Tokenized SSE bootstrap and reconnect with backoff+jitter in App.tsx (`connectSse`, `source.onerror`) | Includes manual reconnect and fallback status when EventSource is unavailable |
| Release quality gate | PASS | TypeScript compile check completed: `npx tsc --noEmit` exited clean after refactor | Code-level validation passed; physical-device runtime matrix still recommended |

## Evidence Artifacts

1. Mobile reliability implementation: App.tsx
2. Added dependencies: expo-secure-store, @react-native-community/netinfo in package.json
3. Compile validation command:

```bash
cd accord-mobile
npx tsc --noEmit
```

## Recommended Final Manual Device Matrix (Before Store Upload)

- iOS phone (latest iOS): toggle airplane mode during API call -> verify offline banner + retry behavior.
- Android mid-tier device: start voice capture, background app, resume app -> verify queue flush and successful commit.
- iOS + Android: disable/re-enable network during SSE session -> verify reconnect state transitions to live.
- Tablet layout (iPad/Android tablet): verify controls remain accessible and no clipped input/buttons.
- Authentication: simulate token expiry response (401/403) -> verify forced relogin flow and token clearance.
