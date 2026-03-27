# Accord Mobile (Expo)

Cross-platform mobile client for Accord Network V2.8.

## What this scaffold includes
- iOS + Android Expo app shell
- Responsive phone/tablet layout
- Mobile session bootstrap
- SME to CA linking flow
- CA approval trigger flow
- UPI tax intent + webhook simulation
- Online transaction list
- Network-aware request retries with timeout protection
- Secure session token persistence via Expo SecureStore
- Voice chunk queue with interruption recovery and manual resend
- SSE realtime reconnect strategy with exponential backoff
- Offline/session-expiry UI banners for graceful failure states

## Run locally

1. Install dependencies

```bash
cd accord-mobile
npm install
```

2. Start Expo

```bash
npm run start
```

3. Run native target

```bash
npm run ios
npm run android
```

## Backend assumption
App points to http://127.0.0.1:8000 and uses:
- POST /api/v2/mobile/auth/session
- POST /api/v2/mobile/connect-ca
- POST /api/v2/mobile/gst/approve
- POST /api/v2/mobile/tax/upi/intent
- POST /api/v2/mobile/tax/upi/webhook
- GET /api/v2/mobile/transactions

## Reliability validation command

```bash
cd accord-mobile
npx tsc --noEmit
```
