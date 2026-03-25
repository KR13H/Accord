import { useEffect, useRef } from "react";

type UseSyncWorkerArgs = {
  networkOnline: boolean;
  onReconnect: () => Promise<void> | void;
  onOnlineHeartbeat?: () => Promise<void> | void;
};

/**
 * Triggers sync when network transitions from offline -> online.
 * Also optionally runs a lightweight heartbeat while online.
 */
export function useSyncWorker({
  networkOnline,
  onReconnect,
  onOnlineHeartbeat,
}: UseSyncWorkerArgs) {
  const wasOnlineRef = useRef<boolean>(networkOnline);

  useEffect(() => {
    const wasOnline = wasOnlineRef.current;
    if (!wasOnline && networkOnline) {
      void onReconnect();
    }
    if (networkOnline && onOnlineHeartbeat) {
      void onOnlineHeartbeat();
    }
    wasOnlineRef.current = networkOnline;
  }, [networkOnline, onReconnect, onOnlineHeartbeat]);
}

export default useSyncWorker;
