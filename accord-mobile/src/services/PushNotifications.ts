import { Platform } from "react-native";
import messaging from "@react-native-firebase/messaging";

type RegisterPushArgs = {
  apiBaseUrl: string;
  headers?: Record<string, string>;
  userId?: number;
  appVersion?: string;
};

export type RegisterPushResult = {
  status: "registered" | "skipped" | "failed";
  detail: string;
  token?: string;
};

function hasPermission(status: number): boolean {
  // 1=AUTHORIZED, 2=PROVISIONAL in RN Firebase messaging.
  return status === 1 || status === 2;
}

export async function registerPushNotifications(args: RegisterPushArgs): Promise<RegisterPushResult> {
  if (Platform.OS !== "ios" && Platform.OS !== "android") {
    return { status: "skipped", detail: "Push notifications only supported on iOS/Android." };
  }

  try {
    const permissionStatus = await messaging().requestPermission();
    if (!hasPermission(permissionStatus)) {
      return { status: "skipped", detail: "Notification permission was denied by the user." };
    }

    await messaging().registerDeviceForRemoteMessages();
    const token = await messaging().getToken();
    if (!token) {
      return { status: "failed", detail: "FCM token retrieval returned an empty value." };
    }

    const response = await fetch(`${args.apiBaseUrl}/api/v1/users/device-token`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(args.headers || {}),
      },
      body: JSON.stringify({
        user_id: args.userId,
        device_token: token,
        platform: Platform.OS,
        app_version: args.appVersion || "dev-local",
      }),
    });

    if (!response.ok) {
      const body = await response.text();
      return {
        status: "failed",
        detail: `Backend device-token registration failed: HTTP ${response.status} ${body}`,
      };
    }

    return { status: "registered", detail: "FCM token registered with backend.", token };
  } catch (error) {
    return { status: "failed", detail: `Push bootstrap failed: ${String(error)}` };
  }
}

export function subscribePushTokenRefresh(args: RegisterPushArgs): () => void {
  const unsubscribe = messaging().onTokenRefresh(async (token) => {
    try {
      await fetch(`${args.apiBaseUrl}/api/v1/users/device-token`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(args.headers || {}),
        },
        body: JSON.stringify({
          user_id: args.userId,
          device_token: token,
          platform: Platform.OS,
          app_version: args.appVersion || "dev-local",
        }),
      });
    } catch {
      // Non-fatal: app can retry token registration on next app launch.
    }
  });

  return unsubscribe;
}
