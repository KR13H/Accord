import axios from "axios";
import { cacheJsonResponse, readJsonResponse } from "../services/OfflineSync";

declare module "axios" {
  interface AxiosRequestConfig {
    offlineCacheKey?: string;
    offlineCacheTtlMs?: number;
  }

  interface InternalAxiosRequestConfig {
    offlineCacheKey?: string;
    offlineCacheTtlMs?: number;
  }
}

const MOBILE_BACKEND_BASE_URL = process.env.EXPO_PUBLIC_API_BASE_URL || "http://192.168.1.10:8000/api/v1";

const mobileApi = axios.create({
  baseURL: MOBILE_BACKEND_BASE_URL,
  timeout: 12000,
});

type OfflineBannerState = {
  visible: boolean;
  message: string;
};

let offlineBannerState: OfflineBannerState = {
  visible: false,
  message: "",
};

const offlineBannerListeners = new Set<(state: OfflineBannerState) => void>();

function publishOfflineBanner(nextState: OfflineBannerState) {
  offlineBannerState = nextState;
  offlineBannerListeners.forEach((listener) => listener(offlineBannerState));
}

export function subscribeOfflineBanner(listener: (state: OfflineBannerState) => void): () => void {
  offlineBannerListeners.add(listener);
  listener(offlineBannerState);
  return () => {
    offlineBannerListeners.delete(listener);
  };
}

mobileApi.interceptors.request.use((config) => {
  config.headers = config.headers ?? {};
  config.headers["X-Role"] = "admin";
  config.headers["X-Admin-Id"] = "1";
  return config;
});

mobileApi.interceptors.response.use(
  async (response) => {
    const config = response.config;
    const method = String(config.method || "get").toLowerCase();
    const cacheKey = String(config.offlineCacheKey || "").trim();

    if (method === "get" && cacheKey) {
      await cacheJsonResponse(cacheKey, response.data, config.offlineCacheTtlMs);
    }

    if (offlineBannerState.visible) {
      publishOfflineBanner({ visible: false, message: "" });
    }

    return response;
  },
  async (error) => {
    const config = error?.config;
    const method = String(config?.method || "get").toLowerCase();
    const cacheKey = String(config?.offlineCacheKey || "").trim();
    const message = String(error?.message || "").toLowerCase();
    const isTimeout = error?.code === "ECONNABORTED" || message.includes("timeout");
    const isNetworkFailure = !error?.response || message.includes("network error") || message.includes("socket");

    if (method === "get" && cacheKey && (isTimeout || isNetworkFailure)) {
      const cachedPayload = await readJsonResponse<unknown>(cacheKey);
      if (cachedPayload !== null) {
        publishOfflineBanner({
          visible: true,
          message: "You are offline - Viewing cached data",
        });

        return {
          data: cachedPayload,
          status: 200,
          statusText: "OK",
          headers: { "x-offline-cache": "hit" },
          config,
          request: error?.request,
        };
      }
    }

    return Promise.reject(error);
  }
);

export default mobileApi;
