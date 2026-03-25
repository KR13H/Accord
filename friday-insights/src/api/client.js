import axios from "axios";
import { v4 as uuidv4 } from "uuid";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "/api/v1";

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
});

apiClient.interceptors.request.use((config) => {
  config.headers = config.headers || {};
  config.headers["X-Role"] = "admin";
  config.headers["X-Admin-Id"] = "1";

  const method = String(config.method || "get").toLowerCase();
  const url = String(config.url || "");
  const shouldIdempotent = method === "post" && ["/rera/allocations", "/bookings"].some((p) => url.includes(p));

  if (shouldIdempotent && !config.headers["Idempotency-Key"]) {
    config.headers["Idempotency-Key"] = uuidv4();
  }
  return config;
});

export default apiClient;
