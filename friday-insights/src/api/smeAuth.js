const SME_ROLE_KEY = "smeRole";
const SME_SESSION_KEY = "smeSessionToken";
const SME_USERNAME_KEY = "smePasskeyUsername";

export function getStoredSmeRole() {
  const role = window.localStorage.getItem(SME_ROLE_KEY);
  return role && role.trim() ? role.trim().toLowerCase() : "owner";
}

export function getStoredSmeSessionToken() {
  return window.localStorage.getItem(SME_SESSION_KEY) || "";
}

export function getStoredSmeUsername() {
  return window.localStorage.getItem(SME_USERNAME_KEY) || "";
}

export function persistSmeSession({ role, sessionToken, username }) {
  if (role) {
    window.localStorage.setItem(SME_ROLE_KEY, role.trim().toLowerCase());
  }
  if (sessionToken) {
    window.localStorage.setItem(SME_SESSION_KEY, sessionToken);
  }
  if (username) {
    window.localStorage.setItem(SME_USERNAME_KEY, username);
  }
}

export function clearSmeSession() {
  window.localStorage.removeItem(SME_SESSION_KEY);
}

export function buildSmeHeaders(extraHeaders = {}, overrides = {}) {
  const headers = { ...extraHeaders };
  const role = overrides.role || getStoredSmeRole();
  const sessionToken = overrides.sessionToken || getStoredSmeSessionToken();

  if (role) {
    headers["X-SME-Role"] = role;
  }
  if (sessionToken) {
    headers["X-SME-Session-Token"] = sessionToken;
  }

  return headers;
}