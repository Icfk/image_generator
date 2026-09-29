// Persistence contract shared by the UI and dependency-free Node tests.
export class RequestStore {
  constructor(storage, userId, newKey = () => crypto.randomUUID()) {
    this.storage = storage;
    this.slot = `content-lab:request:${userId}`;
    this.newKey = newKey;
  }

  read() {
    const value = this.storage.getItem(this.slot);
    return value ? JSON.parse(value) : null;
  }

  prepare(kind, payload) {
    const existing = this.read();
    if (existing) return existing;
    const request = { key: this.newKey(), kind, payload, taskId: null };
    // Persist BEFORE HTTP. Storage failure stops submission instead of losing the key.
    this.storage.setItem(this.slot, JSON.stringify(request));
    return request;
  }

  accepted(taskId) {
    const request = this.read();
    if (!request) throw new Error("Не найден ключ исходного запроса");
    request.taskId = taskId;
    this.storage.setItem(this.slot, JSON.stringify(request));
    return request;
  }

  reset() {
    this.storage.removeItem(this.slot);
  }
}

export function safeResultURL(value, base) {
  if (typeof value !== "string") return null;
  try {
    const url = new URL(value, base);
    return url.protocol === "https:" ||
      (url.origin === new URL(base).origin &&
        url.pathname === "/static/mock-result.svg")
      ? url.href
      : null;
  } catch {
    return null;
  }
}
