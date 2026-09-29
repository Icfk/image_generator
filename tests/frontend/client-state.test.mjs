import { test } from "node:test";
import assert from "node:assert/strict";
import { RequestStore, safeResultURL } from "../../app/static/client-state.js";

function memory() {
  const data = new Map();
  return {
    getItem: (key) => data.get(key) || null,
    setItem: (key, value) => data.set(key, value),
    removeItem: (key) => data.delete(key),
  };
}

test("unknown response and reload preserve key and original parameters", () => {
  const storage = memory();
  const first = new RequestStore(storage, "user", () => "key-1").prepare(
    "image",
    { prompt: "fox" },
  );
  const restarted = new RequestStore(storage, "user", () => "must-not-be-used");
  assert.deepEqual(restarted.prepare("video", { prompt: "different" }), first);
  restarted.accepted("task-1");
  assert.equal(restarted.prepare("image", {}).taskId, "task-1");
});

test("only explicit new-task reset creates a new key", () => {
  let counter = 0;
  const store = new RequestStore(memory(), "user", () => String(++counter));
  assert.equal(store.prepare("image", {}).key, "1");
  assert.equal(store.prepare("image", {}).key, "1");
  store.reset();
  assert.equal(store.prepare("image", {}).key, "2");
});

test("different users have independent pending requests", () => {
  const storage = memory();
  new RequestStore(storage, "a").prepare("image", {});
  assert.equal(new RequestStore(storage, "b").read(), null);
});

test("storage failure stops request preparation", () => {
  const storage = memory();
  storage.setItem = () => {
    throw new Error("quota exceeded");
  };
  assert.throws(
    () => new RequestStore(storage, "a").prepare("image", {}),
    /quota/,
  );
});

test("result URLs reject active content and allow only safe previews", () => {
  const base = "http://localhost:8000";
  for (const url of [
    "javascript:alert(1)",
    "data:text/html,evil",
    "http://evil.test/",
    null,
  ]) {
    assert.equal(safeResultURL(url, base), null);
  }
  assert.equal(
    safeResultURL("/static/mock-result.svg", base),
    base + "/static/mock-result.svg",
  );
  assert.equal(
    safeResultURL("https://result.test/x.png", base),
    "https://result.test/x.png",
  );
});
