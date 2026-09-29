import { RequestStore, safeResultURL } from "/static/client-state.js";

const $ = (id) => document.getElementById(id);
const state = {
  key: localStorage.getItem("apiKey") || "",
  store: null,
  timer: null,
  busy: false,
  epoch: 0,
  blockedUntil: 0,
  config: null,
};

function message(text) {
  $("form-message").textContent = text;
}
function stopPolling() {
  clearTimeout(state.timer);
  state.timer = null;
}

async function api(path, options = {}) {
  const epoch = state.epoch;
  const protectedRoute =
    path === "/balance" || path.startsWith("/generations/");
  if (protectedRoute && Date.now() < state.blockedUntil) {
    throw Object.assign(
      new Error("Лимит запросов: подождите до автоматического повтора."),
      { status: 429 },
    );
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 6500);
  try {
    const response = await fetch(path, {
      ...options,
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": state.key,
        ...options.headers,
      },
    });
    const body = await response.json().catch(() => ({}));
    if (epoch !== state.epoch)
      throw new Error("Аккаунт изменён; ответ проигнорирован");
    if (response.status === 429) {
      state.blockedUntil =
        Date.now() +
        Math.max(1, Number(response.headers.get("Retry-After")) || 60) * 1000;
    }
    if (!response.ok) {
      const detail = Array.isArray(body.detail)
        ? body.detail.map((item) => item.msg).join("; ")
        : body.detail;
      throw Object.assign(new Error(detail || `HTTP ${response.status}`), {
        status: response.status,
      });
    }
    return body;
  } catch (error) {
    if (error.name === "AbortError")
      throw new Error(
        "Ответ не получен. Повторите тот же запрос — ключ сохранён.",
      );
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function connected(on) {
  $("auth-card").classList.toggle("hidden", on);
  $("app-card").classList.toggle("hidden", !on);
  $("connection").textContent = on ? "Подключено" : "Не подключено";
  $("connection").className = `connection ${on ? "online" : "offline"}`;
}

function lockForm(locked) {
  for (const id of ["generation-mode", "prompt", "image-url", "webhook-url"])
    $(id).disabled = locked;
}

function modeChanged() {
  const withImage = $("generation-mode").value.startsWith("image-");
  $("image-source").classList.toggle("hidden", !withImage);
  $("image-url").required = withImage;
  $("prompt").maxLength = $("generation-mode").value.endsWith("video")
    ? 1500
    : 4000;
}

function clearResult() {
  stopPolling();
  $("result-link").classList.add("hidden");
  $("result-link").removeAttribute("href");
  $("error-box").classList.add("hidden");
  $("error-box").textContent = "";
  for (const id of ["image-preview", "video-preview"]) {
    $(id).classList.add("hidden");
    $(id).removeAttribute("src");
  }
  $("video-preview").load();
}

function restoreRequest() {
  const request = state.store.read();
  if (!request) return;
  $("generation-mode").value =
    `${request.payload.image_url ? "image" : "text"}-to-${request.kind}`;
  $("prompt").value = request.payload.prompt;
  $("image-url").value = request.payload.image_url || "";
  $("webhook-url").value = request.payload.webhook_url || "";
  modeChanged();
  lockForm(true);
  if (request.taskId) showTask(request.taskId, "queued");
  else {
    $("generate").textContent = "Повторить тот же запрос";
    message(
      "Есть запрос с неизвестным исходом. Повтор использует сохранённые параметры и ключ.",
    );
  }
}

async function refreshBalance() {
  const data = await api("/balance");
  $("balance").textContent = data.balance;
  $("user-id").textContent = data.external_user_id;
  return data;
}

async function connect() {
  const key = $("api-key").value.trim();
  if (!key) return;
  stopPolling();
  state.epoch += 1;
  state.key = key;
  state.blockedUntil = 0;
  state.busy = false;
  $("connect").disabled = true;
  try {
    const data = await refreshBalance();
    localStorage.setItem("apiKey", key);
    state.store = new RequestStore(localStorage, data.external_user_id);
    resetView();
    connected(true);
    restoreRequest();
  } catch (error) {
    $("new-user").classList.remove("hidden");
    $("new-user").textContent = error.message;
    connected(false);
  } finally {
    $("connect").disabled = false;
  }
}

async function createUser() {
  $("create-user").disabled = true;
  try {
    const user = await api("/auth", { method: "POST" });
    $("api-key").value = user.api_key;
    $("new-user").classList.remove("hidden");
    $("new-user").textContent = `Сохраните API-ключ: ${user.api_key}`;
    await connect();
  } catch (error) {
    $("new-user").classList.remove("hidden");
    $("new-user").textContent = error.message;
  } finally {
    $("create-user").disabled = false;
  }
}

async function generate() {
  if (state.busy || !state.store || !state.config) return;
  const epoch = state.epoch;
  state.busy = true;
  $("generate").disabled = true;
  message("");
  try {
    let request = state.store.read();
    if (!request) {
      const mode = $("generation-mode").value;
      const prompt = $("prompt").value.trim();
      const imageURL = mode.startsWith("image-")
        ? $("image-url").value.trim()
        : null;
      if (!prompt || prompt.length > $("prompt").maxLength)
        throw new Error("Проверьте длину промпта.");
      if (
        mode.startsWith("image-") &&
        (!imageURL || !/^https?:\/\//i.test(imageURL))
      ) {
        throw new Error(
          "Для этого режима нужен HTTP(S) URL исходного изображения.",
        );
      }
      request = state.store.prepare(
        mode.endsWith("video") ? "video" : "image",
        {
          prompt,
          image_url: imageURL,
          webhook_url: $("webhook-url").value.trim() || null,
        },
      );
    }
    lockForm(true);
    if (request.taskId) {
      showTask(request.taskId, "queued");
      return;
    }
    const result = await api(`/generations/${request.kind}s`, {
      method: "POST",
      headers: { "Idempotency-Key": request.key },
      body: JSON.stringify(request.payload),
    });
    if (epoch !== state.epoch) return;
    state.store.accepted(result.id);
    showTask(result.id, result.status);
  } catch (error) {
    if (epoch !== state.epoch) return;
    if ([400, 402, 422].includes(error.status)) {
      state.store.reset();
      lockForm(false);
    }
    message(error.message);
  } finally {
    if (epoch === state.epoch) {
      state.busy = false;
      const request = state.store.read();
      $("generate").disabled = Boolean(request?.taskId) || !state.config;
      $("generate").textContent =
        request && !request.taskId
          ? "Повторить тот же запрос"
          : "Запустить генерацию";
    }
  }
}

function updateStatus(status) {
  $("status-pill").textContent = status;
  $("status-pill").className =
    `pill ${status === "completed" ? "done" : status === "failed" ? "failed" : "active"}`;
  $("status-text").textContent =
    {
      created: "Задача создана",
      queued: "В очереди",
      processing: "Генерация выполняется",
      completed: "Готово",
      failed: "Генерация завершилась ошибкой",
    }[status] || status;
  $("progress").classList.toggle(
    "running",
    !["completed", "failed"].includes(status),
  );
}

function showTask(id, status) {
  clearResult();
  $("empty-result").classList.add("hidden");
  $("result").classList.remove("hidden");
  $("task-id").textContent = id;
  $("generate").disabled = true;
  $("new-task").classList.remove("hidden");
  updateStatus(status);
  poll(id, state.epoch);
}

async function poll(id, epoch) {
  if (epoch !== state.epoch || state.store?.read()?.taskId !== id) return;
  let delay = (state.config?.poll_interval_seconds || 12) * 1000;
  try {
    const task = await api(`/generations/${id}`);
    if (epoch !== state.epoch || state.store?.read()?.taskId !== id) return;
    updateStatus(task.status);
    if (["completed", "failed"].includes(task.status)) {
      const url = safeResultURL(task.result?.url, location.origin);
      if (url) {
        $("result-link").href = url;
        $("result-link").classList.remove("hidden");
        const preview = $(
          task.kind === "video" && !task.result.mock
            ? "video-preview"
            : "image-preview",
        );
        preview.src = url;
        preview.classList.remove("hidden");
      }
      if (task.error) {
        $("error-box").textContent = task.error;
        $("error-box").classList.remove("hidden");
      }
      if (task.result?.mock)
        $("status-text").textContent =
          "Mock завершён: локальная иллюстрация, не результат нейросети и не настоящее видео.";
      await refreshBalance().catch((error) => message(error.message));
      return;
    }
  } catch (error) {
    if (epoch !== state.epoch) return;
    message(error.message);
    if ([401, 404].includes(error.status)) return;
    delay = Math.max(delay, state.blockedUntil - Date.now() + 1000);
  }
  if (epoch === state.epoch)
    state.timer = setTimeout(() => poll(id, epoch), delay);
}

function resetView() {
  clearResult();
  message("");
  lockForm(false);
  $("result").classList.add("hidden");
  $("empty-result").classList.remove("hidden");
  $("status-pill").textContent = "Нет задачи";
  $("status-pill").className = "pill";
  $("new-task").classList.add("hidden");
  $("generate").disabled = !state.config;
  $("generate").textContent = "Запустить генерацию";
}

async function topUp() {
  const amount = Number($("topup-amount").value),
    key = $("topup-transaction").value;
  if (
    !state.config?.demo_payments ||
    !key.trim() ||
    !Number.isInteger(amount) ||
    amount < 1 ||
    amount > 1000000
  ) {
    message("Проверьте ID платежа и целую сумму от 1 до 1000000.");
    return;
  }
  $("topup").disabled = true;
  try {
    const result = await api("/webhooks/top-up", {
      method: "POST",
      body: JSON.stringify({
        external_user_id: $("user-id").textContent,
        amount,
        transaction_id: key,
      }),
    });
    message(
      result.duplicate ? "Этот платёж уже учтён." : "Тестовый баланс пополнен.",
    );
    await refreshBalance();
  } catch (error) {
    message(error.message);
  } finally {
    $("topup").disabled = false;
  }
}

$("create-user").onclick = createUser;
$("connect").onclick = connect;
$("generate").onclick = generate;
$("topup").onclick = topUp;
$("generation-mode").onchange = modeChanged;
$("new-task").onclick = () => {
  if (!state.busy) {
    state.store.reset();
    resetView();
  }
};
$("logout").onclick = () => {
  stopPolling();
  state.epoch += 1;
  state.busy = false;
  state.store = null;
  state.key = "";
  localStorage.removeItem("apiKey");
  $("api-key").value = "";
  $("new-user").textContent = "";
  resetView();
  connected(false);
};

try {
  state.config = await api("/client-config");
  const mock = state.config.provider === "mock";
  $("provider-name").textContent = mock ? "Mock" : "Fal.ai";
  $("provider-mode").textContent = mock
    ? "Без внешних вызовов"
    : "Платная генерация";
  $("mode-notice").textContent = mock
    ? "Бесплатное демо: генерация имитируется локально, Fal.ai не вызывается."
    : "Реальный провайдер Fal.ai: запуск расходует средства провайдера.";
  $("topup-card").classList.toggle("hidden", !state.config.demo_payments);
} catch {
  $("mode-notice").textContent =
    "Не удалось определить режим. Перезагрузите страницу перед запуском.";
  $("generate").disabled = true;
}
modeChanged();
if (state.key) {
  $("api-key").value = state.key;
  await connect();
}
