import { test, expect } from "@playwright/test";

async function setup(page, options = {}) {
  const calls = [];
  let first = true;
  await page.route("**/*", async (route) => {
    const request = route.request(),
      url = new URL(request.url());
    if (url.hostname !== "127.0.0.1") return route.abort();
    const reply = (body, status = 200, headers = {}) =>
      route.fulfill({ status, json: body, headers });
    if (url.pathname === "/client-config")
      return reply({
        provider: options.provider || "mock",
        demo_payments: !options.production,
        poll_interval_seconds: 12,
      });
    if (url.pathname === "/balance")
      return reply({ external_user_id: "user-1", balance: 5 });
    if (url.pathname === "/auth")
      return reply({ external_user_id: "user-1", api_key: "offline-key" });
    if (
      request.method() === "POST" &&
      url.pathname.startsWith("/generations/")
    ) {
      calls.push({
        key: request.headers()["idempotency-key"],
        payload: request.postDataJSON(),
        path: url.pathname,
      });
      if (options.unknown && first) {
        first = false;
        return route.abort("failed");
      }
      if (options.delay)
        await new Promise((resolve) => setTimeout(resolve, 100));
      return reply({ id: "task-1", status: "queued" }, 202);
    }
    if (url.pathname === "/generations/task-1") {
      if (options.limited)
        return reply({ detail: "Rate limit exceeded" }, 429, {
          "Retry-After": "60",
        });
      return reply({
        id: "task-1",
        kind: "image",
        status: "completed",
        result: { url: "/static/mock-result.svg", mock: true },
        error: null,
      });
    }
    if (url.pathname === "/webhooks/top-up")
      return reply({ duplicate: false, balance: 10 });
    return route.continue();
  });
  await page.goto("/");
  await expect(page.locator("#provider-name")).toHaveText(
    options.provider === "fal" ? "Fal.ai" : "Mock",
  );
  await page.locator("#api-key").fill("offline-key");
  await page.locator("#connect").click();
  await expect(page.locator("#app-card")).toBeVisible();
  return calls;
}

test("double-click submits once and new task clears old result", async ({
  page,
}) => {
  const calls = await setup(page, { delay: true });
  await page.locator("#prompt").fill("fox");
  await page.locator("#generate").evaluate((button) => {
    button.click();
    button.click();
  });
  await expect(page.locator("#status-pill")).toHaveText("completed");
  expect(calls).toHaveLength(1);
  expect(calls[0].key).toBeTruthy();
  await expect(page.locator("#image-preview")).toBeVisible();
  await page.locator("#new-task").click();
  await expect(page.locator("#result-link")).toBeHidden();
  await expect(page.locator("#image-preview")).toBeHidden();
  await expect(page.locator("#error-box")).toBeHidden();
});

test("network failure and reload reuse exact request and key", async ({
  page,
}) => {
  const calls = await setup(page, { unknown: true });
  await page.locator("#prompt").fill("original fox");
  await page.locator("#generate").click();
  await expect(page.locator("#generate")).toHaveText("Повторить тот же запрос");
  await page.reload();
  await expect(page.locator("#generate")).toHaveText("Повторить тот же запрос");
  await expect(page.locator("#prompt")).toHaveValue("original fox");
  await expect(page.locator("#prompt")).toBeDisabled();
  await page.locator("#generate").click();
  await expect(page.locator("#status-pill")).toHaveText("completed");
  expect(calls).toHaveLength(2);
  expect(calls[0]).toEqual(calls[1]);
  await page.reload();
  await expect(page.locator("#status-pill")).toHaveText("completed");
  expect(calls).toHaveLength(2);
});

for (const mode of [
  "text-to-image",
  "image-to-image",
  "text-to-video",
  "image-to-video",
]) {
  test(`mode ${mode} validates source and builds the right request`, async ({
    page,
  }) => {
    const calls = await setup(page);
    await page.locator("#generation-mode").selectOption(mode);
    await page.locator("#prompt").fill("fox");
    if (mode.startsWith("image-")) {
      await page.locator("#generate").click();
      await expect(page.locator("#form-message")).toContainText(
        "исходного изображения",
      );
      expect(calls).toHaveLength(0);
      await page.locator("#image-url").fill("https://input.test/image.png");
    }
    await page.locator("#generate").click();
    await expect(page.locator("#status-pill")).toHaveText("completed");
    expect(calls[0].path).toBe(
      mode.endsWith("video") ? "/generations/videos" : "/generations/images",
    );
    expect(calls[0].payload.image_url).toBe(
      mode.startsWith("image-") ? "https://input.test/image.png" : null,
    );
  });
}

test("production clearly shows provider and hides demo payments", async ({
  page,
}) => {
  await setup(page, { production: true, provider: "fal" });
  await expect(page.locator("#mode-notice")).toContainText(
    "расходует средства",
  );
  await expect(page.locator("#topup-card")).toBeHidden();
});

test("429 pauses polling instead of generating traffic every second", async ({
  page,
}) => {
  await page.clock.install();
  await setup(page, { limited: true });
  let reads = 0;
  page.on("request", (request) => {
    if (request.url().endsWith("/generations/task-1")) reads++;
  });
  await page.locator("#prompt").fill("fox");
  await page.locator("#generate").click();
  await expect(page.locator("#form-message")).toContainText("Rate limit");
  await page.clock.fastForward(59000);
  expect(reads).toBe(1);
  await page.clock.fastForward(3000);
  await expect.poll(() => reads).toBe(2);
});

test("mobile layout fits and has no JavaScript runtime errors", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await setup(page);
  await expect(page.locator("#generation-mode")).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  expect(errors).toEqual([]);
  await page.screenshot({ path: "test-results/mobile.png", fullPage: true });
});
