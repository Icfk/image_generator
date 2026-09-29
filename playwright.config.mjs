import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/frontend",
  testMatch: "**/*.spec.mjs",
  fullyParallel: true,
  workers: 2,
  use: {
    baseURL: "http://127.0.0.1:8871",
    trace: "retain-on-failure",
    channel: process.env.PLAYWRIGHT_CHANNEL || undefined,
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command:
      ".venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8871",
    env: {
      APP_ENV: "test",
      GENERATION_PROVIDER: "mock",
      REQUIRE_WEBHOOK_SIGNATURE: "false",
    },
    url: "http://127.0.0.1:8871/health",
    reuseExistingServer: false,
  },
});
