import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { chromium } from "playwright";

export async function checkUI(browser, base = "http://127.0.0.1:8000", directory = "test-results") {
  await mkdir(directory, { recursive: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  const mode = await (await page.request.get(`${base}/api/v1/pipeline/status`)).json();
  assert.equal(mode.mode, "demo", "UI checks require explicitly labeled synthetic demo data");
  await page.goto(base);
  await page.getByRole("link", { name: "#aurora", exact: true }).waitFor();
  assert.equal(await page.locator("#demo-banner").isVisible(), true);
  assert.match(await page.locator("#topics").innerText(), /24[\s\S]*4[\s\S]*4\.3×/);
  await page.getByRole("button", { name: /Refresh/ }).click();
  await page.getByRole("button", { name: /Refresh/ }).waitFor({ state: "visible" });
  await page.waitForFunction(() => !document.getElementById("refresh").disabled);
  await page.screenshot({ path: `${directory}/overview.png`, fullPage: true });
  await page.screenshot({ path: `${directory}/overview-viewport.png` });
  await page.getByRole("link", { name: "#aurora", exact: true }).click();
  await page.waitForFunction(() => document.getElementById("current-count").textContent === "24");
  assert.equal(await page.locator("#timeline svg").count(), 1);
  await page.screenshot({ path: `${directory}/topic.png`, fullPage: true });
  await page.screenshot({ path: `${directory}/topic-viewport.png` });
  assert.match(await page.locator("#page-info").innerText(), /1–10 of 24/);
  await page.getByRole("button", { name: "Next →" }).click();
  await page.waitForFunction(() => document.getElementById("page-info").textContent.startsWith("11–20"));
  await page.getByRole("button", { name: "← Previous" }).click();
  await page.waitForFunction(() => document.getElementById("previous").disabled);
  await page.getByText("View timeline data", { exact: true }).click();
  assert.equal(await page.locator("#timeline-table").isVisible(), true);
  await page.getByText("View timeline data", { exact: true }).click();
  // Source text must not become executable markup.
  await page.route("**/api/v1/topics/*/posts?*", async route => {
    const response = await route.fetch(); const data = await response.json();
    data.items[0].text = "<img src=x onerror='window.injected=true'>";
    await route.fulfill({ response, json: data });
  });
  await page.getByRole("button", { name: /Refresh/ }).click();
  await page.getByText("<img src=x onerror='window.injected=true'>", { exact: true }).waitFor();
  assert.equal(await page.evaluate(() => window.injected), undefined);
  await page.unroute("**/api/v1/topics/*/posts?*");
  await page.getByRole("link", { name: "← Back to overview" }).click();
  await page.getByLabel("Search posts", { exact: true }).fill("aurora");
  await page.getByRole("button", { name: "Search posts", exact: true }).click();
  await page.waitForFunction(() => document.querySelectorAll("#search-results .post").length > 0);
  await page.getByLabel("Search posts", { exact: true }).fill("no-such-topic-xyz");
  await page.getByRole("button", { name: "Search posts", exact: true }).click();
  await page.getByText("No available posts match this view. Deleted posts are excluded.", { exact: true }).waitFor();
  await page.route("**/api/v1/topics/trending", route => route.fulfill({ status: 503, json: {detail: "Unavailable"} }));
  await page.getByRole("button", { name: /Refresh/ }).click();
  await page.locator("#error").waitFor({state: "visible"});
  await page.screenshot({ path: `${directory}/error.png`, fullPage: true });
  await page.unroute("**/api/v1/topics/trending");
  await page.getByRole("button", { name: /Refresh/ }).click();
  await page.locator("#error").waitFor({state: "hidden"});
  await page.goto(`${base}/topics/999999`);
  await page.getByText("This topic could not be found.", { exact: true }).waitFor();
  await page.getByRole("link", { name: "← Back to overview" }).click();
  await page.getByRole("link", { name: "API reference", exact: false }).click();
  await page.waitForURL("**/docs");
  const mobile = await browser.newContext({viewport: {width: 390, height: 844}, isMobile: true, hasTouch: true});
  const phone = await mobile.newPage();
  await phone.goto(base);
  await phone.getByRole("link", {name: "#aurora", exact: true}).waitFor();
  assert.equal(await phone.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  await phone.screenshot({path: `${directory}/mobile-overview.png`, fullPage: true});
  await phone.screenshot({path: `${directory}/mobile-overview-viewport.png`});
  await phone.getByRole("link", {name: "#aurora", exact: true}).click();
  await phone.locator("#timeline svg").waitFor();
  assert.equal(await phone.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  await phone.screenshot({path: `${directory}/mobile-topic.png`, fullPage: true});
  await phone.screenshot({path: `${directory}/mobile-topic-viewport.png`});
  assert.deepEqual(errors, []);
  await mobile.close(); await context.close();
  return "UI passed: overview, detail, refresh, evidence pagination, timeline table, search, empty/error recovery, missing topic, API docs, XSS text rendering, desktop/mobile fit.";
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const browser = await chromium.launch({headless: true});
  try { console.log(await checkUI(browser, process.env.UI_BASE_URL)); }
  finally { await browser.close(); }
}
