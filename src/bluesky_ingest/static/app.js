"use strict";
const $ = (id) => document.getElementById(id);
const node = (tag, text, className) => { const el = document.createElement(tag); if (text !== undefined) el.textContent = text; if (className) el.className = className; return el; };
const number = (n, decimals = 0) => Number(n).toLocaleString(undefined, {maximumFractionDigits: decimals});
const time = (s) => s ? new Date(s).toLocaleTimeString([], {hour: "2-digit", minute: "2-digit"}) : "Not yet";
const stamp = (s) => s ? new Date(s).toLocaleString() : "Not yet";
const endpoint = async (path) => { const response = await fetch(path, {cache: "no-store"}); if (!response.ok) throw new Error(response.status === 404 ? "This topic could not be found." : "Data is temporarily unavailable. Check pipeline readiness and try Refresh."); return response.json(); };
let busy = false, offset = 0, total = 0, currentSearch = "";
const topicId = location.pathname.split("/")[2];
function showError(error) { $("error").textContent = error.message; $("error").hidden = false; }
function demoMode(enabled) { $("demo-banner").hidden = !enabled; }
function renderPosts(items, container) {
  container.replaceChildren();
  if (!items.length) { container.append(node("p", "No available posts match this view. Deleted posts are excluded.", "empty")); return; }
  items.forEach((post) => {
    const article = node("article", undefined, "post");
    const meta = node("div", undefined, "post-meta");
    meta.append(node("span", post.author_did, "post-author"), node("span", stamp(post.indexed_at)));
    article.append(meta, node("p", post.text, "post-text"));
    if (post.original_url) { const a = node("a", "View original post ↗"); a.href = post.original_url; a.target = "_blank"; a.rel = "noopener noreferrer"; article.append(a); }
    else article.append(node("span", "Synthetic demonstration post", "muted"));
    container.append(article);
  });
}
async function pipeline() {
  const p = await endpoint("/api/v1/pipeline/status");
  $("connection").textContent = p.mode === "demo" ? "Demo workspace" : p.connection_state;
  demoMode(p.mode === "demo");
  $("connection").className = "badge" + (p.connection_state === "failed" ? " failed" : ["backoff", "connecting"].includes(p.connection_state) ? " warning" : "");
  if ($("event-rate")) {
    $("event-rate").textContent = number(p.events_per_second, 1);
    $("lag").textContent = p.processing_lag_seconds === null ? "—" : `${number(p.processing_lag_seconds * 1000)} ms`;
    $("event-count").textContent = `${number(p.events_received)} / ${number(p.events_persisted)}`;
    $("queue").textContent = `${p.queue_depth} / ${p.queue_capacity}`;
    $("health-state").textContent = p.connection_state;
    $("retries").textContent = number(p.retries);
    $("errors").textContent = number(p.errors);
    $("last-event").textContent = time(p.last_event_at);
    $("detector-state").textContent = p.detection_error || (p.last_detection_at ? "Ready" : "Warming up");
  }
}
async function overview() {
  const data = await endpoint("/api/v1/topics/trending");
  demoMode(data.demo); $("topic-count").textContent = data.items.length;
  $("window-label").textContent = `Latest completed ${data.window_minutes}-minute window · ${time(data.window_start)} · keeping ${data.retention_hours} hours of posts`;
  $("updated").textContent = `Last detected ${time(data.last_updated)}`;
  const table = $("topics"); table.replaceChildren();
  if (!data.items.length) { const row = node("tr"); const cell = node("td", data.baseline_ready ? "No qualifying spikes in the latest completed window." : `Building the ${data.baseline_minutes_required}-minute baseline. Rankings appear after enough source history has been observed.`, "empty"); cell.colSpan = 5; row.append(cell); table.append(row); }
  data.items.forEach(({topic, metric}) => {
    const row = node("tr"); const cell = node("td"); const link = node("a", topic.label, "topic-link"); link.href = `/topics/${topic.id}`;
    cell.append(link, node("span", metric.persistence, "topic-sub")); row.append(cell, node("td", number(metric.post_count)), node("td", number(metric.baseline_expected_count, 1)));
    const growth = node("td"); growth.append(node("span", `${number(metric.rate_ratio, 1)}×`, "growth")); row.append(growth, node("td", number(metric.spike_score, 1))); table.append(row);
  });
  if (currentSearch) await searchPosts(currentSearch);
}
function renderChart(items) {
  const container = $("timeline"); container.replaceChildren(); $("timeline-table").replaceChildren();
  if (!items.length) { container.append(node("p", "No completed-window metrics yet.", "empty")); return; }
  const NS = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(NS, "svg"); const width = Math.max(330, container.clientWidth - 20); svg.setAttribute("viewBox", `0 0 ${width} 250`); svg.setAttribute("role", "img"); svg.setAttribute("aria-label", "Observed post volume and recent baseline over completed time windows. Values are available in the timeline data table below.");
  const add = (tag, attrs, text) => { const el = document.createElementNS(NS, tag); Object.entries(attrs).forEach(([key, value]) => el.setAttribute(key, value)); if (text !== undefined) el.textContent = text; svg.append(el); return el; };
  const max = Math.max(1, ...items.flatMap((m) => [m.post_count, m.baseline_expected_count]));
  const step = (width - 90) / items.length;
  [0, .5, 1].forEach((fraction) => { const y = 205 - fraction * 165; add("line", {x1: 50, x2: width - 25, y1: y, y2: y, stroke: "#e6ecea", "stroke-dasharray": "4 4"}); add("text", {x: 37, y: y + 4, "text-anchor": "end"}, number(max * fraction)); });
  let points = [];
  items.forEach((m, i) => {
    const x = 55 + step * i, height = m.post_count / max * 165;
    add("rect", {x, y: 205 - height, width: Math.max(step - 12, 3), height, rx: 4, fill: i === items.length - 1 ? "#137e6f" : "#9bd0c1"});
    points.push(`${x + (step - 12) / 2},${205 - m.baseline_expected_count / max * 165}`);
    const stride = Math.max(1, Math.ceil(items.length / (width < 500 ? 3 : 6)));
    if (i === 0 || i === items.length - 1 || (i % stride === 0 && items.length - 1 - i >= stride)) add("text", {x: x + step / 2 - 6, y: 235, "text-anchor": "middle"}, time(m.window_start));
    const row = node("tr"); [stamp(m.window_start), number(m.post_count), number(m.baseline_expected_count, 1), number(m.spike_score, 1)].forEach((value) => row.append(node("td", value))); $("timeline-table").append(row);
  });
  add("polyline", {points: points.join(" "), stroke: "#7c9790", fill: "none", "stroke-width": 2, "stroke-dasharray": "6 4"}); container.append(svg);
}
async function evidence() {
  const data = await endpoint(`/api/v1/topics/${topicId}/posts?limit=10&offset=${offset}`); total = data.total;
  if (offset && !data.items.length) { offset = 0; return evidence(); }
  renderPosts(data.items, $("posts")); $("evidence-count").textContent = `${number(total)} available posts in the scored window`;
  $("page-info").textContent = total ? `${offset + 1}–${Math.min(offset + 10, total)} of ${total}` : "No evidence";
  $("previous").disabled = offset === 0; $("next").disabled = offset + 10 >= total;
}
async function detail() {
  const [data, timeline] = await Promise.all([endpoint(`/api/v1/topics/${topicId}`), endpoint(`/api/v1/topics/${topicId}/timeline`)]);
  $("topic-title").textContent = data.topic.label; document.title = `${data.topic.label} · Signal Watch`; demoMode(data.demo);
  $("terms").replaceChildren(...data.topic.terms.map((term) => node("span", term))); $("stale").hidden = !data.data_stale;
  const m = data.latest_metric;
  if (m) {
    $("explanation").textContent = m.explanation; $("updated").textContent = `Detected ${time(m.detected_at)}`;
    $("current-count").textContent = number(m.post_count); $("baseline").textContent = number(m.baseline_expected_count, 1);
    $("growth").textContent = m.score_components.baseline_ready === false ? "—" : `${number(m.rate_ratio, 1)}×`; $("increase").textContent = `Absolute increase: ${number(m.absolute_increase, 1)}`;
    $("persistence").textContent = m.persistence; $("sample-label").textContent = m.score_components.baseline_ready === false ? "Building baseline history" : m.sample_sufficient ? `Completed ${m.window_minutes}-minute window` : "Sample too small for a signal";
    const components = $("score-components"); components.replaceChildren();
    const c = m.score_components;
    [["Spike score", number(m.spike_score, 1)], ["Smoothing α", c.alpha], ["Baseline floor", c.baseline_floor], ["Minimum volume", c.min_current_count], ["Minimum growth", `${c.min_rate_ratio}×`], ["Consecutive windows", c.consecutive_spike_windows || 0]].forEach(([label, value]) => { const row = node("div"); row.append(node("dt", label), node("dd", String(value))); components.append(row); });
  } else {
    $("explanation").textContent = "Not enough completed-window evidence for a score yet.";
    ["current-count", "baseline", "growth", "persistence"].forEach(id => $(id).textContent = "—");
    $("score-components").replaceChildren(); $("sample-label").textContent = "No current scored sample";
    $("increase").textContent = "Absolute increase: —"; $("updated").textContent = "No current detection";
  }
  renderChart(timeline.items); await evidence();
}
async function refresh() {
  if (busy) return; busy = true; $("refresh").disabled = true;
  try { await Promise.all([pipeline(), document.body.dataset.page === "topic" ? detail() : overview()]); $("error").hidden = true; }
  catch (error) { showError(error); if (error.message.includes("could not be found") && $("topic-title")) $("topic-title").textContent = "Topic unavailable"; }
  finally { busy = false; $("refresh").disabled = false; }
}
$("refresh").addEventListener("click", refresh);
if ($("next")) { $("next").addEventListener("click", async () => { offset += 10; try { await evidence(); } catch (error) { showError(error); } }); $("previous").addEventListener("click", async () => { offset = Math.max(0, offset - 10); try { await evidence(); } catch (error) { showError(error); } }); }
async function searchPosts(q) {
  const data = await endpoint(`/api/v1/search?q=${encodeURIComponent(q)}`);
  renderPosts(data.items, $("search-results"));
  $("search-results").prepend(node("p", `${data.total} available matches · showing up to 20`, "muted"));
}
if ($("search-form")) $("search-form").addEventListener("submit", async (event) => {
  event.preventDefault(); const q = $("search").value.trim(); if (!q) return;
  currentSearch = q; $("search-results").textContent = "Searching…";
  try { await searchPosts(q); } catch (error) { $("search-results").textContent = error.message; }
});
document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
refresh(); setInterval(() => { if (!document.hidden) refresh(); }, 15000);
