"use strict";

const API = (window.CRAWLER && window.CRAWLER.apiPrefix) || "/api/v1";

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
const REDUCE = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

async function api(path, options) {
  const res = await fetch(API + path, options);
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = body.detail || JSON.stringify(body);
    } catch (_) {}
    throw new Error(detail);
  }
  return res.status === 204 ? null : res.json();
}

const fmt = (n) => (n == null ? "–" : Number(n).toLocaleString());
const esc = (s) =>
  String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));

// ---- Palette ------------------------------------------------------------
const C = {
  ink: "#eef2e9", dim: "#93a7a8", faint: "#5c7176", line: "rgba(29,54,64,0.75)",
  beacon: "#f5b13d", beaconLight: "#ffd27d", signal: "#4fd6c1", alert: "#ff6b5e",
  base0: "#0a1418",
};

// 2xx reads as signal, redirects as beacon, client errors amber, server errors coral.
function statusColor(s) {
  if (s == null) return C.faint;
  if (s < 300) return C.signal;
  if (s < 400) return C.beacon;
  if (s < 500) return "#ff9f45";
  return C.alert;
}
function statusBand(s) {
  if (s == null) return "unknown";
  if (s < 300) return "2xx ok";
  if (s < 400) return "3xx redirect";
  if (s < 500) return "4xx client";
  return "5xx server";
}

// ---- Numeric count-up ---------------------------------------------------
function animateCount(el, to) {
  const from = Number(el.dataset.count || 0);
  el.dataset.count = String(to);
  if (REDUCE || from === to) {
    el.textContent = fmt(to);
    return;
  }
  const start = performance.now();
  const dur = 600;
  const step = (now) => {
    const t = Math.min(1, (now - start) / dur);
    const eased = 1 - Math.pow(1 - t, 3);
    el.textContent = fmt(Math.round(from + (to - from) * eased));
    if (t < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}

// ---- Charts -------------------------------------------------------------
const charts = {};
const HAS_CHART = typeof Chart !== "undefined";
if (HAS_CHART) {
  Chart.defaults.color = C.dim;
  Chart.defaults.borderColor = C.line;
  Chart.defaults.font.family = '"IBM Plex Sans", ui-sans-serif, system-ui, sans-serif';
  Chart.defaults.font.size = 12;
}

function upsertChart(id, config) {
  if (!HAS_CHART) return;
  if (charts[id]) {
    charts[id].data = config.data;
    charts[id].options = config.options;
    charts[id].update();
  } else {
    charts[id] = new Chart($("#" + id).getContext("2d"), config);
  }
}

// ---- Frontier Scope (signature) ----------------------------------------
// Each crawled page is a node placed on a ring by its depth from the seed,
// angle is a stable hash of the URL, size grows with link-degree, colour
// tracks HTTP status. An amber beam sweeps like a sonar and lights nodes.
const scope = { nodes: [], maxDepth: 0, hasData: false, beam: 0, raf: 0, last: 0 };

function hash01(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0) / 4294967295;
}

function buildScope(docs) {
  const maxDepth = docs.reduce((m, d) => Math.max(m, d.depth || 0), 0);
  scope.maxDepth = maxDepth;
  scope.nodes = docs.map((d) => {
    const deg = (d.in_degree || 0) + (d.out_degree || 0);
    return {
      angle: hash01(d.url) * Math.PI * 2,
      depthN: maxDepth ? (d.depth || 0) / maxDepth : 0,
      jitter: (hash01(d.url + "#r") - 0.5),
      size: 2.4 + Math.min(9, Math.sqrt(deg) * 1.7),
      color: (d.depth || 0) === 0 ? C.beacon : statusColor(d.http_status),
      seed: (d.depth || 0) === 0,
    };
  });
  scope.hasData = docs.length > 0;

  $("#scope-reach").textContent = scope.hasData ? "d" + maxDepth : "–";
  $("#scope-empty").classList.toggle("hidden", scope.hasData);
  $("#scope-empty").classList.toggle("grid", !scope.hasData);

  const bands = new Map();
  docs.forEach((d) => {
    if ((d.depth || 0) === 0) return;
    const b = statusBand(d.http_status);
    bands.set(b, statusColor(d.http_status));
  });
  const items = ['<span><span style="display:inline-block;width:8px;height:8px;border-radius:9px;background:' +
    C.beacon + ';box-shadow:0 0 8px ' + C.beacon + ';vertical-align:middle"></span> seed</span>'];
  bands.forEach((color, band) => {
    items.push('<span><span style="display:inline-block;width:8px;height:8px;border-radius:9px;background:' +
      color + ';vertical-align:middle"></span> ' + esc(band) + "</span>");
  });
  $("#scope-legend").innerHTML = scope.hasData ? items.join("") : "";

  ensureScopeLoop();
}

function fitCanvas(canvas) {
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  if (w === 0 || h === 0) return null;
  if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) {
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
  }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w, h };
}

function drawScope(now) {
  const canvas = $("#scope");
  if (!canvas) return;
  const fit = fitCanvas(canvas);
  if (!fit) return;
  const { ctx, w, h } = fit;
  const cx = w / 2;
  const cy = h / 2;
  const maxR = Math.min(w, h) / 2 - 16;
  const innerR = maxR * 0.16;

  ctx.clearRect(0, 0, w, h);

  // Advance beam (time-based so it's frame-rate independent)
  if (!REDUCE) {
    const dt = scope.last ? (now - scope.last) / 1000 : 0;
    scope.beam = (scope.beam + dt * (Math.PI * 2) / 8) % (Math.PI * 2);
  } else {
    scope.beam = -Math.PI / 3;
  }
  scope.last = now;

  const rings = Math.max(1, scope.maxDepth);
  const ringGap = (maxR - innerR) / rings;

  // Radial spokes
  ctx.strokeStyle = "rgba(29,54,64,0.5)";
  ctx.lineWidth = 1;
  for (let i = 0; i < 12; i++) {
    const a = (i / 12) * Math.PI * 2;
    ctx.beginPath();
    ctx.moveTo(cx + Math.cos(a) * innerR, cy + Math.sin(a) * innerR);
    ctx.lineTo(cx + Math.cos(a) * maxR, cy + Math.sin(a) * maxR);
    ctx.stroke();
  }

  // Depth rings + labels
  ctx.textAlign = "left";
  ctx.font = '10px "IBM Plex Mono", monospace';
  for (let d = 0; d <= rings; d++) {
    const r = innerR + d * ringGap;
    ctx.beginPath();
    ctx.arc(cx, cy, r, 0, Math.PI * 2);
    ctx.strokeStyle = d === 0 ? "rgba(245,177,61,0.35)" : "rgba(29,54,64,0.9)";
    ctx.lineWidth = d === 0 ? 1.5 : 1;
    ctx.stroke();
    if (scope.hasData && d <= scope.maxDepth) {
      ctx.fillStyle = C.faint;
      ctx.fillText("d" + d, cx + 4, cy - r - 3);
    }
  }

  // Sweep — trailing wedge + leading beam
  if (scope.hasData) {
    ctx.save();
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, maxR, scope.beam - 0.6, scope.beam);
    ctx.closePath();
    ctx.fillStyle = "rgba(245,177,61,0.10)";
    ctx.fill();
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.lineTo(cx + Math.cos(scope.beam) * maxR, cy + Math.sin(scope.beam) * maxR);
    ctx.strokeStyle = "rgba(245,177,61,0.55)";
    ctx.lineWidth = 1.5;
    ctx.shadowColor = C.beacon;
    ctx.shadowBlur = 12;
    ctx.stroke();
    ctx.restore();
  }

  // Nodes
  for (const n of scope.nodes) {
    const r = n.seed ? innerR * 0.5 : innerR + (n.depthN * rings) * ringGap + n.jitter * ringGap * 0.55;
    const x = cx + Math.cos(n.angle) * r;
    const y = cy + Math.sin(n.angle) * r;

    let glow = 0;
    if (!REDUCE) {
      let delta = Math.abs(((n.angle - scope.beam) % (Math.PI * 2)));
      if (delta > Math.PI) delta = Math.PI * 2 - delta;
      glow = Math.max(0, 1 - delta / (Math.PI * 0.55));
    }

    if (glow > 0.05) {
      ctx.beginPath();
      ctx.arc(x, y, n.size + 6 * glow, 0, Math.PI * 2);
      ctx.fillStyle = n.color;
      ctx.globalAlpha = 0.16 * glow;
      ctx.fill();
      ctx.globalAlpha = 1;
    }
    ctx.beginPath();
    ctx.arc(x, y, n.size, 0, Math.PI * 2);
    ctx.fillStyle = n.color;
    ctx.globalAlpha = 0.55 + 0.45 * glow;
    ctx.fill();
    ctx.globalAlpha = 1;
  }

  // Seed core pulse
  const pulse = REDUCE ? 0.5 : (Math.sin(now / 600) + 1) / 2;
  ctx.beginPath();
  ctx.arc(cx, cy, 4 + pulse * 3, 0, Math.PI * 2);
  ctx.fillStyle = C.beacon;
  ctx.shadowColor = C.beacon;
  ctx.shadowBlur = 16;
  ctx.fill();
  ctx.shadowBlur = 0;
}

function scopeTick(now) {
  scope.raf = 0;
  const canvas = $("#scope");
  const visible = canvas && canvas.clientWidth > 0 && activeTab === "overview" && !document.hidden;
  if (visible) drawScope(now);
  else scope.last = 0;
  if (!REDUCE && scope.hasData) scope.raf = requestAnimationFrame(scopeTick);
}

function ensureScopeLoop() {
  if (REDUCE) {
    requestAnimationFrame(drawScope);
    return;
  }
  if (!scope.raf && scope.hasData) scope.raf = requestAnimationFrame(scopeTick);
}

// ---- Health -------------------------------------------------------------
async function refreshHealth() {
  const dot = $("#health-dot");
  const text = $("#health-text");
  const rdot = $("#rail-dot");
  const rtext = $("#rail-health");
  try {
    const h = await (await fetch("/health")).json();
    dot.style.background = C.signal;
    dot.style.boxShadow = "0 0 8px " + C.signal;
    text.textContent = "online · v" + h.version;
    rdot.style.background = C.signal;
    rtext.textContent = "signal locked";
  } catch (_) {
    dot.style.background = C.alert;
    dot.style.boxShadow = "none";
    text.textContent = "offline — can't reach the crawler";
    rdot.style.background = C.alert;
    rtext.textContent = "no signal";
  }
}

// ---- Overview -----------------------------------------------------------
async function loadOverview() {
  // Load each panel independently so one failing request can't blank the whole
  // page. `limit` must stay within the route's cap (<= 200) or it 422s.
  const [statsR, domainsR, pageR] = await Promise.allSettled([
    api("/stats"),
    api("/domains?limit=8"),
    api("/documents?limit=200"),
  ]);

  if (statsR.status === "fulfilled") {
    const stats = statsR.value;
    animateCount($("#stat-documents"), stats.documents);
    animateCount($("#stat-edges"), stats.edges);
    animateCount($("#stat-domains"), stats.domains);
    animateCount($("#stat-dupes"), stats.near_duplicates);

    const statuses = stats.by_status || [];
    upsertChart("status-chart", {
      type: "doughnut",
      data: {
        labels: statuses.map((s) => (s.status == null ? "none" : String(s.status))),
        datasets: [{
          data: statuses.map((s) => s.count),
          backgroundColor: statuses.map((s) => statusColor(s.status)),
          borderColor: C.base0,
          borderWidth: 2,
        }],
      },
      options: {
        cutout: "68%",
        plugins: { legend: { position: "right", labels: { usePointStyle: true, boxWidth: 8, padding: 14 } } },
      },
    });
  }

  if (pageR.status === "fulfilled") {
    buildScope(pageR.value.items || []);
  }

  if (domainsR.status === "fulfilled") {
    const domains = domainsR.value;
    upsertChart("domains-chart", {
      type: "bar",
      data: {
        labels: domains.map((d) => d.registered_domain),
        datasets: [{
          data: domains.map((d) => d.documents),
          backgroundColor: domains.map((_, i) => (i === 0 ? C.beacon : C.signal)),
          borderRadius: 5,
          maxBarThickness: 22,
        }],
      },
      options: {
        indexAxis: "y",
        plugins: { legend: { display: false } },
        scales: {
          x: { beginAtZero: true, ticks: { precision: 0 }, grid: { color: C.line } },
          y: { grid: { display: false } },
        },
      },
    });
  }

  for (const r of [statsR, domainsR, pageR]) {
    if (r.status === "rejected") console.error("overview: a panel failed to load", r.reason);
  }
}

// ---- Crawls -------------------------------------------------------------
const STATUS_STYLES = {
  running: "text-signal",
  completed: "text-signal",
  pending: "text-ink-dim",
  stopped: "text-beacon",
  failed: "text-alert",
};

// Crawl timestamps arrive as UTC (ISO with offset); render them in IST so the
// "Started" column matches the operator's wall clock.
function fmtIST(iso) {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "–";
  return (
    d.toLocaleString("en-IN", {
      timeZone: "Asia/Kolkata",
      day: "2-digit",
      month: "short",
      hour: "2-digit",
      minute: "2-digit",
      hour12: true,
    }) + " IST"
  );
}

async function loadCrawls() {
  const crawls = await api("/crawls?limit=100");
  const body = $("#crawls-body");
  $("#crawls-empty").classList.toggle("hidden", crawls.length > 0);
  body.innerHTML = crawls
    .map((c) => {
      const tone = STATUS_STYLES[c.status] || "text-ink-dim";
      const dot =
        c.status === "failed" ? C.alert
        : c.status === "pending" ? C.faint
        : c.status === "stopped" ? C.beacon
        : C.signal;
      const created = c.created_at ? fmtIST(c.created_at) : "–";
      const canStop = c.status === "running" || c.status === "pending";
      const action = canStop
        ? `<button data-stop="${c.id}" class="rounded-md border border-alert/40 px-2.5 py-1 text-xs font-medium text-alert transition hover:bg-alert/10 focus:outline-none focus:ring-2 focus:ring-alert/50">Stop</button>`
        : "";
      return `<tr class="row-link">
        <td class="px-4 py-3 font-mono text-ink-faint">#${c.id}</td>
        <td class="px-4 py-3 font-medium">${esc(c.name)}</td>
        <td class="px-4 py-3">
          <span class="inline-flex items-center gap-2 ${tone}">
            <span class="h-1.5 w-1.5 rounded-full" style="background:${dot}"></span>${esc(c.status)}
          </span>
        </td>
        <td class="px-4 py-3 font-mono tnum">${c.max_depth}</td>
        <td class="px-4 py-3 font-mono tnum text-ink-dim">${fmt(c.max_pages)}</td>
        <td class="px-4 py-3 font-mono tnum">${fmt(c.documents)}</td>
        <td class="px-4 py-3 text-ink-dim">${esc(created)}</td>
        <td class="px-4 py-3 text-right">${action}</td>
      </tr>`;
    })
    .join("");
}

async function stopCrawl(id) {
  try {
    await api(`/crawls/${id}/stop`, { method: "POST" });
  } catch (e) {
    console.error("stop crawl failed", e);
  }
  loadCrawls();
}

// ---- Documents ----------------------------------------------------------
const docState = { limit: 25, offset: 0, q: "", domain: "" };

async function loadDocuments() {
  const params = new URLSearchParams({ limit: docState.limit, offset: docState.offset });
  if (docState.q) params.set("q", docState.q);
  if (docState.domain) params.set("domain", docState.domain);
  const page = await api("/documents?" + params.toString());

  const body = $("#docs-body");
  $("#docs-empty").classList.toggle("hidden", page.items.length > 0);
  body.innerHTML = page.items
    .map((d) => {
      const sc = statusColor(d.http_status);
      return `<tr class="row-link cursor-pointer" data-doc="${d.id}">
        <td class="px-4 py-3 font-mono text-ink-faint">${d.id}</td>
        <td class="px-4 py-3">
          <div class="font-medium truncate max-w-md">${esc(d.title || "(untitled)")}</div>
          <div class="font-mono text-xs text-ink-faint truncate max-w-md">${esc(d.url)}</div>
        </td>
        <td class="px-4 py-3 font-mono text-ink-dim">${esc(d.registered_domain)}</td>
        <td class="px-4 py-3 font-mono tnum" style="color:${sc}">${d.http_status == null ? "–" : d.http_status}</td>
        <td class="px-4 py-3 font-mono tnum">${d.in_degree}</td>
        <td class="px-4 py-3 font-mono tnum">${d.out_degree}</td>
      </tr>`;
    })
    .join("");

  const from = page.total === 0 ? 0 : page.offset + 1;
  const to = Math.min(page.offset + page.limit, page.total);
  $("#docs-count").textContent = `${from}–${to} of ${fmt(page.total)}`;
  $("#docs-prev").disabled = page.offset <= 0;
  $("#docs-next").disabled = to >= page.total;
}

async function openDocument(id) {
  const modal = $("#doc-modal");
  const el = $("#doc-detail");
  el.innerHTML = '<div class="text-ink-dim">Loading…</div>';
  openModal("doc-modal");
  try {
    const d = await api("/documents/" + id);
    const field = (label, value) =>
      `<div><dt class="eyebrow">${label}</dt><dd class="mt-1 font-mono">${value}</dd></div>`;
    const links = (d.links || [])
      .map(
        (l) => `<li class="py-1.5 border-b border-line last:border-0">
          <a href="${esc(l.url)}" target="_blank" rel="noopener" class="font-mono text-signal hover:underline break-all">${esc(l.url)}</a>
          ${l.anchor ? `<span class="text-ink-faint"> — ${esc(l.anchor)}</span>` : ""}
        </li>`
      )
      .join("");
    const sc = statusColor(d.http_status);
    el.innerHTML = `
      <div>
        <a href="${esc(d.url)}" target="_blank" rel="noopener" class="font-mono text-signal hover:underline break-all">${esc(d.url)}</a>
        <h3 class="font-display text-lg font-medium mt-1">${esc(d.title || "(untitled)")}</h3>
      </div>
      <dl class="grid grid-cols-2 sm:grid-cols-3 gap-4">
        ${field("Host", esc(d.registered_domain))}
        ${field("Status", `<span style="color:${sc}">${d.http_status == null ? "–" : d.http_status}</span>`)}
        ${field("Content type", esc(d.content_type || "–"))}
        ${field("Depth", "d" + d.depth)}
        ${field("In-degree", d.in_degree)}
        ${field("Out-degree", d.out_degree)}
      </dl>
      <div>
        <div class="eyebrow mb-2">Outbound links · ${(d.links || []).length}</div>
        <ul class="text-sm max-h-64 overflow-auto">${links || '<li class="text-ink-faint py-1.5">No outbound links.</li>'}</ul>
      </div>`;
  } catch (e) {
    el.innerHTML = `<div class="text-alert">Couldn't load this page: ${esc(e.message)}</div>`;
  }
}

// ---- Graph --------------------------------------------------------------
async function loadGraph() {
  const g = await api("/graph?limit=15");
  const pages = g.top_pages || [];
  const allZero = pages.length > 0 && pages.every((p) => (p.in_degree || 0) === 0);
  $("#graph-hint").classList.toggle("hidden", !allZero);

  upsertChart("graph-chart", {
    type: "bar",
    data: {
      labels: pages.map((p) => shortUrl(p.url)),
      datasets: [{
        data: pages.map((p) => p.in_degree),
        backgroundColor: C.beacon,
        borderRadius: 5,
        maxBarThickness: 26,
      }],
    },
    options: {
      plugins: { legend: { display: false } },
      scales: {
        y: { beginAtZero: true, ticks: { precision: 0 }, grid: { color: C.line } },
        x: { grid: { display: false } },
      },
    },
  });
  $("#graph-list").innerHTML = pages
    .map(
      (p) => `<div class="flex items-center justify-between gap-4 py-2 text-sm">
        <a href="${esc(p.url)}" target="_blank" rel="noopener" class="font-mono text-signal hover:underline break-all">${esc(p.url)}</a>
        <span class="shrink-0 font-mono tnum text-ink-dim">${p.in_degree} in</span>
      </div>`
    )
    .join("");
}

function shortUrl(url) {
  try {
    const u = new URL(url);
    const path = u.pathname.length > 18 ? u.pathname.slice(0, 17) + "…" : u.pathname;
    return u.hostname + path;
  } catch (_) {
    return url.slice(0, 28);
  }
}

// ---- Tabs ---------------------------------------------------------------
const TAB_TITLES = { overview: "Overview", crawls: "Crawls", documents: "Documents", graph: "Link graph" };
const TAB_CRUMBS = { overview: "observatory", crawls: "runs", documents: "corpus", graph: "link graph" };
const TAB_LOADERS = { overview: loadOverview, crawls: loadCrawls, documents: loadDocuments, graph: loadGraph };
let activeTab = "overview";

function activateTab(tab) {
  activeTab = tab;
  $("#page-title").textContent = TAB_TITLES[tab] || tab;
  $("#crumb").textContent = TAB_CRUMBS[tab] || tab;
  $$("section[data-panel]").forEach((s) => s.classList.toggle("hidden", s.dataset.panel !== tab));
  $$(".nav-item, .nav-chip").forEach((b) => b.classList.toggle("is-active", b.dataset.tab === tab));
  if (tab === "overview") ensureScopeLoop();
  refresh();
}

async function refresh() {
  try {
    await TAB_LOADERS[activeTab]();
  } catch (e) {
    console.error("load failed", e);
  }
}

// ---- Modals -------------------------------------------------------------
function openModal(id) { $("#" + id).classList.remove("hidden"); }
function closeModal(id) { $("#" + id).classList.add("hidden"); }

async function submitCrawl(event) {
  event.preventDefault();
  const form = event.target;
  const msg = $("#crawl-form-msg");
  const seeds = form.seeds.value.split(/[\n,]+/).map((s) => s.trim()).filter(Boolean);
  const allowed = form.allowed_domains.value.split(/[\n,]+/).map((s) => s.trim()).filter(Boolean);
  const payload = {
    seeds,
    name: form.name.value || "crawl",
    max_depth: Number(form.max_depth.value),
    max_pages: Number(form.max_pages.value),
    allowed_domains: allowed.length ? allowed : null,
  };
  msg.textContent = "Sending to the frontier…";
  msg.className = "min-h-[1.25rem] text-sm text-ink-dim";
  try {
    const res = await api("/crawls", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (res.seeded > 0) {
      msg.textContent = `Crawl #${res.id} started — crawling ${res.seeded} seed(s). Watch it live below.`;
      msg.className = "min-h-[1.25rem] text-sm text-signal";
      form.reset();
      setTimeout(() => { closeModal("crawl-modal"); msg.textContent = ""; }, 1500);
      activateTab("crawls");
    } else {
      msg.textContent = "No valid seed URLs — check them and try again.";
      msg.className = "min-h-[1.25rem] text-sm text-alert";
    }
  } catch (e) {
    msg.textContent = "Couldn't start the crawl: " + e.message;
    msg.className = "min-h-[1.25rem] text-sm text-alert";
  }
}

// ---- Wire up ------------------------------------------------------------
let searchTimer;
function debounce(fn, ms) {
  return (...args) => { clearTimeout(searchTimer); searchTimer = setTimeout(() => fn(...args), ms); };
}

function init() {
  $$(".nav-item, .nav-chip").forEach((b) => b.addEventListener("click", () => activateTab(b.dataset.tab)));

  $("#new-crawl-btn").addEventListener("click", () => openModal("crawl-modal"));
  $$("[data-close]").forEach((b) =>
    b.addEventListener("click", (e) => closeModal(e.target.closest("[id$='-modal']").id))
  );
  $$("#crawl-modal, #doc-modal").forEach((m) =>
    m.addEventListener("click", (e) => { if (e.target === m) m.classList.add("hidden"); })
  );
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") $$("[id$='-modal']").forEach((m) => m.classList.add("hidden"));
  });
  $("#crawl-form").addEventListener("submit", submitCrawl);

  $("#docs-body").addEventListener("click", (e) => {
    const row = e.target.closest("[data-doc]");
    if (row) openDocument(row.dataset.doc);
  });

  $("#crawls-body").addEventListener("click", (e) => {
    const btn = e.target.closest("[data-stop]");
    if (btn) stopCrawl(btn.dataset.stop);
  });
  $("#docs-prev").addEventListener("click", () => { docState.offset = Math.max(0, docState.offset - docState.limit); loadDocuments(); });
  $("#docs-next").addEventListener("click", () => { docState.offset += docState.limit; loadDocuments(); });
  const onFilter = debounce(() => {
    docState.q = $("#doc-search").value.trim();
    docState.domain = $("#doc-domain").value.trim();
    docState.offset = 0;
    loadDocuments();
  }, 300);
  $("#doc-search").addEventListener("input", onFilter);
  $("#doc-domain").addEventListener("input", onFilter);

  window.addEventListener("resize", () => { if (activeTab === "overview") ensureScopeLoop(); });

  refreshHealth();
  activateTab("overview");
  setInterval(refreshHealth, 10000);
  setInterval(() => {
    if (activeTab === "overview") loadOverview();
    else if (activeTab === "crawls") loadCrawls();
  }, 5000);
}

document.addEventListener("DOMContentLoaded", init);
