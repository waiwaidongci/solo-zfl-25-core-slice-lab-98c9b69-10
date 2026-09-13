import http from "node:http";
import { mkdir, readFile, writeFile, rename } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = join(__dirname, "data", "core-slices.json");
const port = Number(process.env.PORT || 3025);
const statuses = ["待切割", "制片中", "待观察", "已交付"];
const taskSteps = ["取样", "切割", "研磨", "染色", "观察"];
// 重开后切片回到“退回”，从这里继续制片旧流程
const sliceSteps = [...taskSteps, "退回"];
const inProgressSteps = ["取样", "切割", "研磨", "染色", "退回"];

const seed = {
  samples: [
    {
      id: "CORE-001",
      project: "东岭铜矿薄片",
      borehole: "ZK-17",
      coreBox: "BX-09",
      depth: "128.4-128.8m",
      owner: "陆川",
      status: "制片中",
      delivery: "未交付",
      slices: [
        { id: "SL-001-A", method: "茜素红染色", observation: "", status: "研磨", logs: [{ at: "2026-06-12T10:00:00.000Z", step: "取样", note: "截取含矿化条带位置" }, { at: "2026-06-13T11:20:00.000Z", step: "切割", note: "完成粗切" }] }
      ]
    }
  ]
};

// ---- 存储：整文件 JSON，tmp+rename 原子落盘，重启后归档仍在 ----
async function loadDb() {
  if (!existsSync(dbPath)) {
    await mkdir(dirname(dbPath), { recursive: true });
    await writeFile(dbPath, JSON.stringify(seed, null, 2));
  }
  const db = JSON.parse(await readFile(dbPath, "utf8"));
  // 旧数据迁移：补齐版本与归档字段，旧流程样本不受影响
  for (const sample of db.samples) {
    if (typeof sample.version !== "number") sample.version = 1;
    if (typeof sample.archived !== "boolean") sample.archived = sample.delivery === "已交付";
    if (!Array.isArray(sample.archives)) sample.archives = [];
    if (!sample.lastReopen) sample.lastReopen = null;
  }
  return db;
}
async function saveDb(db) {
  const tmp = `${dbPath}.tmp`;
  await writeFile(tmp, JSON.stringify(db, null, 2));
  await rename(tmp, dbPath);
}

// ---- 写操作互斥：同进程内所有变更串行化，保证并发重开只成功一次 ----
let writeChain = Promise.resolve();
function withWriteLock(fn) {
  const run = writeChain.then(fn, fn);
  // 不让单次失败中断后续请求的锁链
  writeChain = run.then(() => {}, () => {});
  return run;
}

async function body(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
}
function sendJson(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(data, null, 2));
}
function updateSampleStatus(sample) {
  if (sample.archived) {
    sample.status = "已交付";
    return;
  }
  const sliceStatuses = sample.slices.map(slice => slice.status);
  if (sliceStatuses.length && sliceStatuses.every(step => step === "观察")) sample.status = "待观察";
  else if (sliceStatuses.some(step => inProgressSteps.includes(step))) sample.status = "制片中";
  else sample.status = "待切割";
}
// 交付瞬间抓取不可变快照（深拷贝），之后工作副本的任何修改都触达不到它
function takeSnapshot(sample, deliveredAt) {
  return {
    project: sample.project,
    borehole: sample.borehole,
    coreBox: sample.coreBox,
    depth: sample.depth,
    owner: sample.owner,
    status: sample.status,
    delivery: "已交付",
    slices: structuredClone(sample.slices)
  };
}

const page = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>岩芯样本切片实验室</title>
  <style>
    :root { --bg:#f1f3ef; --panel:#fff; --ink:#242822; --muted:#687062; --line:#d7ddd1; --accent:#526f43; --stone:#73706a; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
    header { padding:22px 28px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; gap:16px; }
    h1 { margin:0; font-size:26px; } main { display:grid; grid-template-columns:390px 1fr; gap:22px; padding:22px 28px; }
    form,.panel,.card,.stat { background:#fff; border:1px solid var(--line); border-radius:8px; padding:16px; } h2 { margin:0 0 12px; font-size:18px; }
    label { display:block; margin:10px 0 5px; color:var(--muted); font-size:13px; } input,select,textarea { width:100%; border:1px solid var(--line); border-radius:6px; padding:9px; font:inherit; background:#fff; } textarea { min-height:68px; }
    button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:10px 13px; font-weight:700; cursor:pointer; }
    .stats { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:14px; } .stat strong { display:block; font-size:24px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(330px,1fr)); gap:12px; } .card { display:grid; gap:8px; }
    .meta { color:var(--muted); font-size:13px; } .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:3px 8px; font-size:12px; }
    .pill.ver { border-color:var(--accent); color:var(--accent); } .pill.archived { background:var(--stone); border-color:var(--stone); color:#fff; }
    .slice { border-top:1px solid var(--line); padding-top:10px; } .toolbar { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:14px; }
    .history { border-top:2px solid var(--line); padding-top:10px; margin-top:6px; display:grid; gap:8px; }
    .archive { border:1px dashed var(--stone); border-radius:6px; padding:8px; background:#faf9f7; }
    .readonly { color:var(--stone); } .reopen-reason { background:#f3f0ea; border-radius:6px; padding:6px 8px; font-size:13px; }
    @media (max-width:950px){ header{display:block;padding:18px 16px;} main{grid-template-columns:1fr;padding:16px;} .stats{grid-template-columns:1fr 1fr;} }
  </style>
</head>
<body>
  <header><div><h1>岩芯样本切片实验室</h1><div class="meta">样本、切片任务、制片步骤、交付归档与复核重开</div></div><button id="reload">刷新</button></header>
  <main>
    <form id="form">
      <h2>创建岩芯样本</h2>
      <label>项目</label><input name="project" required>
      <label>钻孔编号</label><input name="borehole" required>
      <label>岩芯箱号</label><input name="coreBox" required>
      <label>取样深度</label><input name="depth" required>
      <label>负责人</label><input name="owner" required>
      <label>初始切片编号</label><input name="sliceId" required>
      <label>染色方法</label><input name="method" required>
      <button>保存样本</button>
    </form>
    <section>
      <div class="stats" id="stats"></div>
      <div class="grid" id="samples"></div>
    </section>
  </main>
  <script>
    const statuses = ${JSON.stringify(statuses)};
    const steps = ${JSON.stringify(sliceSteps)};
    const stats = document.querySelector("#stats");
    const samplesEl = document.querySelector("#samples");
    let samples = [];
    function esc(value) {
      return String(value ?? "").replace(/[&<>"']/g, ch => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[ch]));
    }
    async function api(path, options) {
      const res = await fetch(path, options && options.body ? { ...options, headers:{ "Content-Type":"application/json" } } : options);
      const data = await res.json();
      if (!res.ok) {
        const messages = {
          sample_archived_readonly: "样本已归档，切片和日志只读，不能修改",
          sample_not_archived: "样本当前不是归档状态，不能重开（请求可能已重复提交）",
          stale_reopen_request: "重开请求已过期：版本已变化，请刷新后重试",
          invalid_reopen_reason: "请填写重开原因",
          sample_not_found: "样本不存在",
          slice_not_found: "切片不存在"
        };
        throw new Error(messages[data.error] || data.error || "请求失败");
      }
      return data;
    }
    function sliceList(slices, readonly) {
      return slices.map(slice =>
        '<div class="slice"><b>'+esc(slice.id)+'</b><div class="meta">'+esc(slice.method)+' · 当前步骤 '+esc(slice.status)+'</div>' +
        (readonly
          ? '<div class="meta readonly">归档切片只读，共 '+slice.logs.length+' 条日志</div>'
          : '<select data-step="'+esc(slice.__sampleId)+'|'+esc(slice.id)+'">'+steps.map(step => '<option>'+esc(step)+'</option>').join("")+'</select>' +
            '<textarea data-note="'+esc(slice.__sampleId)+'|'+esc(slice.id)+'" placeholder="步骤备注或观察结果"></textarea>' +
            '<button data-log="'+esc(slice.__sampleId)+'|'+esc(slice.id)+'">记录步骤</button>') +
        '<div class="meta">'+slice.logs.map(log => esc(log.step)+"："+esc(log.note)).join(" / ")+'</div></div>'
      ).join("");
    }
    function archiveBlock(archive) {
      const snap = archive.snapshot;
      return '<div class="archive"><div><span class="pill archived">已归档 v'+archive.version+'</span></div>' +
        '<div class="meta">交付于 '+esc(archive.deliveredAt)+' · '+esc(snap.borehole)+' · '+esc(snap.coreBox)+' · '+esc(snap.depth)+'</div>' +
        sliceList(snap.slices, true) +
        (archive.reopen
          ? '<div class="reopen-reason">重开原因：'+esc(archive.reopen.reason)+'<br>负责人：'+esc(archive.reopen.by)+' · '+esc(archive.reopen.at)+'</div>'
          : '<div class="meta readonly">该归档版本未重开（只读）</div>') +
        '</div>';
    }
    function render() {
      stats.innerHTML = statuses.map(s => '<div class="stat"><span>'+s+'</span><strong>'+samples.filter(item => item.status === s).length+'</strong></div>').join("");
      samplesEl.innerHTML = samples.map(sample => {
        const workingSlices = sample.slices.map(slice => ({ ...slice, __sampleId: sample.id }));
        const reopenInfo = sample.lastReopen
          ? '<div class="reopen-reason">当前 v'+sample.version+' 由归档 v'+sample.lastReopen.fromVersion+' 重开<br>原因：'+esc(sample.lastReopen.reason)+'<br>负责人：'+esc(sample.lastReopen.by)+' · '+esc(sample.lastReopen.at)+'</div>'
          : '';
        const body = sample.archived
          ? '<div class="meta readonly">样本已归档，切片与日志不可修改，仅可由负责人填写原因后复核重开。</div>' +
            sliceList(workingSlices, true) +
            '<label>重开原因（必填）</label><textarea data-reopen-reason="'+esc(sample.id)+'" placeholder="例如：观察结果存疑，需补磨片复查"></textarea>' +
            '<label>重开负责人</label><input data-reopen-by="'+esc(sample.id)+'" value="'+esc(sample.owner)+'">' +
            '<button data-reopen="'+esc(sample.id)+'">复核重开为 v'+(sample.version+1)+'</button>'
          : '<label>新增切片</label><input data-new-slice="'+esc(sample.id)+'" placeholder="切片编号"><input data-method="'+esc(sample.id)+'" placeholder="染色方法"><button data-add="'+esc(sample.id)+'">添加切片</button>' +
            sliceList(workingSlices, false) +
            '<button data-deliver="'+esc(sample.id)+'">标记交付并归档 v'+(sample.version)+'</button>';
        return '<article class="card"><h3>'+esc(sample.project)+'</h3>' +
          '<div><span class="pill">'+esc(sample.status)+'</span> <span class="pill ver">当前版本 v'+sample.version+'</span> ' +
          (sample.archived ? '<span class="pill archived">已归档</span>' : '') + '</div>' +
          '<div class="meta">'+esc(sample.borehole)+' · '+esc(sample.coreBox)+' · '+esc(sample.depth)+' · '+esc(sample.owner)+'</div>' +
          reopenInfo + body +
          (sample.archives.length ? '<div class="history"><b>归档版本（旧版本只读保留）</b>' + sample.archives.map(archiveBlock).join("") + '</div>' : '') +
          '</article>';
      }).join("");
      document.querySelectorAll("[data-step]").forEach(sel => {
        const [sampleId, sliceId] = sel.dataset.step.split("|");
        const slice = samples.find(s => s.id === sampleId).slices.find(s => s.id === sliceId);
        sel.value = slice.status;
      });
      document.querySelectorAll("[data-add]").forEach(btn => btn.onclick = async () => {
        const id = btn.dataset.add;
        await api('/api/samples/'+encodeURIComponent(id)+'/slices', { method:'POST', body: JSON.stringify({ id: document.querySelector('[data-new-slice="'+id+'"]').value, method: document.querySelector('[data-method="'+id+'"]').value || "未指定" }) });
        await load();
      });
      document.querySelectorAll("[data-log]").forEach(btn => btn.onclick = async () => {
        const [sampleId, sliceId] = btn.dataset.log.split("|");
        await api('/api/samples/'+encodeURIComponent(sampleId)+'/slices/'+encodeURIComponent(sliceId)+'/logs', { method:'POST', body: JSON.stringify({ step: document.querySelector('[data-step="'+sampleId+'|'+sliceId+'"]').value, note: document.querySelector('[data-note="'+sampleId+'|'+sliceId+'"]').value || "步骤完成" }) });
        await load();
      });
      document.querySelectorAll("[data-deliver]").forEach(btn => btn.onclick = async () => {
        await api('/api/samples/'+encodeURIComponent(btn.dataset.deliver)+'/deliver', { method:'POST', body: JSON.stringify({}) });
        await load();
      });
      document.querySelectorAll("[data-reopen]").forEach(btn => btn.onclick = async () => {
        const id = btn.dataset.reopen;
        const current = samples.find(s => s.id === id);
        try {
          await api('/api/samples/'+encodeURIComponent(id)+'/reopen', { method:'POST', body: JSON.stringify({
            reason: document.querySelector('[data-reopen-reason="'+id+'"]').value,
            by: document.querySelector('[data-reopen-by="'+id+'"]').value || current.owner,
            expectedVersion: current.version
          }) });
        } catch (error) { alert("重开失败：" + error.message); }
        await load();
      });
    }
    async function load(){ samples = await api("/api/samples"); render(); }
    document.querySelector("#reload").onclick = load;
    document.querySelector("#form").onsubmit = async event => {
      event.preventDefault();
      const form = event.target;
      await api("/api/samples", { method:"POST", body: JSON.stringify(Object.fromEntries(new FormData(form).entries())) });
      form.reset(); await load();
    };
    load();
  </script>
</body>
</html>`;

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    if (req.method === "GET" && url.pathname === "/") {
      res.writeHead(200, { "Content-Type":"text/html; charset=utf-8" });
      return res.end(page);
    }
    if (req.method === "GET" && url.pathname === "/api/samples") {
      const db = await loadDb();
      return sendJson(res, 200, db.samples);
    }
    if (req.method === "POST" && url.pathname === "/api/samples") {
      const input = await body(req);
      return withWriteLock(async () => {
        const db = await loadDb();
        const sample = { id: `CORE-${Date.now()}`, project: input.project, borehole: input.borehole, coreBox: input.coreBox, depth: input.depth, owner: input.owner, status: "待切割", delivery: "未交付", version: 1, archived: false, archives: [], lastReopen: null, slices: [{ id: input.sliceId, method: input.method, observation: "", status: "取样", logs: [{ at: new Date().toISOString(), step: "取样", note: "创建初始切片任务" }] }] };
        updateSampleStatus(sample);
        db.samples.unshift(sample);
        await saveDb(db);
        sendJson(res, 201, sample);
      });
    }
    const addSlice = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices$/);
    if (addSlice && req.method === "POST") {
      const input = await body(req);
      return withWriteLock(async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === decodeURIComponent(addSlice[1]));
        if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
        if (sample.archived) return sendJson(res, 409, { error: "sample_archived_readonly" });
        sample.slices.push({ id: input.id, method: input.method || "未指定", observation: "", status: "取样", logs: [{ at: new Date().toISOString(), step: "取样", note: "新增切片任务" }] });
        updateSampleStatus(sample);
        await saveDb(db);
        sendJson(res, 201, sample);
      });
    }
    const logMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/logs$/);
    if (logMatch && req.method === "POST") {
      const input = await body(req);
      return withWriteLock(async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === decodeURIComponent(logMatch[1]));
        if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
        if (sample.archived) return sendJson(res, 409, { error: "sample_archived_readonly" });
        const slice = sample.slices.find(item => item.id === decodeURIComponent(logMatch[2]));
        if (!slice) return sendJson(res, 404, { error: "slice_not_found" });
        slice.status = input.step;
        if (input.step === "观察") slice.observation = input.note || slice.observation;
        slice.logs.push({ at: new Date().toISOString(), step: input.step, note: input.note || "" });
        updateSampleStatus(sample);
        await saveDb(db);
        sendJson(res, 200, sample);
      });
    }
    const deliverMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/deliver$/);
    if (deliverMatch && req.method === "POST") {
      await body(req);
      return withWriteLock(async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === decodeURIComponent(deliverMatch[1]));
        if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
        // 已归档不能重复交付；重开后的新版本可以再次交付并生成新快照
        if (sample.archived) return sendJson(res, 409, { error: "sample_archived_readonly" });
        updateSampleStatus(sample);
        const deliveredAt = new Date().toISOString();
        // 先抓不可变快照，再翻转归档状态
        sample.archives.push({ version: sample.version, deliveredAt, snapshot: takeSnapshot(sample, deliveredAt), reopen: null });
        sample.delivery = "已交付";
        sample.archived = true;
        updateSampleStatus(sample);
        await saveDb(db);
        sendJson(res, 200, sample);
      });
    }
    const reopenMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/reopen$/);
    if (reopenMatch && req.method === "POST") {
      const input = await body(req);
      return withWriteLock(async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === decodeURIComponent(reopenMatch[1]));
        if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
        const reason = String(input.reason || "").trim();
        if (!reason) return sendJson(res, 400, { error: "invalid_reopen_reason" });
        // 未归档不可重开
        if (!sample.archived) return sendJson(res, 409, { error: "sample_not_archived" });
        // 过期请求：页面持有的版本号与当前归档版本不一致
        if (Number.isInteger(input.expectedVersion) && input.expectedVersion !== sample.version) {
          return sendJson(res, 409, { error: "stale_reopen_request", currentVersion: sample.version });
        }
        const archive = sample.archives.find(item => item.version === sample.version);
        if (!archive) return sendJson(res, 409, { error: "archive_version_not_found" });
        const at = new Date().toISOString();
        const by = String(input.by || sample.owner).trim() || sample.owner;
        // 旧版本保留：仅在归档条目上登记重开元数据，快照本身不动
        archive.reopen = { reason, by, at };
        // 工作副本成为新版本，切片从“退回”继续
        const fromVersion = sample.version;
        for (const slice of sample.slices) {
          slice.status = "退回";
          slice.logs.push({ at, step: "退回", note: `归档 v${fromVersion} 复核重开：${reason}（负责人 ${by}）` });
        }
        sample.version = fromVersion + 1;
        sample.archived = false;
        sample.delivery = "未交付";
        sample.lastReopen = { reason, by, at, fromVersion };
        updateSampleStatus(sample);
        await saveDb(db);
        sendJson(res, 200, sample);
      });
    }
    sendJson(res, 404, { error: "not_found" });
  } catch (error) {
    sendJson(res, 500, { error: error.message });
  }
});

server.listen(port, () => console.log(`Core slice lab app listening on http://localhost:${port}`));
