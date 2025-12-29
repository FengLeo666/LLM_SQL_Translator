/* =========================
       State
    ========================= */
const App = {
    sessionId: (crypto.randomUUID ? crypto.randomUUID() : String(Date.now())),
    sourceFileName: "input.sql",
    sourceSqlText: "",
    templateName: "",
    destinationExampleText: "",
    chunks: [],
    autoConvertAfterPrompt: false,
    promptText: "",            // 后端返回的 general_prompt（只读）
    paramsConfirmed: false,    // true => 第一部分锁定；false => 第一部分可编辑
    busyCount: 0,

    // 固定参数
    // fixedConcurrency: 188,#用户输入
    // fixedMaxTry: 3,#后端决定
};

const STATUS = {WAIT: "wait", RUN: "run", OK: "ok", ERR: "err"};

/* =========================
   Session Persistence
   仅三处会触发：刷新 chunk + save storage（刷新后再存）
   1) 点击【确认参数】
   2) 点击【生成 General Prompt】(请求发出前)
   3) 后端返回 general_prompt 成功后
========================= */
const STORAGE_KEY = "sql_chunk_converter_state_v1";

function buildSessionSnapshot() {
    return {
        version: 1,
        sessionId: App.sessionId,
        params: readSharedParams(),
        autoConvertAfterPrompt: !!App.autoConvertAfterPrompt,

        sourceFileName: App.sourceFileName,
        sourceSqlText: App.sourceSqlText,

        templateName: App.templateName,
        destinationExampleText: App.destinationExampleText,

        chunks: App.chunks,

        userPrompt: valSafe("specialNeeds"),
        promptText: App.promptText,

        paramsConfirmed: !!App.paramsConfirmed,
    };
}

function markRunningChunksAsFailed() {
    if (!Array.isArray(App.chunks) || App.chunks.length === 0) return 0;

    let changed = 0;
    for (const c of App.chunks) {
        if (c && c.status === STATUS.RUN) {
            c.status = STATUS.ERR;
            c.dst = c.dst || "";
            c.exception = c.exception || "页面刷新/重启：上一次执行中的任务已中断，请重做该 Chunk。";
            changed++;
        }
    }
    return changed;
}

function saveSessionToStorage() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(buildSessionSnapshot()));
}

function loadSessionFromStorage() {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return false;

    try {
        const data = JSON.parse(raw);
        if (!data || data.version !== 1) return false;

        App.sessionId = data.sessionId || App.sessionId;

        // 回填参数
        setValSafe("sourceFormat", data.params?.source_format || "");
        setValSafe("destFormat", data.params?.destination_format || "");
        setValSafe("targetSchema", data.params?.target_schema || "");
        setValSafe("destLang", data.params?.destination_sql_language || "");
        setValSafe("mergeN", data.params?.merge_n ?? 1);
        setValSafe("concurrency", data.params?.concurrency ?? 188);

        // 源 SQL / 模板
        App.sourceFileName = data.sourceFileName || "input.sql";
        App.sourceSqlText = data.sourceSqlText || "";
        App.templateName = data.templateName || "";
        App.destinationExampleText = data.destinationExampleText || "";

        // chunk / prompt
        App.chunks = Array.isArray(data.chunks) ? data.chunks : [];
        App.promptText = typeof data.promptText === "string" ? data.promptText : "";
        App.paramsConfirmed = !!data.paramsConfirmed;

        App.autoConvertAfterPrompt = !!data.autoConvertAfterPrompt;
        const tgl = document.getElementById("autoConvertToggle");
        if (tgl) tgl.checked = App.autoConvertAfterPrompt;

        // 启动恢复：凡是 RUN，一律置为 ERR（因为刷新后不可能还在跑）
        const n = markRunningChunksAsFailed();
        if (n > 0) {
            // 可选：给用户一点提示（不强制）
            textSafe("runInfo", `检测到 ${n} 个执行中的 Chunk（RUN）在刷新后已中断，已标记为失败，请重做。`);
            // saveSessionToStorage(); // 立即落盘，避免下次刷新仍是 RUN
        }
        // 回填用户 Prompt
        if (typeof data.userPrompt === "string") {
            setValSafe("specialNeeds", data.userPrompt);
            autoResizeTextarea($("specialNeeds"));
        }

        // 回填 general_prompt
        setValSafe("generatedPrompt", App.promptText || "");
        autoResizeTextarea($("generatedPrompt"));
        setPromptBadge(App.promptText && App.promptText.trim() ? STATUS.OK : STATUS.WAIT,
            App.promptText && App.promptText.trim() ? "已生成" : "未生成");

        // 回放上传状态 UI（不改动 chunks）
        syncSqlDropUI();
        syncTemplateDropUI();

        // 回放按钮状态
        syncParamsButtons();

        return true;
    } catch (e) {
        console.warn("Failed to restore session:", e);
        return false;
    }
}

/* =========================
   Helpers
========================= */
function $(id) {
    return document.getElementById(id);
}

function el(id) {
    return document.getElementById(id);
}

function exists(id) {
    return !!el(id);
}

function textSafe(id, t) {
    const n = el(id);
    if (n) n.textContent = t;
}

function valSafe(id) {
    const n = el(id);
    return n ? n.value : "";
}

function setValSafe(id, v) {
    const n = el(id);
    if (n) n.value = v;
}

function clamp(n, a, b) {
    return Math.max(a, Math.min(b, n));
}

function autoResizeTextarea(elm) {
    if (!elm) return;
    elm.style.height = "auto";
    elm.style.height = (elm.scrollHeight + 2) + "px";
}

function sanitizeFilename(name) {
    return name.replace(/[\\\/:*?"<>|]+/g, "_");
}

function downloadText(filename, content) {
    const blob = new Blob([content], {type: "text/plain;charset=utf-8"});
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
}

async function postJson(url, payload) {
    const resp = await fetch(url, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
    });

    // 先尽量解析 JSON（错误/成功都可能是 JSON）
    let data = null;
    const ct = resp.headers.get("content-type") || "";
    if (ct.includes("application/json")) {
        try {
            data = await resp.json();
        } catch {
        }
    } else {
        const txt = await resp.text().catch(() => "");
        data = txt ? {raw: txt} : null;
    }

    if (!resp.ok) {
        // 构造一个结构化 Error，携带 status + body
        const err = new Error(
            (data && data.detail && (typeof data.detail === "string"
                ? data.detail
                : (data.detail.message || "Request failed")))
            || `HTTP ${resp.status}`
        );
        err.status = resp.status;
        err.body = data;
        throw err;
    }

    return data;
}

function isBusy() {
    return App.busyCount > 0;
}

function setBusy(on) {
    App.busyCount += on ? 1 : -1;
    if (App.busyCount < 0) App.busyCount = 0;
    applySectionMasks();
    updateCounters();
}

function escapeHtml(s) {
    return String(s || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function escapeHtmlTextarea(s) {
    return escapeHtml(s).replaceAll("\r\n", "\n");
}

function setPromptBadge(state, text) {
    const badge = el("promptBadge");
    const icon = el("promptBadgeIcon");
    const info = el("promptInfo");
    if (!badge || !icon || !info) return;

    // state: wait | run | ok | err
    badge.classList.remove("wait", "run", "ok", "err");
    badge.classList.add(state);

    // run 用 spinner，其余用 dot（与 chunk 一致）
    icon.className = (state === STATUS.RUN) ? "spinner" : "dot";

    info.textContent = text || "";
}

/* =========================
   Splitter
========================= */
function splitSqlByRegex(sqlText) {
    const reCt = /create\s+table/ig;
    const matches = [];
    let m;
    while ((m = reCt.exec(sqlText)) !== null) matches.push({idx: m.index});

    const trimmed = (sqlText || "").trim();
    if (matches.length === 0) {
        return trimmed ? [trimmed] : [];
    }

    const boundariesSet = new Set();
    for (const it of matches) {
        const ctPos = it.idx;
        const semi = sqlText.lastIndexOf(";", ctPos);
        if (semi !== -1) boundariesSet.add(semi + 1);
    }

    const boundaries = Array.from(boundariesSet)
        .filter(b => b > 0 && b < sqlText.length)
        .sort((a, b) => a - b);

    const out = [];
    let start = 0;
    for (const b of boundaries) {
        if (b <= start) continue;
        const part = sqlText.slice(start, b).trim();
        if (part) out.push(part);
        start = b;
    }
    const last = sqlText.slice(start).trim();
    if (last) out.push(last);
    return out;
}

function guessChunkName(sqlChunk) {
    const m = (sqlChunk || "").match(/create\s+table\s+([`"\[]?)([a-zA-Z0-9_.]+)/i);
    if (!m) return "SQL Chunk";
    return `CREATE TABLE ${m[2]}`;
}

function mergeParts(parts, n) {
    const nn = Math.max(1, parseInt(n, 10) || 1);
    const merged = [];
    for (let i = 0; i < parts.length; i += nn) {
        merged.push(parts.slice(i, i + nn).join("\n\n"));
    }
    return merged;
}

/* =========================
   UI Sync (Drop zones)
========================= */
function syncSqlDropUI() {
    const dz = el("sqlDrop");
    if (!dz) return;

    const hasSql = !!(App.sourceSqlText && App.sourceSqlText.trim());

    if (hasSql) {
        dz.classList.add("uploaded");
        dz.classList.remove("dragover");
        el("sqlDropTag")?.classList.remove("hidden");
        textSafe("sqlDropTitle", "SQL 文件已上传（点击「确认参数」后分片）");

        const chunkText = (Array.isArray(App.chunks) && App.chunks.length > 0)
            ? `已分片 ${App.chunks.length} 个 chunk`
            : `待确认参数后分片`;

        textSafe(
            "sqlDropSub",
            `${App.sourceFileName} · ${App.sourceSqlText.length} chars · ${chunkText}`
        );
        textSafe("fileInfo", `已加载：${App.sourceFileName}（${App.sourceSqlText.length} chars）`);
    } else {
        dz.classList.remove("uploaded");
        dz.classList.remove("dragover");
        el("sqlDropTag")?.classList.add("hidden");
        textSafe("sqlDropTitle", "拖拽 SQL 文件到这里（确认参数后分片）");
        textSafe("sqlDropSub", "或点击右侧按钮选择文件（内容仅保存在浏览器内存中）");
        textSafe("fileInfo", "未上传 SQL 文件");
    }
}

function syncTemplateDropUI() {
    const dz = el("tplDrop");
    if (!dz) return;

    const hasTpl = !!(App.destinationExampleText && App.destinationExampleText.trim());
    if (hasTpl) {
        dz.classList.add("uploaded");
        el("tplDropTag")?.classList.remove("hidden");
        textSafe("tplDropTitle", "参考模版已上传（可拖拽替换）");
        textSafe("tplDropSub", `${App.templateName} · ${App.destinationExampleText.length} chars`);
    } else {
        dz.classList.remove("uploaded");
        el("tplDropTag")?.classList.add("hidden");
        textSafe("tplDropTitle", "可选：上传参考模版（destination_example）");
        textSafe("tplDropSub", "用于生成 Prompt（normalize_prompt 的 destination_example）");
    }
}

/* =========================
   Chunk Build / Reset
   - “刷新 chunk”指：重建 chunks 并清空结果/状态
   - 注意：只有三处会触发 refresh+save（由调用方控制）
========================= */
function rebuildChunksFromCurrentSql({clearResults = true} = {}) {
    const sql = App.sourceSqlText || "";
    const p = readSharedParams();
    const parts0 = splitSqlByRegex(sql);
    const parts = mergeParts(parts0, p.merge_n);

    App.chunks = parts.map((src, idx) => ({
        id: `${App.sessionId}:${idx}`,
        idx,
        name: guessChunkName(src),
        src,
        dst: "",
        status: STATUS.WAIT,
        exception: "",
        showPrompt: false,
        promptOverride: "",
    }));

    syncSqlDropUI();
}

function refreshChunks(reason) {
    // 统一“刷新 chunk”：重建并清空所有转换结果
    rebuildChunksFromCurrentSql({clearResults: true});

    if (reason) textSafe("runInfo", reason);
    renderChunks();
    updateCounters();
}

function refreshChunksAndPersist(reason) {
    // 仅允许在三个地方调用：刷新后再存
    refreshChunks(reason);
    saveSessionToStorage();
}

/* =========================
   Params / Validation
========================= */
function readSharedParams() {
    const source_format = (valSafe("sourceFormat") || "").trim();
    const destination_format = (valSafe("destFormat") || "").trim();
    const target_schema = (valSafe("targetSchema") || "").trim();
    const destination_sql_language = (valSafe("destLang") || "").trim();

    const merge_n = Math.max(1, parseInt(valSafe("mergeN"), 10) || 1);
    const concurrency = Math.max(1, parseInt(valSafe("concurrency"), 10) || 188);

    return {
        source_format,
        destination_format,
        target_schema,
        destination_sql_language,
        merge_n,
        concurrency
    };
}

function validateBeforeRequest() {
    const p = readSharedParams();
    const errors = [];
    if (!p.source_format) errors.push("source_format 必填");
    if (!p.destination_format) errors.push("destination_format 必填");

    const msg = errors.length ? `参数校验失败：${errors.join("；")}` : "";
    textSafe("paramError", msg);
    return errors.length === 0;
}

function syncParamsButtons() {
    const confirmBtn = el("btnConfirmParams");
    const editBtn = el("btnEditParams");

    if (confirmBtn) confirmBtn.disabled = App.paramsConfirmed;     // confirmed => 不能再 confirm
    if (editBtn) editBtn.disabled = !App.paramsConfirmed;          // 只有 confirmed 才能点“修改参数”

    textSafe("paramsStatus", App.paramsConfirmed
        ? "已确认：第一部分已锁定，可继续生成 Prompt。"
        : "未确认：请填写参数/上传文件后点击“确认参数”。"
    );
}

/* =========================
   Section Masks
   - 第一部分：paramsConfirmed=true 时锁定（包括文件上传）
   - 第二部分：仅在 paramsConfirmed=true 时可用
   - chunk：仅在 promptText 存在时可用
========================= */
function applySectionMasks() {
    const busy = isBusy();
    const paramsLocked = App.paramsConfirmed === true;
    const promptReady = !!(App.promptText && App.promptText.trim());

    /* ===== 第一部分：参数区 ===== */

    // 参数输入框（select / input）
    ["sourceFormat", "destFormat", "targetSchema", "destLang", "mergeN", "concurrency"].forEach(id => {
        const elx = document.getElementById(id);
        if (elx) elx.disabled = busy || paramsLocked;
    });

    // SQL / Template 上传区（整个 dropzone）
    const sqlDrop = document.getElementById("sqlDrop");
    const tplDrop = document.getElementById("tplDrop");

    if (sqlDrop) sqlDrop.classList.toggle("disabledMask", busy || paramsLocked);
    if (tplDrop) tplDrop.classList.toggle("disabledMask", busy || paramsLocked);

    // 文件选择按钮
    const pickSqlBtn = document.getElementById("pickSqlBtn");
    const pickTplBtn = document.getElementById("pickTplBtn");
    if (pickSqlBtn) pickSqlBtn.disabled = busy || paramsLocked;
    if (pickTplBtn) pickTplBtn.disabled = busy || paramsLocked;

    /* ===== 第二部分：Prompt ===== */

    const secPrompt = document.getElementById("sectionPrompt");
    if (secPrompt) {
        secPrompt.classList.toggle("disabledMask", busy || !paramsLocked);
    }

//         /* ===== Chunk 区 ===== */
//         const secChunks = document.getElementById("sectionChunks");
//         if (secChunks) {
//             secChunks.classList.toggle("disabledMask", busy || !promptReady);
//         }
//
// // ✅ 补上：锁层显示/隐藏（promptReady=true 时隐藏）
//         const chunksLock = document.getElementById("chunksLock");
//         if (chunksLock) {
//             chunksLock.classList.toggle("show", busy || !promptReady);
//             // 可选：动态提示文字
//             if (busy) {
//                 textSafe("chunksLockTitle", "Chunk 区域已冻结");
//                 textSafe("chunksLockMsg", "当前有任务执行中，请稍候完成后再操作。");
//             } else if (!promptReady) {
//                 textSafe("chunksLockTitle", "Chunk 区域已冻结");
//                 textSafe("chunksLockMsg", "请先在右侧生成 General Prompt；生成完成后将自动解锁。");
//             }
//         }

    // 自动 convert 开关：busy 时禁用；不跟随 paramsLocked
    const autoTgl = document.getElementById("autoConvertToggle");
    const autoWrap = autoTgl ? autoTgl.closest(".switchWrap") : null;
    if (autoTgl) autoTgl.disabled = busy;
    if (autoWrap) autoWrap.classList.toggle("disabledMask", busy);
}


/* =========================
   Eligibility
========================= */
function canGeneratePrompt() {
    if (isBusy()) return false;
    if (!App.paramsConfirmed) return false;
    if (!(App.sourceSqlText && App.sourceSqlText.trim())) return false;
    if (!valSafe("specialNeeds").trim()) return false;
    return validateBeforeRequest();
}

function canConvertAll() {
    if (isBusy()) return false;
    if (!App.paramsConfirmed) return false;
    if (!App.promptText.trim()) return false;
    if (!Array.isArray(App.chunks) || App.chunks.length === 0) return false;
    return validateBeforeRequest();
}

function allSuccess() {
    return App.chunks.length > 0 && App.chunks.every(c => c.status === STATUS.OK);
}

function anyFailed() {
    return App.chunks.some(c => c.status === STATUS.ERR);
}

/* =========================
   Rendering
========================= */
function updateTopPills() {
    textSafe("sessionPill", App.sessionId.slice(0, 8));
    textSafe("chunksPill", String(App.chunks.length));
    textSafe("paramsPill", App.paramsConfirmed ? "是" : "否");
    textSafe("promptPill", App.promptText.trim() ? "是" : "否");
}

function updateCounters() {
    const cntWait = App.chunks.filter(c => c.status === STATUS.WAIT).length;
    const cntRun = App.chunks.filter(c => c.status === STATUS.RUN).length;
    const cntOk = App.chunks.filter(c => c.status === STATUS.OK).length;
    const cntErr = App.chunks.filter(c => c.status === STATUS.ERR).length;

    textSafe("cntWait", String(cntWait));
    textSafe("cntRun", String(cntRun));
    textSafe("cntOk", String(cntOk));
    textSafe("cntErr", String(cntErr));
    textSafe("exportReady", allSuccess() ? "是" : "否");

    const btnGen = el("btnGeneratePrompt");
    const btnAll = el("btnConvertAll");
    const btnRedo = el("btnRedoFailed");
    const btnExp = el("btnExport");

    if (btnGen) btnGen.disabled = !canGeneratePrompt();
    if (btnAll) btnAll.disabled = !canConvertAll();
    if (btnRedo) btnRedo.disabled = isBusy() || !anyFailed() || !App.promptText.trim() || !validateBeforeRequest();
    if (btnExp) btnExp.disabled = isBusy() || !allSuccess();

    updateTopPills();
    applySectionMasks();
    syncParamsButtons();
}

function chunkCardClass(c) {
    if (c.status === STATUS.RUN) return "chunkCard chunkStateRun";
    if (c.status === STATUS.OK) return "chunkCard chunkStateOk";
    if (c.status === STATUS.ERR) return "chunkCard chunkStateErr";
    return "chunkCard chunkStateWait";
}

function chunkBadgeHtml(c) {
    if (c.status === STATUS.RUN) return `<span class="badge run"><span class="spinner"></span>执行中</span>`;
    if (c.status === STATUS.OK) return `<span class="badge ok"><span class="dot"></span>成功</span>`;
    if (c.status === STATUS.ERR) return `<span class="badge err"><span class="dot"></span>失败</span>`;
    return `<span class="badge wait"><span class="dot"></span>等待</span>`;
}

function renderChunks() {
    const container = el("chunksContainer");
    if (!container) return;
    container.innerHTML = "";

    const promptReady = !!App.promptText.trim();
    const disableAll = isBusy() || !promptReady;

    App.chunks.forEach(c => {
        const wrap = document.createElement("div");
        wrap.className = chunkCardClass(c);
        wrap.dataset.chunkId = c.id;

        const disableThis = disableAll || c.status === STATUS.RUN;

        wrap.innerHTML = `
      <div class="chunkHd">
        <div class="chunkTitle">
          <div class="t">
            <span>Chunk #${c.idx + 1}</span>
            ${chunkBadgeHtml(c)}
          </div>
          <div class="m">${escapeHtml(c.name)} · <span class="mono">${c.src.length}</span> chars</div>
        </div>
        <div class="chunkActions">
          <button data-act="togglePrompt" data-id="${c.id}" ${disableAll ? "disabled" : ""}>
            ${c.showPrompt ? "隐藏提示词" : "修改提示词重做"}
          </button>
          <button data-act="redoOne" data-id="${c.id}" class="primary" ${disableThis ? "disabled" : ""}>
            重做此 Chunk
          </button>
          <button data-act="copyOut" data-id="${c.id}" ${(!c.dst || !c.dst.trim() || disableAll) ? "disabled" : ""}>
            复制结果
          </button>
        </div>
      </div>

      <div class="chunkBody">
        <div class="${c.showPrompt ? "" : "hidden"}" id="promptBox-${c.id}">
          <label>该 Chunk 专用 Prompt（仅在需要重做时显示；留空表示使用生成后的 general_prompt）</label>
          <textarea class="mono" id="prompt-${c.id}" placeholder="留空则使用生成后的 general_prompt"
            ${disableAll ? "readonly" : ""}>${escapeHtmlTextarea(c.promptOverride || "")}</textarea>

          <div class="btnrow">
            <button data-act="savePrompt" data-id="${c.id}" ${disableAll ? "disabled" : ""}>仅保存提示词</button>
            <button data-act="saveAndRedo" data-id="${c.id}" class="primary" ${disableThis ? "disabled" : ""}>保存并重做</button>
            <span class="hint">默认 general_prompt：${promptReady ? "已就绪" : "未生成"}</span>
          </div>
          <div class="sep"></div>
        </div>

        <div class="twoCol">
          <div class="panel  panel-src">
            <div class="ph"><div>转换前</div><div class="hint mono">source chunk</div></div>
            <div class="pb">
              <textarea readonly class="mono">${escapeHtmlTextarea(c.src)}</textarea>
            </div>
          </div>

          <div class="panel panel-dst">
            <div class="ph">
              <div>转换后</div>
              <div class="hint mono">${c.status === STATUS.OK ? "result" : (c.status === STATUS.RUN ? "running..." : "pending")}</div>
            </div>
            <div class="pb">
              <textarea class="mono" id="out-${c.id}" ${disableThis ? "readonly" : ""}>
${escapeHtmlTextarea(c.dst || "")}</textarea>
              <div class="errBox ${c.exception ? "show" : ""}" id="err-${c.id}">
${escapeHtml(c.exception || "")}
              </div>
            </div>
          </div>
        </div>
      </div>
    `;

        container.appendChild(wrap);

        if (c.showPrompt) {
            const pta = el("prompt-" + c.id);
            autoResizeTextarea(pta);
            pta?.addEventListener("input", () => autoResizeTextarea(pta));
        }

        const outTa = el("out-" + c.id);
        if (outTa) {
            outTa.addEventListener("input", () => {
                c.dst = outTa.value || "";
                updateCounters();
            });
        }
    });

    container.querySelectorAll("button[data-act]").forEach(btn => {
        btn.addEventListener("click", onChunkAction);
    });

    updateCounters();
}


let collapseTimer = null;

function expandChunkById(chunkId) {
    const card = document.querySelector(`.chunkCard[data-chunk-id="${CSS.escape(chunkId)}"]`);
    if (!card) return;
    card.classList.add("expanded");
}

function collapseChunkById(chunkId) {
    const card = document.querySelector(`.chunkCard[data-chunk-id="${CSS.escape(chunkId)}"]`);
    if (!card) return;
    card.classList.remove("expanded");
}

/**
 * 事件委托：任何 textarea focus => 展开所在 chunk；blur => 延迟收起
 * 用 capture=true 捕获 focus/blur（它们不冒泡）
 */
function bindChunkFocusExpand() {
    const container = document.getElementById("chunksContainer");
    if (!container) return;

    // 每个 chunkCard 独立的收起 timer
    const timers = new WeakMap();

    function clearTimer(card) {
        const t = timers.get(card);
        if (t) clearTimeout(t);
        timers.delete(card);
    }

    container.addEventListener("focusin", (e) => {
        const ta = e.target;
        if (!(ta instanceof HTMLTextAreaElement)) return;

        const card = ta.closest(".chunkCard");
        if (!card) return;

        // 关键：当焦点进入某个 chunk，先把其它 chunk 全部收起
        container.querySelectorAll(".chunkCard.expanded").forEach((c) => {
            if (c !== card) {
                clearTimer(c);
                c.classList.remove("expanded");
            }
        });

        clearTimer(card);
        card.classList.add("expanded");
    });

    container.addEventListener("focusout", (e) => {
        const ta = e.target;
        if (!(ta instanceof HTMLTextAreaElement)) return;

        const card = ta.closest(".chunkCard");
        if (!card) return;

        clearTimer(card);

        // 延迟一点点，避免从本 chunk 的一个 textarea 切到另一个 textarea 时误收起
        const timer = setTimeout(() => {
            // 只要本 chunk 内没有任何 textarea 仍然 focus，就收起
            const anyFocused = !!card.querySelector("textarea:focus");
            if (!anyFocused) card.classList.remove("expanded");
        }, 80);

        timers.set(card, timer);
    });
}


/* =========================
   File handling
   - 仅允许在“未确认参数（可编辑）”时操作
   - 这里不做 storage 保存（按你的新约束）
========================= */
async function loadSqlFile(file) {
    if (isBusy()) return;
    if (App.paramsConfirmed) return;

    App.sourceFileName = file.name || "input.sql";
    App.sourceSqlText = await file.text();

    // 确认前不分片：清空旧 chunks，避免误用旧分片
    App.chunks = [];
    syncSqlDropUI();

    // 编辑态：prompt 失效
    App.promptText = "";
    setValSafe("generatedPrompt", "");
    setPromptBadge(STATUS.WAIT, "未生成");

    textSafe("runInfo", `已加载 SQL：${App.sourceFileName}（${App.sourceSqlText.length} chars）。请点击“确认参数”后执行分片。`);

    renderChunks();
    updateCounters();
}

async function loadTemplateFile(file) {
    if (isBusy()) return;
    if (App.paramsConfirmed) return; // 第一部分锁定时不允许变更

    App.templateName = file.name || "template.txt";
    App.destinationExampleText = await file.text();

    syncTemplateDropUI();

    // 编辑第一部分时：prompt 视为失效（但不保存）
    App.promptText = "";
    setValSafe("generatedPrompt", "");
    setPromptBadge(STATUS.WAIT, "未生成");

    textSafe("runInfo", `已加载参考模版：${App.templateName}。请确认参数后再生成 Prompt。`);

    updateCounters();
    renderChunks();
}

function bindDropZone(zoneEl, onFile) {
    zoneEl.addEventListener("dragover", (e) => {
        e.preventDefault();
        zoneEl.classList.add("dragover");
    });
    zoneEl.addEventListener("dragleave", () => zoneEl.classList.remove("dragover"));
    zoneEl.addEventListener("drop", async (e) => {
        e.preventDefault();
        zoneEl.classList.remove("dragover");
        const file = e.dataTransfer.files && e.dataTransfer.files[0];
        if (file) await onFile(file);
    });
}

/* =========================
   Prompt generation
========================= */
function buildDefaultSpecialNeedsPrompt() {
    const p = readSharedParams();
    return `你是一个专业的 SQL 转写器。

目标：
- 将输入 SQL 从【${p.source_format || "源格式"}】转换为【${p.destination_format || "目标格式"}】。
- 目标 schema：${p.target_schema || "(未指定)"}。

输出要求：
1) 仅输出转换后的 SQL（不要解释、不要 Markdown）。
2) 保持语义等价；不兼容语法请做合理改写。
3) 尽量保留注释、分区/分桶、约束、默认值等（目标方言支持时）。
4) 必要时用 SQL 注释解释不可避免的差异。`;
}

function buildMainStatePayload(userPrompt) {
    const p = readSharedParams();
    return {
        task_id: App.sessionId,
        general_prompt: userPrompt || "",
        source_format: p.source_format,
        destination_format: p.destination_format,
        destination_sql_language: p.destination_sql_language,
        target_schema: p.target_schema,
        merge_n: p.merge_n,

        source_sql: App.sourceSqlText || "",
        destination_example: App.destinationExampleText || "",

        chunked_sql: App.chunks.map(c => c.src),
        result_chunks: App.chunks.map(c => ({sql: c.dst || "", exception: c.exception || ""})),
        result: App.chunks.map(c => (c.dst || "")).join("\n\n"),
    };
}

async function onGeneratePrompt() {
    if (isBusy()) return;

    if (!App.paramsConfirmed) {
        textSafe("promptStatus", "请先确认参数（第一部分已锁定后才能生成 Prompt）。");
        return;
    }
    if (!validateBeforeRequest()) return;
    if (!App.sourceSqlText.trim()) {
        textSafe("promptStatus", "请先在第一部分上传 SQL 文件，然后确认参数。");
        return;
    }

    const userPrompt = valSafe("specialNeeds").trim();
    if (!userPrompt) {
        textSafe("promptStatus", "用户 Prompt 不能为空。");
        return;
    }

    // 是否在本次成功后自动 Convert
    let shouldAutoConvert = false;

    // 【保存点 #2】点击生成 general_prompt：先刷新 chunk，再存
    setBusy(true);
    setPromptBadge(STATUS.RUN, "生成中...");
    textSafe(
        "runInfo",
        "已发起 general_prompt 生成：Chunk 已刷新并冻结，等待后端返回..."
    );
    refreshChunksAndPersist(
        "已发起 general_prompt 生成：Chunk 已刷新并冻结，等待后端返回..."
    );

    try {
        const payload = buildMainStatePayload(userPrompt);
        const resp = await postJson("/api/normalize_prompt", payload);

        if (resp && typeof resp.general_prompt === "string") {
            App.promptText = resp.general_prompt;

            setValSafe("generatedPrompt", App.promptText);
            autoResizeTextarea($("generatedPrompt"));

            setPromptBadge(STATUS.OK, "已生成");
            textSafe(
                "promptStatus",
                "general_prompt 已生成（只读）。如需修改请改上方用户 Prompt 后重新生成。"
            );

            // 【保存点 #3】后端返回 general_prompt：刷新 chunk 后再存
            refreshChunksAndPersist(
                "general_prompt 已更新：Chunk 已刷新，可开始 Convert All。"
            );

            // 记录是否需要自动 Convert（延后到 finally 执行）
            shouldAutoConvert = !!App.autoConvertAfterPrompt;
        } else {
            setPromptBadge(STATUS.ERR, "返回异常");
            textSafe(
                "promptStatus",
                "后端未返回 general_prompt 或格式不符合预期。"
            );
            textSafe("runInfo", "general_prompt 生成失败：后端返回异常。");
        }
    } catch (e) {
        setPromptBadge(STATUS.ERR, "生成失败");
        textSafe("promptStatus", `错误：${e.message || e}`);
        textSafe(
            "runInfo",
            "general_prompt 生成失败：请检查后端/网络后重试。"
        );
    } finally {
        // 先释放 busy，再允许后续操作
        setBusy(false);
        updateCounters();
        renderChunks();

        // ✅ 自动 Convert All（在 busy 释放后执行，避免状态冲突）
        if (shouldAutoConvert) {
            textSafe(
                "runInfo",
                "general_prompt 已生成，已根据设置自动开始 Convert All..."
            );
            await onConvertAll();
        }
    }
}

function replacePromptParamsWithNew(prompt, p) {
    if (!prompt) return prompt;

    let out = String(prompt);

    // 1) 替换“从【X】转换为【Y】”这段（兼容你的默认模板）
    // 例：- 将输入 SQL 从【gbase8c】转换为【gbasehd】。
    out = out.replace(
        /(将输入\s*SQL\s*从【)([^】]*)(】\s*转换为【)([^】]*)(】)/gi,
        `$1${p.source_format || "源格式"}$3${p.destination_format || "目标格式"}$5`
    );

    // 兜底：如果用户删掉了“将输入 SQL”但仍保留“从【】转换为【】”
    out = out.replace(
        /(^|\n)(.*?从【)([^】]*)(】\s*转换为【)([^】]*)(】)/gi,
        (m, g1, g2, _oldSrc, g4, _oldDst, g6) =>
            `${g1}${g2}${p.source_format || "源格式"}${g4}${p.destination_format || "目标格式"}${g6}`
    );

    // 2) 替换目标 schema 行
    // 例：- 目标 schema：stg_xxx。
    out = out.replace(
        /(目标\s*schema\s*[:：]\s*)(.*)/i,
        `$1${p.target_schema ? p.target_schema : "(未指定)"}`
    );

    // 兜底：如果用户用的是 target_schema: xxx
    out = out.replace(
        /(target_schema\s*[:：]\s*)(.*)/i,
        `$1${p.target_schema ? p.target_schema : "(未指定)"}`
    );

    // 3) （可选）替换“目标数据库方言”行（如果用户写了这行）
    out = out.replace(
        /(目标数据库方言\s*[:：]\s*)(.*)/i,
        `$1${p.destination_sql_language ? p.destination_sql_language : "(未指定)"}`
    );

    // 4) （可选）替换 merge_n（如果用户 Prompt 里提到了）
    out = out.replace(
        /(合并表数量\s*n\s*[:：]\s*)(\d+)/i,
        `$1${p.merge_n}`
    );

    return out;
}


/* =========================
   Chunk execution
   （按你当前新约束：不在这里做 storage save/refresh）
========================= */
function buildChunkStatePayload(chunk) {
    const p = readSharedParams();
    const promptOverride = (chunk.promptOverride || "").trim();
    const promptToUse = promptOverride || App.promptText || "";

    return {
        task_id: chunk.id,
        general_prompt: promptToUse,
        source_format: p.source_format,
        destination_format: p.destination_format,
        destination_sql_language: p.destination_sql_language,
        target_schema: p.target_schema,

        destination_example: App.destinationExampleText || "",
        sql: chunk.src,

        // 兼容字段（如果后端仍读 prompt）
        prompt: promptToUse
    };
}

async function convertOneChunk(chunk) {
    if (!validateBeforeRequest()) return;
    if (!App.promptText.trim()) {
        textSafe("runInfo", "请先生成 Prompt。");
        return;
    }

    chunk.status = STATUS.RUN;
    chunk.exception = "";
    renderChunks(); // 立即刷新状态

    try {
        const payload = buildChunkStatePayload(chunk);
        const resp = await postJson("/api/convert_chunk", payload);

        const outSql = (resp && typeof resp.sql === "string") ? resp.sql : "";
        const ex = (resp && typeof resp.exception === "string") ? resp.exception : "";

        chunk.dst = outSql || "";
        chunk.exception = ex || "";

        if (chunk.dst.trim() && !chunk.exception) {
            chunk.status = STATUS.OK;
        } else {
            chunk.status = STATUS.ERR;
            if (!chunk.exception) {
                chunk.exception = "后端未返回 sql 或 sql 为空。";
            }
        }
    } catch (e) {
        chunk.status = STATUS.ERR;
        chunk.dst = "";

        const detail = e && e.body && e.body.detail;
        if (typeof detail === "string") {
            chunk.exception = detail;
        } else if (detail && typeof detail === "object") {
            chunk.exception = detail.exception || detail.message || JSON.stringify(detail);
        } else {
            chunk.exception = String(e?.message || e || "Unknown error");
        }
    } finally {
        renderChunks();
        saveSessionToStorage();
    }
}


async function runWithConcurrency(items, workerFn, limit) {
    const results = new Array(items.length);
    let next = 0;

    async function worker() {
        while (true) {
            const i = next++;
            if (i >= items.length) return;
            results[i] = await workerFn(items[i], i);
        }
    }

    const n = clamp(limit || 188, 1, items.length || 1);
    const workers = [];
    for (let i = 0; i < n; i++) workers.push(worker());
    await Promise.all(workers);
    return results;
}

async function onConvertAll() {
    if (!validateBeforeRequest()) return;
    if (!App.promptText.trim()) {
        textSafe("runInfo", "请先生成 Prompt。");
        return;
    }
    if (App.chunks.length === 0) {
        textSafe("runInfo", "请先上传 SQL 文件并分片。");
        return;
    }

    setBusy(true);
    textSafe("runInfo", "开始 Convert All（已暂时禁用页面操作）...");

    // reset non-ok
    App.chunks.forEach(c => {
        if (c.status !== STATUS.OK) {
            c.status = STATUS.WAIT;
            c.exception = "";
            c.dst = c.dst || "";
        }
    });
    renderChunks();

    try {
        const {concurrency} = readSharedParams();
        await runWithConcurrency(App.chunks, async (ch) => {
            if (ch.status === STATUS.OK) return true;
            await convertOneChunk(ch);
            return true;
        }, concurrency);

        textSafe(
            "runInfo",
            allSuccess()
                ? "全部 Chunk 成功，可导出。"
                : "执行结束：仍有失败 Chunk，可一键重做失败项或单独重做。"
        );
    } finally {
        // ✅ 关键修复：busy 结束后再 render 一次恢复 disabled/readonly
        setBusy(false);
        renderChunks(); // ← 修复点
    }
}


async function onRedoFailed() {
    if (!validateBeforeRequest()) return;
    if (!App.promptText.trim()) return;

    const failed = App.chunks.filter(c => c.status === STATUS.ERR);
    if (failed.length === 0) {
        textSafe("runInfo", "没有失败 Chunk。");
        return;
    }

    setBusy(true);
    textSafe("runInfo", `开始重做失败 Chunk（共 ${failed.length} 个）...`);

    try {
        const {concurrency} = readSharedParams();
        await runWithConcurrency(failed, async (ch) => {
            ch.status = STATUS.WAIT;
            ch.exception = "";
            ch.dst = "";
            renderChunks();
            await convertOneChunk(ch);
            return true;
        }, concurrency);

        textSafe(
            "runInfo",
            allSuccess()
                ? "重做完成：全部成功，可导出。"
                : "重做完成：仍有失败项。"
        );
    } finally {
        // ✅ 关键修复：busy 结束后再 render 一次恢复 disabled/readonly
        setBusy(false);
        renderChunks(); // ← 修复点
    }
}


function onExport() {
    if (!allSuccess()) {
        textSafe("runInfo", "只有全部 Chunk 成功后才能导出。");
        return;
    }

    const dst = (valSafe("destFormat").trim() || "output").toLowerCase();
    const base = (App.sourceFileName || "input").replace(/\.(sql|txt)$/i, "");
    const filename = sanitizeFilename(`${base}_to_${dst}.sql`);

    const out = App.chunks.map(c => (c.dst || "").trim()).join("\n\n");
    downloadText(filename, out);
    textSafe("runInfo", `已导出：${filename}`);
}

/* =========================
   Chunk UI actions
========================= */
async function onChunkAction(e) {
    const act = e.target.getAttribute("data-act");
    const id = e.target.getAttribute("data-id");
    const chunk = App.chunks.find(c => c.id === id);
    if (!chunk) return;

    // 未生成 general_prompt 前禁止任何 chunk 操作
    if (!App.promptText.trim()) return;

    if (act === "togglePrompt") {
        if (isBusy()) return;

        chunk.showPrompt = !chunk.showPrompt;

        // 第一次展开时，自动复制 general_prompt 作为初始值
        if (chunk.showPrompt && (!chunk.promptOverride || !chunk.promptOverride.trim())) {
            chunk.promptOverride = App.promptText || "";
        }

        renderChunks();
        return;
    }

    if (act === "savePrompt") {
        if (isBusy()) return;
        const ta = el("prompt-" + chunk.id);
        chunk.promptOverride = (ta ? ta.value : "") || "";
        textSafe("runInfo", `已保存 Chunk #${chunk.idx + 1} 的专用 Prompt。`);
        renderChunks();
        return;
    }

    if (act === "saveAndRedo") {
        if (isBusy()) return;
        const ta = el("prompt-" + chunk.id);
        chunk.promptOverride = (ta ? ta.value : "") || "";

        textSafe("runInfo", `重做 Chunk #${chunk.idx + 1}...`);
        try {
            chunk.status = STATUS.WAIT;
            chunk.exception = "";
            chunk.dst = "";
            renderChunks();

            await convertOneChunk(chunk);

            textSafe(
                "runInfo",
                chunk.status === STATUS.OK
                    ? `Chunk #${chunk.idx + 1} 成功。`
                    : `Chunk #${chunk.idx + 1} 失败，请调整提示词后重做。`
            );
        } finally {
            // 这里不需要 setBusy(false)，因为本分支没有 setBusy(true)
            updateCounters();
        }
        return;
    }

    if (act === "redoOne") {
        if (isBusy()) return;

        setBusy(true);
        textSafe("runInfo", `重做 Chunk #${chunk.idx + 1}...`);

        try {
            chunk.status = STATUS.WAIT;
            chunk.exception = "";
            chunk.dst = "";
            renderChunks();

            await convertOneChunk(chunk);

            textSafe(
                "runInfo",
                chunk.status === STATUS.OK
                    ? `Chunk #${chunk.idx + 1} 成功。`
                    : `Chunk #${chunk.idx + 1} 失败，请调整提示词后重做。`
            );
        } finally {
            // ✅ 关键修复：busy 结束后，必须再 render 一次，把 disabled/readonly 恢复
            setBusy(false);
            renderChunks(); // ← 这一行是修复点
            // renderChunks() 内部会 updateCounters()，这里可不再重复调用
        }
        return;
    }

    if (act === "copyOut") {
        const t = chunk.dst || "";
        if (!t.trim()) return;
        try {
            await navigator.clipboard.writeText(t);
            textSafe("runInfo", `已复制 Chunk #${chunk.idx + 1} 的结果。`);
        } catch {
            textSafe("runInfo", "复制失败：浏览器不允许剪贴板操作。");
        }
    }
}


/* =========================
   Params lock/unlock
   - 仅【确认参数】会 refresh+save（保存点 #1）
========================= */
function onConfirmParams() {
    if (isBusy()) return;
    if (!validateBeforeRequest()) {
        textSafe("paramsStatus", "请先修正必填参数后再确认。");
        return;
    }
    if (!App.sourceSqlText.trim()) {
        textSafe("runInfo", "请先上传 SQL 文件，再点击“确认参数”。");
        return;
    }

    App.paramsConfirmed = true;
    syncParamsButtons();

    const p = readSharedParams();
    const currentUserPrompt = valSafe("specialNeeds");

// 当用户 Prompt 不为空：用新参数替换其中的旧参数；否则才写默认模板
    if (currentUserPrompt && currentUserPrompt.trim()) {
        const replaced = replacePromptParamsWithNew(currentUserPrompt, p);
        setValSafe("specialNeeds", replaced);
    } else {
        setValSafe("specialNeeds", buildDefaultSpecialNeedsPrompt());
    }

    autoResizeTextarea($("specialNeeds"));

    // prompt 清理
    App.promptText = "";
    setValSafe("generatedPrompt", "");
    setPromptBadge(STATUS.WAIT, "未生成");

    // ✅ 关键：确认参数后才分片
    rebuildChunksFromCurrentSql({clearResults: true});

    // 保存点 #1：确认参数后保存（此时 chunks 已生成）
    saveSessionToStorage();

    textSafe("runInfo", `参数已确认并锁定：已分片 ${App.chunks.length} 个 chunk。请生成 General Prompt 后再执行 Convert All。`);

    updateCounters();
    renderChunks();
}

function onEditParams() {
    if (isBusy()) return;

    // 解锁第一部分（不 refresh、不 save）
    App.paramsConfirmed = false;
    syncParamsButtons();

    // 解锁编辑通常意味着 prompt 不再可信：立刻清理（不保存）
    App.promptText = "";
    setValSafe("generatedPrompt", "");
    setPromptBadge(STATUS.WAIT, "未生成");
    textSafe("runInfo", "已进入参数编辑模式：第一部分已解锁。修改完成后请点击“确认参数”。");

    updateCounters();
    renderChunks();
}

/* =========================
   Init
========================= */
function init() {
    document.addEventListener("dragover", (e) => e.preventDefault());
    document.addEventListener("drop", (e) => e.preventDefault());
    const restored = loadSessionFromStorage();

    // session pill
    textSafe("sessionPill", App.sessionId.slice(0, 8));

    // 若未恢复：保持初始状态（第一部分可编辑）
    if (!restored) {
        App.paramsConfirmed = false;
        syncParamsButtons();
        syncSqlDropUI();
        syncTemplateDropUI();
        setPromptBadge(STATUS.WAIT, "未生成");
        setValSafe("generatedPrompt", "");
        setValSafe("specialNeeds", "");
        App.autoConvertAfterPrompt = false;
        document.getElementById("autoConvertToggle") && (document.getElementById("autoConvertToggle").checked = false);
    }

    // 监听参数输入（仅做校验/按钮状态刷新，不保存、不刷新 chunk）
    const paramIds = ["sourceFormat", "destFormat", "targetSchema", "destLang", "mergeN", "concurrency"];
    paramIds.forEach(id => {
        el(id)?.addEventListener("input", () => {
            validateBeforeRequest();
            syncSqlDropUI();
            updateCounters();
        });
    });

    // 用户 Prompt 输入：只做 UI resize/按钮刷新（不保存）
    el("specialNeeds")?.addEventListener("input", () => {
        autoResizeTextarea(el("specialNeeds"));
        updateCounters();
    });

    // 拖拽上传
    if (el("sqlDrop")) bindDropZone(el("sqlDrop"), loadSqlFile);
    if (el("tplDrop")) bindDropZone(el("tplDrop"), loadTemplateFile);

    el("pickSqlBtn")?.addEventListener("click", () => el("sqlFile")?.click());
    el("sqlFile")?.addEventListener("change", async () => {
        const f = el("sqlFile")?.files && el("sqlFile").files[0];
        if (f) await loadSqlFile(f);
    });

    el("pickTplBtn")?.addEventListener("click", () => el("tplFile")?.click());
    el("tplFile")?.addEventListener("change", async () => {
        const f = el("tplFile")?.files && el("tplFile").files[0];
        if (f) await loadTemplateFile(f);
    });

    // 按钮绑定
    el("btnConfirmParams")?.addEventListener("click", onConfirmParams);
    el("btnEditParams")?.addEventListener("click", onEditParams);
    el("btnGeneratePrompt")?.addEventListener("click", onGeneratePrompt);
    el("btnConvertAll")?.addEventListener("click", onConvertAll);
    el("btnRedoFailed")?.addEventListener("click", onRedoFailed);
    el("btnExport")?.addEventListener("click", onExport);

    document.getElementById("autoConvertToggle")?.addEventListener("change", (e) => {
        App.autoConvertAfterPrompt = !!e.target.checked;
        saveSessionToStorage();
    });

    validateBeforeRequest();
    applySectionMasks();
    updateCounters();
    renderChunks();

    if (restored) {
        textSafe("runInfo", "已从浏览器恢复上一次会话。");
    }

    bindChunkFocusExpand();

}

init();