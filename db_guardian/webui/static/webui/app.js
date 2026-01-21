const state = {
  access: localStorage.getItem("av_access"),
  refresh: localStorage.getItem("av_refresh"),
  user: null,
  instances: [],
  teams: [],
  sqlLastResult: null,
};

const view = document.getElementById("view");
const titleEl = document.getElementById("view-title");
const overlay = document.getElementById("login-overlay");
const loginForm = document.getElementById("login-form");
const loginError = document.getElementById("login-error");
const userBadge = document.getElementById("user-badge");
const modalOverlay = document.getElementById("modal-overlay");
const modalTitle = document.getElementById("modal-title");
const modalBody = document.getElementById("modal-body");
const modalClose = document.getElementById("modal-close");

const routes = {
  dashboard: { title: "概览", render: renderDashboard },
  instances: { title: "实例列表", render: renderInstances },
  databases: { title: "数据库", render: renderDatabases },
  metrics: { title: "监控指标", render: renderMetrics },
  "sql-terminal": { title: "SQL 终端", render: renderSqlTerminal },
  "sql-history": { title: "SQL 执行历史", render: renderSqlHistory },
  "backup-strategies": { title: "备份策略", render: renderBackupStrategies },
  "backup-records": { title: "备份记录", render: renderBackupRecords },
  "backup-tasks": { title: "定时任务", render: renderBackupTasks },
  "backup-restore": { title: "恢复", render: renderBackupRestore },
  "auth-users": { title: "用户", render: renderAuthUsers },
  "auth-teams": { title: "团队", render: renderAuthTeams },
  "auth-roles": { title: "角色", render: renderAuthRoles },
  "auth-permissions": { title: "权限", render: renderAuthPermissions },
  account: { title: "修改密码", render: renderAccount },
};

async function downloadWithAuth(url) {
  const response = await fetch(url, {
    headers: state.access ? { Authorization: `Bearer ${state.access}` } : {},
  });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(formatApiError(data) || "下载失败");
  }
  const blob = await response.blob();
  const disposition = response.headers.get("Content-Disposition") || "";
  const match = disposition.match(/filename\\*?=(?:UTF-8''|\"?)([^\";]+)/i);
  const filename = match ? decodeURIComponent(match[1]) : "backup.sql";
  const link = document.createElement("a");
  link.href = window.URL.createObjectURL(blob);
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(link.href);
}

function setActiveNav(route) {
  document.querySelectorAll(".nav-link").forEach((link) => {
    link.classList.toggle("active", link.getAttribute("href") === `#${route}`);
  });
}

function showLogin(show) {
  overlay.classList.toggle("hidden", !show);
  document.getElementById("app").style.display = show ? "none" : "flex";
}

function openModal(title, html) {
  modalTitle.textContent = title;
  modalBody.innerHTML = html;
  modalOverlay.classList.remove("hidden");
}

function closeModal() {
  modalOverlay.classList.add("hidden");
  modalBody.innerHTML = "";
}

modalClose?.addEventListener("click", closeModal);
modalOverlay?.addEventListener("click", (e) => {
  if (e.target === modalOverlay) closeModal();
});

  function escapeHtml(value) {
    if (value === null || value === undefined) return "";
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function formatApiError(data) {
    if (!data || typeof data !== "object") return "";
    if (data.detail) return String(data.detail);
    if (data.message) return String(data.message);
    if (data.errors) return JSON.stringify(data.errors);
    const entries = Object.entries(data);
    if (!entries.length) return "";
    return entries
      .map(([key, value]) => {
        if (Array.isArray(value)) return `${key}: ${value.join("，")}`;
        if (typeof value === "object") return `${key}: ${JSON.stringify(value)}`;
        return `${key}: ${value}`;
      })
      .join("；");
  }

async function apiFetch(path, options = {}) {
  const headers = options.headers || {};
  if (!(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
  }
  if (state.access) {
    headers.Authorization = `Bearer ${state.access}`;
  }

  const response = await fetch(path, { ...options, headers });
  if (response.status === 401 && state.refresh) {
    const refreshed = await refreshToken();
    if (refreshed) {
      return apiFetch(path, options);
    }
  }
    if (response.status === 204) return null;
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const message = formatApiError(data) || "请求失败";
      throw new Error(message);
    }
    return data;
  }

  async function ensureInstances() {
    if (state.instances.length) return state.instances;
    const data = await apiFetch("/api/instances/");
    state.instances = normalizeList(data);
    return state.instances;
  }

  async function ensureTeams() {
    if (state.teams.length) return state.teams;
    const data = await apiFetch("/api/auth/teams/");
    state.teams = normalizeList(data);
    return state.teams;
  }

async function refreshToken() {
  try {
    const data = await fetch("/api/auth/token/refresh/", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh: state.refresh }),
    }).then((res) => res.json());
    if (data.access) {
      state.access = data.access;
      if (data.refresh) {
        state.refresh = data.refresh;
        localStorage.setItem("av_refresh", data.refresh);
      }
      localStorage.setItem("av_access", data.access);
      return true;
    }
  } catch (err) {
    console.error(err);
  }
  logout();
  return false;
}

async function login(username, password) {
  const data = await fetch("/api/auth/token/", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  }).then((res) => res.json());

  if (!data.access) {
    throw new Error(data.detail || "登录失败");
  }
  state.access = data.access;
  state.refresh = data.refresh;
  state.user = data.user || null;
  localStorage.setItem("av_access", data.access);
  localStorage.setItem("av_refresh", data.refresh);
  showLogin(false);
  await loadUser();
}

async function loadUser() {
  try {
    const user = await apiFetch("/api/auth/users/me/");
    state.user = user;
    userBadge.textContent = `${user.username || "已登录"}`;
  } catch {
    userBadge.textContent = "未登录";
  }
}

function logout() {
  state.access = null;
  state.refresh = null;
  state.user = null;
  localStorage.removeItem("av_access");
  localStorage.removeItem("av_refresh");
  showLogin(true);
  userBadge.textContent = "未登录";
}

function normalizeList(data) {
  if (!data) return [];
  return Array.isArray(data) ? data : data.results || [];
}

function setView(title, html) {
  titleEl.textContent = title;
  view.innerHTML = html;
}

function renderJsonEditor(title, json, onSubmit) {
  view.insertAdjacentHTML(
    "beforeend",
    `<div class="card">
      <h3>${escapeHtml(title)}</h3>
      <textarea id="json-editor" style="width:100%;min-height:160px;">${escapeHtml(
        JSON.stringify(json, null, 2)
      )}</textarea>
      <div class="toolbar">
        <button class="primary" id="json-save">保存</button>
      </div>
    </div>`
  );
  document.getElementById("json-save").onclick = () => {
    try {
      const value = JSON.parse(document.getElementById("json-editor").value);
      onSubmit(value);
    } catch (err) {
      alert("JSON 格式错误");
    }
  };
}

async function openInstanceForm(instance = null) {
  const teams = await ensureTeams();
  if (!teams.length) {
    alert("请先创建团队");
    return;
  }
  const data = {
    alias: instance?.alias || "",
    host: instance?.host || "",
    port: instance?.port || 3306,
    username: instance?.username || "root",
    password: "",
    team: instance?.team || "",
    deployment_type: instance?.deployment_type || "docker",
    docker_container_name: instance?.docker_container_name || "",
    mysql_service_name: instance?.mysql_service_name || "",
    data_dir: instance?.data_dir || "",
    remote_backup_root: instance?.remote_backup_root || "",
    ssh_host: instance?.ssh_host || "",
    ssh_port: instance?.ssh_port || 22,
    ssh_user: instance?.ssh_user || "",
    ssh_password: "",
    ssh_key_path: instance?.ssh_key_path || "",
  };

  const teamOptions = teams
    .map(
      (t) =>
        `<option value="${t.id}" ${
          String(t.id) === String(data.team) ? "selected" : ""
        }>${escapeHtml(t.name)}</option>`
    )
    .join("");

  openModal(
    instance ? "编辑实例" : "新增实例",
    `<form id="instance-form">
      <div class="modal-grid">
        <label>别名<input name="alias" value="${escapeHtml(data.alias)}" required></label>
        <label>主机<input name="host" value="${escapeHtml(data.host)}" required></label>
        <label>端口<input name="port" type="number" value="${data.port}" required></label>
        <label>用户名<input name="username" value="${escapeHtml(data.username)}" required></label>
        <label>密码<input name="password" type="password" placeholder="${instance ? "留空不修改" : ""}"></label>
        <label>团队
          <select name="team" required>
            <option value="">请选择</option>
            ${teamOptions}
          </select>
        </label>
        <label>部署方式
          <select name="deployment_type">
            <option value="docker" ${data.deployment_type === "docker" ? "selected" : ""}>Docker</option>
            <option value="systemd" ${data.deployment_type === "systemd" ? "selected" : ""}>系统服务</option>
          </select>
        </label>
        <label>容器名称<input name="docker_container_name" value="${escapeHtml(data.docker_container_name)}"></label>
        <label>服务名称<input name="mysql_service_name" value="${escapeHtml(data.mysql_service_name)}"></label>
        <label>数据目录<input name="data_dir" value="${escapeHtml(data.data_dir)}"></label>
        <label>远程备份根目录<input name="remote_backup_root" value="${escapeHtml(data.remote_backup_root)}"></label>
        <label>SSH 主机<input name="ssh_host" value="${escapeHtml(data.ssh_host)}"></label>
        <label>SSH 端口<input name="ssh_port" type="number" value="${data.ssh_port}"></label>
        <label>SSH 用户<input name="ssh_user" value="${escapeHtml(data.ssh_user)}"></label>
        <label>SSH 密码<input name="ssh_password" type="password"></label>
        <label>SSH 密钥路径<input name="ssh_key_path" value="${escapeHtml(data.ssh_key_path)}"></label>
      </div>
      <div class="toolbar" style="margin-top:16px;">
        <button class="primary" type="submit">保存</button>
      </div>
    </form>`
  );

  document.getElementById("instance-form").onsubmit = async (e) => {
    e.preventDefault();
    const formData = new FormData(e.target);
    const payload = Object.fromEntries(formData.entries());
    payload.port = Number(payload.port || 3306);
    payload.ssh_port = Number(payload.ssh_port || 22);
    payload.team = payload.team ? Number(payload.team) : null;

    if (!payload.team) {
      alert("请选择团队");
      return;
    }
    if (payload.deployment_type === "docker" && !payload.docker_container_name) {
      alert("Docker 部署方式必须填写容器名称");
      return;
    }
    if (payload.deployment_type === "systemd" && !payload.mysql_service_name) {
      alert("系统服务部署必须填写服务名称");
      return;
    }
    if (payload.ssh_host && !payload.ssh_user) {
      alert("配置 SSH 主机时必须填写 SSH 用户");
      return;
    }
    if (!payload.password) delete payload.password;
    if (!payload.ssh_password) delete payload.ssh_password;

    const url = instance ? `/api/instances/${instance.id}/` : "/api/instances/";
    const method = instance ? "PATCH" : "POST";
    await apiFetch(url, { method, body: JSON.stringify(payload) });
    closeModal();
    await renderInstances();
  };
}

async function renderDashboard() {
  const [instances, strategies, records] = await Promise.all([
    apiFetch("/api/instances/"),
    apiFetch("/api/backups/strategies/"),
    apiFetch("/api/backups/records/"),
  ]);
  setView(
    "概览",
    `<div class="card">
      <h3>总览</h3>
      <p>实例数量：${normalizeList(instances).length}</p>
      <p>备份策略：${normalizeList(strategies).length}</p>
      <p>备份记录：${normalizeList(records).length}</p>
    </div>`
  );
}

async function renderInstances() {
  const data = await apiFetch("/api/instances/");
  const instances = normalizeList(data);
  state.instances = instances;
  const rows = instances
    .map(
      (item) => `
      <tr>
        <td>${escapeHtml(item.alias)}</td>
        <td>${escapeHtml(item.host)}</td>
        <td>${escapeHtml(item.port)}</td>
        <td>${escapeHtml(item.status)}</td>
        <td>
          <button class="ghost" data-action="refresh" data-id="${item.id}">刷新状态</button>
          <button class="ghost" data-action="edit" data-id="${item.id}">编辑</button>
          <button class="danger" data-action="delete" data-id="${item.id}">删除</button>
        </td>
      </tr>`
    )
    .join("");

  setView(
    "实例列表",
    `<div class="card">
      <div class="toolbar">
        <button class="primary" id="add-instance">新增实例</button>
        <button class="ghost" id="sync-status">刷新全部</button>
      </div>
      <table>
        <thead><tr><th>别名</th><th>主机</th><th>端口</th><th>状态</th><th>操作</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`
  );

  document.getElementById("add-instance").onclick = () => {
    openInstanceForm();
  };

  document.getElementById("sync-status").onclick = renderInstances;

  view.querySelectorAll("button[data-action]").forEach((btn) => {
    btn.onclick = async () => {
      const id = btn.dataset.id;
      const action = btn.dataset.action;
      if (action === "refresh") {
        await apiFetch(`/api/instances/${id}/refresh-status/`, { method: "POST" });
        await renderInstances();
      }
      if (action === "edit") {
        const target = instances.find((i) => String(i.id) === id);
        openInstanceForm(target);
      }
      if (action === "delete") {
        if (!confirm("确认删除该实例？")) return;
        await apiFetch(`/api/instances/${id}/`, { method: "DELETE" });
        await renderInstances();
      }
    };
  });
}

  async function renderDatabases() {
    await ensureInstances();
    const cards = state.instances
      .map(
        (instance) => `
        <details class="db-instance" data-id="${instance.id}">
          <summary>
            <span>${escapeHtml(instance.alias)} (${escapeHtml(instance.host)}:${escapeHtml(instance.port)})</span>
            <span class="tag">${escapeHtml(instance.status || "")}</span>
          </summary>
          <div class="db-body">
            <div class="toolbar">
              <button class="ghost" data-action="refresh">刷新</button>
            </div>
            <div class="db-table"><span class="muted">展开后加载数据库列表</span></div>
          </div>
        </details>
      `
      )
      .join("");

    setView(
      "数据库",
      `<div class="card">
        <div class="toolbar">
          <button class="ghost" id="db-refresh-all">刷新全部</button>
        </div>
        <div id="db-accordion">
          ${cards || '<p class="muted">暂无实例</p>'}
        </div>
      </div>`
    );

    async function loadInstance(details, force = true) {
      const id = details.dataset.id;
      const table = details.querySelector(".db-table");
      table.innerHTML = `<span class="muted">加载中...</span>`;
      try {
        const list = await apiFetch(`/api/instances/${id}/databases/?refresh=${force ? 1 : 0}`);
        const rows = normalizeList(list)
          .map(
            (db) =>
              `<tr><td>${escapeHtml(db.name)}</td><td>${db.table_count}</td><td>${db.size_mb}</td></tr>`
          )
          .join("");
        table.innerHTML = `
          <table>
            <thead><tr><th>数据库</th><th>表数量</th><th>大小(MB)</th></tr></thead>
            <tbody>${rows}</tbody>
          </table>`;
        details.dataset.loaded = "true";
      } catch (err) {
        table.innerHTML = `<span class="error-text">加载失败：${escapeHtml(err.message)}</span>`;
      }
    }

    document.querySelectorAll(".db-instance").forEach((details) => {
      const refreshBtn = details.querySelector('[data-action="refresh"]');
      refreshBtn.onclick = (event) => {
        event.preventDefault();
        event.stopPropagation();
        loadInstance(details, true);
      };
      details.addEventListener("toggle", () => {
        if (details.open && !details.dataset.loaded) {
          loadInstance(details, true);
        }
      });
    });

    const refreshAll = document.getElementById("db-refresh-all");
    if (refreshAll) {
      refreshAll.onclick = async () => {
        const all = Array.from(document.querySelectorAll(".db-instance"));
        for (const details of all) {
          await loadInstance(details, true);
        }
      };
    }
  }

  async function renderMetrics() {
    await ensureInstances();
  const options = state.instances
    .map((i) => `<option value="${i.id}">${escapeHtml(i.alias)}</option>`)
    .join("");
  setView(
    "监控指标",
    `<div class="card">
      <div class="toolbar">
        <label>实例：
          <select id="metrics-instance">${options}</select>
        </label>
        <button class="ghost" id="metrics-refresh">刷新</button>
      </div>
      <div id="metrics-list"></div>
    </div>`
  );
  const select = document.getElementById("metrics-instance");
  const load = async () => {
    const data = await apiFetch(`/api/instances/${select.value}/metrics/?hours=24`);
    const list = normalizeList(data);
    const latest = list[0];
    if (!latest) {
      document.getElementById("metrics-list").innerHTML = `<p>暂无监控数据</p>`;
      return;
    }

    const cpu = Number(latest.cpu_usage || 0);
    const mem = Number(latest.memory_usage || 0);
    const disk = Number(latest.disk_usage || 0);

    const chart = (value, label) => `
      <div class="metric-card">
        <div class="pie" style="--value:${Math.min(100, Math.max(0, value))};"></div>
        <div class="metric-info">
          <span>${label}</span>
          <strong>${value.toFixed(1)}%</strong>
        </div>
      </div>`;

    document.getElementById("metrics-list").innerHTML = `
      <div class="metrics-grid">
        ${chart(cpu, "CPU")}
        ${chart(mem, "内存")}
        ${chart(disk, "磁盘")}
      </div>
      <div class="metric-meta">更新时间：${escapeHtml(latest.timestamp)}</div>
    `;
  };
  document.getElementById("metrics-refresh").onclick = load;
    await load();
  }

  function buildAsciiTable(columns, rows) {
    if (!columns || !columns.length) return "(无结果)";
    const safeRows = Array.isArray(rows) ? rows : [];
    const widths = columns.map((col) => {
      const headerLen = String(col).length;
      const rowLen = safeRows.reduce((max, row) => {
        const value = row && row[col] !== undefined ? String(row[col]) : "";
        return Math.max(max, value.length);
      }, 0);
      return Math.max(headerLen, rowLen);
    });
    const line = "+" + widths.map((w) => "-".repeat(w + 2)).join("+") + "+";
    const formatRow = (values) =>
      "|" +
      values
        .map((value, idx) => {
          const text = String(value ?? "");
          return ` ${text}${" ".repeat(widths[idx] - text.length)} `;
        })
        .join("|") +
      "|";
    const header = formatRow(columns);
    const body = safeRows.map((row) => formatRow(columns.map((c) => row?.[c] ?? ""))).join("\n");
    return [line, header, line, body || "", line].filter(Boolean).join("\n");
  }

  function renderSqlOutput(result, mode) {
    const output = document.getElementById("sql-output");
    if (!output) return;
    if (!result) {
      output.textContent = "";
      return;
    }
    if (mode === "json") {
      output.textContent = JSON.stringify(result, null, 2);
      return;
    }
    const rows = Array.isArray(result.data) ? result.data.length : 0;
    const affected = result.rows_affected ?? rows;
    const elapsed = result.execution_time_ms ?? 0;
    const type = result.sql_type || "SQL";
    const meta = `OK (${type}) rows=${affected} time=${elapsed}ms`;
    const table = buildAsciiTable(result.columns || [], result.data || []);
    output.textContent = `${meta}\n${table}`;
  }

  async function renderSqlTerminal() {
    await ensureInstances();
    const options = state.instances
      .map((i) => `<option value="${i.id}">${escapeHtml(i.alias)}</option>`)
      .join("");
    setView(
      "SQL 终端",
      `<div class="card">
        <div class="toolbar">
          <label>实例：
            <select id="sql-instance">${options}</select>
          </label>
          <label>数据库：
            <input id="sql-db" placeholder="可选" />
          </label>
          <label>输出：
            <select id="sql-output-mode">
              <option value="table">表格输出</option>
              <option value="json">JSON 输出</option>
            </select>
          </label>
          <button class="primary" id="sql-run">执行</button>
        </div>
        <textarea id="sql-text" style="width:100%;min-height:140px;" placeholder="输入 SQL"></textarea>
        <pre id="sql-output" style="margin-top:12px;"></pre>
      </div>`
    );
    const modeSelect = document.getElementById("sql-output-mode");
    const savedMode = localStorage.getItem("av_sql_output") || "table";
    modeSelect.value = savedMode;
    modeSelect.onchange = () => {
      localStorage.setItem("av_sql_output", modeSelect.value);
      renderSqlOutput(state.sqlLastResult, modeSelect.value);
    };
    document.getElementById("sql-run").onclick = async () => {
      const instanceId = document.getElementById("sql-instance").value;
      const sql = document.getElementById("sql-text").value.trim();
      const rawDb = document.getElementById("sql-db").value.trim();
      const database = rawDb.replace(/;+\s*$/, "");
      if (!sql) {
        document.getElementById("sql-output").textContent = "ERROR: 请输入 SQL 语句";
        return;
      }
      try {
        const result = await apiFetch(`/api/instances/${instanceId}/query/`, {
          method: "POST",
          body: JSON.stringify({ sql, database }),
        });
        state.sqlLastResult = result;
        renderSqlOutput(result, modeSelect.value);
      } catch (err) {
        state.sqlLastResult = null;
        document.getElementById("sql-output").textContent = `ERROR: ${err.message || "执行失败"}`;
      }
    };
  }

async function renderSqlHistory() {
  const data = await apiFetch("/api/sql/history/");
  const rows = normalizeList(data)
    .map(
      (item) => `<tr>
      <td>${escapeHtml(item.sql_type)}</td>
      <td>${escapeHtml(item.database_name)}</td>
      <td>${escapeHtml(item.executed_at)}</td>
      <td>${escapeHtml(item.status)}</td>
    </tr>`
    )
    .join("");
  setView(
    "SQL 执行历史",
    `<div class="card">
      <table>
        <thead><tr><th>类型</th><th>数据库</th><th>时间</th><th>状态</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`
  );
}

async function renderBackupStrategies() {
  const data = await apiFetch("/api/backups/strategies/");
  const rows = normalizeList(data)
    .map(
      (s) => `<tr>
        <td>${escapeHtml(s.name)}</td>
        <td>${escapeHtml(s.backup_type_display)}</td>
        <td>${s.is_enabled ? "启用" : "禁用"}</td>
        <td>
          <button class="ghost" data-action="enable" data-id="${s.id}">启用</button>
          <button class="ghost" data-action="disable" data-id="${s.id}">禁用</button>
          <button class="ghost" data-action="edit" data-id="${s.id}">编辑</button>
          <button class="danger" data-action="delete" data-id="${s.id}">删除</button>
        </td>
      </tr>`
    )
    .join("");
  setView(
    "备份策略",
    `<div class="card">
      <div class="toolbar">
        <button class="primary" id="add-strategy">新增策略</button>
      </div>
      <table>
        <thead><tr><th>名称</th><th>类型</th><th>状态</th><th>操作</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`
  );
  document.getElementById("add-strategy").onclick = () => {
    renderJsonEditor("新增策略", {
      name: "",
      instance_id: null,
      cron_expression: "0 2 * * *",
      backup_type: "full",
      retention_days: 7,
      is_enabled: true,
      storage_mode: "default",
    }, async (value) => {
      await apiFetch("/api/backups/strategies/", { method: "POST", body: JSON.stringify(value) });
      await renderBackupStrategies();
    });
  };
  view.querySelectorAll("button[data-action]").forEach((btn) => {
    btn.onclick = async () => {
      const id = btn.dataset.id;
      const action = btn.dataset.action;
      if (action === "enable") await apiFetch(`/api/backups/strategies/${id}/enable/`, { method: "POST" });
      if (action === "disable") await apiFetch(`/api/backups/strategies/${id}/disable/`, { method: "POST" });
      if (action === "edit") {
        const target = normalizeList(data).find((i) => String(i.id) === id);
        renderJsonEditor("编辑策略", target, async (value) => {
          await apiFetch(`/api/backups/strategies/${id}/`, { method: "PATCH", body: JSON.stringify(value) });
          await renderBackupStrategies();
        });
        return;
      }
      if (action === "delete") {
        if (!confirm("确认删除该策略？")) return;
        await apiFetch(`/api/backups/strategies/${id}/`, { method: "DELETE" });
      }
      await renderBackupStrategies();
    };
  });
}

async function renderBackupRecords() {
  const data = await apiFetch("/api/backups/records/");
  const rows = normalizeList(data)
    .map(
      (r) => `<tr>
        <td>${escapeHtml(r.instance_alias)}</td>
        <td>${escapeHtml(r.backup_type_display)}</td>
        <td>${escapeHtml(r.status_display)}</td>
        <td>${escapeHtml(r.created_at)}</td>
        <td>
          <button class="ghost" data-action="download" data-id="${r.id}">下载</button>
          <button class="ghost" data-action="restore" data-id="${r.id}">恢复</button>
          <button class="ghost" data-action="verify" data-id="${r.id}">验证</button>
          <button class="danger" data-action="delete" data-id="${r.id}">删除</button>
        </td>
      </tr>`
    )
    .join("");
  setView(
    "备份记录",
    `<div class="card">
      <table>
        <thead><tr><th>实例</th><th>类型</th><th>状态</th><th>时间</th><th>操作</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`
  );
  view.querySelectorAll("button[data-action]").forEach((btn) => {
    btn.onclick = async () => {
        const id = btn.dataset.id;
        const action = btn.dataset.action;
        if (action === "download") {
          try {
            await downloadWithAuth(`/api/backups/records/${id}/download/`);
          } catch (err) {
            alert(err.message || "下载失败");
          }
        }
        if (action === "restore") {
        const target = prompt("目标数据库（可选）");
        await apiFetch(`/api/backups/records/${id}/restore/`, {
          method: "POST",
          body: JSON.stringify({ target_database: target || "", confirm: true }),
        });
        alert("恢复任务已提交");
      }
      if (action === "verify") {
        await apiFetch(`/api/backups/records/${id}/verify/`, { method: "POST" });
        alert("验证任务已创建");
      }
      if (action === "delete") {
        if (!confirm("确认删除该记录？")) return;
        await apiFetch(`/api/backups/records/${id}/`, { method: "DELETE" });
      }
      await renderBackupRecords();
    };
  });
}

async function renderBackupTasks() {
  const data = await apiFetch("/api/backups/oneoff-tasks/");
  const rows = normalizeList(data)
    .map(
      (t) => `<tr>
        <td>${escapeHtml(t.name)}</td>
        <td>${escapeHtml(t.backup_type_display)}</td>
        <td>${escapeHtml(t.status_display)}</td>
        <td>${escapeHtml(t.run_at)}</td>
        <td>
          <button class="ghost" data-action="run" data-id="${t.id}">立即执行</button>
          <button class="danger" data-action="delete" data-id="${t.id}">删除</button>
        </td>
      </tr>`
    )
    .join("");
  setView(
    "定时任务",
    `<div class="card">
      <div class="toolbar">
        <button class="primary" id="add-task">新增任务</button>
      </div>
      <table>
        <thead><tr><th>名称</th><th>类型</th><th>状态</th><th>执行时间</th><th>操作</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`
  );
  document.getElementById("add-task").onclick = () => {
    renderJsonEditor("新增定时任务", {
      name: "",
      instance_id: null,
      run_at: new Date().toISOString(),
      backup_type: "full",
      storage_mode: "default",
    }, async (value) => {
      await apiFetch("/api/backups/oneoff-tasks/", { method: "POST", body: JSON.stringify(value) });
      await renderBackupTasks();
    });
  };
  view.querySelectorAll("button[data-action]").forEach((btn) => {
    btn.onclick = async () => {
      const id = btn.dataset.id;
      const action = btn.dataset.action;
      if (action === "run") {
        await apiFetch(`/api/backups/oneoff-tasks/${id}/run-now/`, { method: "POST" });
      }
      if (action === "delete") {
        if (!confirm("确认删除该任务？")) return;
        await apiFetch(`/api/backups/oneoff-tasks/${id}/`, { method: "DELETE" });
      }
      await renderBackupTasks();
    };
  });
}

  async function renderBackupRestore() {
  const records = await apiFetch("/api/backups/records/?status=success&ordering=-created_at");
  const rows = normalizeList(records)
    .map(
      (r) => `<tr>
        <td>${escapeHtml(r.instance_alias)}</td>
        <td>${escapeHtml(r.backup_type_display)}</td>
        <td>${escapeHtml(r.created_at)}</td>
        <td>
          <button class="ghost" data-action="restore" data-id="${r.id}">从记录恢复</button>
          <button class="ghost" data-action="download" data-id="${r.id}">下载</button>
        </td>
      </tr>`
    )
    .join("");

  setView(
    "恢复",
    `<div class="card">
      <h3>从备份记录恢复</h3>
      <table>
        <thead><tr><th>实例</th><th>类型</th><th>时间</th><th>操作</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    <div class="card">
      <h3>上传备份文件恢复</h3>
      <form id="restore-upload">
        <label>实例ID <input name="instance_id" required></label>
        <label>目标数据库 <input name="target_database"></label>
        <label>备份文件 <input name="backup_file" type="file" required></label>
        <button class="primary" type="submit">上传恢复</button>
      </form>
    </div>`
  );

  view.querySelectorAll("button[data-action]").forEach((btn) => {
    btn.onclick = async () => {
        const id = btn.dataset.id;
        const action = btn.dataset.action;
        if (action === "download") {
          try {
            await downloadWithAuth(`/api/backups/records/${id}/download/`);
          } catch (err) {
            alert(err.message || "下载失败");
          }
        }
        if (action === "restore") {
        const target = prompt("目标数据库（可选）");
        await apiFetch(`/api/backups/records/${id}/restore/`, {
          method: "POST",
          body: JSON.stringify({ target_database: target || "", confirm: true }),
        });
        alert("恢复任务已提交");
      }
    };
  });

  document.getElementById("restore-upload").onsubmit = async (e) => {
    e.preventDefault();
    const form = e.target;
    const formData = new FormData(form);
    formData.append("confirm", "true");
    await apiFetch("/api/backups/records/restore-upload/", { method: "POST", body: formData });
    alert("恢复任务已提交");
  };
}

async function renderAuthUsers() {
  let data = [];
  let teams = [];
  let roles = [];
  try {
    data = normalizeList(await apiFetch("/api/auth/users/"));
    teams = normalizeList(await apiFetch("/api/auth/teams/"));
    roles = normalizeList(await apiFetch("/api/auth/roles/"));
  } catch (err) {
    setView("用户", `<div class="card"><p>加载失败：${escapeHtml(err.message)}</p></div>`);
    return;
  }

  const canManageUsers = state.user && state.user.is_superuser;
  const rows = data
    .map((u) => {
      const canEdit = canManageUsers || (state.user && u.id === state.user.id);
      return `<tr>
        <td>${escapeHtml(u.username)}</td>
        <td>${escapeHtml(u.email || "")}</td>
        <td>${u.is_active ? "启用" : "停用"}</td>
        <td>
          ${canEdit ? `<button class="ghost" data-action="edit" data-id="${u.id}">编辑</button>` : ""}
          ${canManageUsers ? `<button class="danger" data-action="delete" data-id="${u.id}">删除</button>` : ""}
        </td>
      </tr>`;
    })
    .join("");

  const teamOptions = teams
    .map((t) => `<option value="${t.id}">${escapeHtml(t.name)}</option>`)
    .join("");
  const roleOptions = roles
    .map((r) => `<option value="${r.id}">${escapeHtml(r.name)}</option>`)
    .join("");

  setView(
    "用户",
    `<div class="card">
      <div class="toolbar">
        ${canManageUsers ? `<button class="primary" id="add-user">新增用户</button>` : `<span class="tag">仅超级管理员可新增/删除用户</span>`}
      </div>
      <div id="user-form-panel" class="hidden">
        <h3 id="user-form-title">新增用户</h3>
        <form id="user-form">
          <div class="form-grid">
            <label>用户名<input name="username" required></label>
            <label>邮箱<input name="email" type="email" required></label>
            <label>手机号<input name="phone"></label>
            <label>初始密码<input name="password" type="password" required></label>
            <label>激活状态
              <select name="is_active">
                <option value="true">启用</option>
                <option value="false">停用</option>
              </select>
            </label>
            <label>所属团队（可选）
              <select name="team_ids" multiple>${teamOptions}</select>
            </label>
            <label>角色（团队必填）
              <select name="role_id">
                <option value="">请选择角色</option>
                ${roleOptions}
              </select>
            </label>
          </div>
          <div class="toolbar">
            <button class="primary" type="submit">保存</button>
            <button class="ghost" type="button" id="cancel-user-form">取消</button>
          </div>
          <div class="error-text" id="user-form-error"></div>
        </form>
      </div>
      <table>
        <thead><tr><th>用户名</th><th>邮箱</th><th>状态</th><th>操作</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`
  );

  const panel = document.getElementById("user-form-panel");
  const form = document.getElementById("user-form");
  const errorBox = document.getElementById("user-form-error");

  function showForm(mode, user) {
    panel.classList.remove("hidden");
    errorBox.textContent = "";
    document.getElementById("user-form-title").textContent = mode === "edit" ? "编辑用户" : "新增用户";
    form.reset();

    if (mode === "edit") {
      form.username.value = user.username || "";
      form.email.value = user.email || "";
      form.phone.value = user.phone || "";
      form.is_active.value = user.is_active ? "true" : "false";
      form.password.value = "";
      form.username.disabled = true;
      form.password.disabled = true;
      form.team_ids.disabled = true;
      form.role_id.disabled = true;
    } else {
      form.username.disabled = false;
      form.password.disabled = false;
      form.team_ids.disabled = false;
      form.role_id.disabled = false;
    }

    form.dataset.mode = mode;
    form.dataset.userId = user ? user.id : "";
  }

  if (canManageUsers) {
    document.getElementById("add-user").onclick = () => showForm("create");
  }

  document.getElementById("cancel-user-form").onclick = () => {
    panel.classList.add("hidden");
  };

  form.onsubmit = async (e) => {
    e.preventDefault();
    errorBox.textContent = "";
    const payload = {
      username: form.username.value.trim(),
      email: form.email.value.trim(),
      phone: form.phone.value.trim(),
      is_active: form.is_active.value === "true",
    };
    const mode = form.dataset.mode;

    try {
      if (mode === "create") {
        payload.password = form.password.value;
        const selectedTeams = Array.from(form.team_ids.selectedOptions).map((o) => Number(o.value));
        if (selectedTeams.length) {
          payload.team_ids = selectedTeams;
          if (!form.role_id.value) {
            errorBox.textContent = "选择团队时必须指定角色";
            return;
          }
          payload.role_id = Number(form.role_id.value);
        }
        await apiFetch("/api/auth/users/", { method: "POST", body: JSON.stringify(payload) });
      } else if (mode === "edit") {
        const userId = form.dataset.userId;
        await apiFetch(`/api/auth/users/${userId}/`, { method: "PATCH", body: JSON.stringify(payload) });
      }
      panel.classList.add("hidden");
      await renderAuthUsers();
    } catch (err) {
      errorBox.textContent = err.message || "保存失败";
    }
  };

  view.querySelectorAll("button[data-action]").forEach((btn) => {
    btn.onclick = async () => {
      const id = Number(btn.dataset.id);
      const action = btn.dataset.action;
      const target = data.find((u) => u.id === id);
      if (action === "edit" && target) {
        showForm("edit", target);
        return;
      }
      if (action === "delete") {
        if (!confirm("确认删除该用户？")) return;
        try {
          await apiFetch(`/api/auth/users/${id}/`, { method: "DELETE" });
          await renderAuthUsers();
        } catch (err) {
          alert(err.message || "删除失败");
        }
      }
    };
  });
}

async function renderAuthTeams() {
  const data = await apiFetch("/api/auth/teams/");
  const rows = normalizeList(data)
    .map(
      (t) => `<tr>
        <td>${escapeHtml(t.name)}</td>
        <td>${escapeHtml(t.description || "")}</td>
      </tr>`
    )
    .join("");
  setView(
    "团队",
    `<div class="card">
      <div class="toolbar">
        <button class="primary" id="add-team">新增团队</button>
      </div>
      <table>
        <thead><tr><th>名称</th><th>描述</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`
  );
  document.getElementById("add-team").onclick = () => {
    renderJsonEditor("新增团队", { name: "", description: "" }, async (value) => {
      await apiFetch("/api/auth/teams/", { method: "POST", body: JSON.stringify(value) });
      await renderAuthTeams();
    });
  };
}

async function renderAuthRoles() {
  const data = await apiFetch("/api/auth/roles/");
  const rows = normalizeList(data)
    .map((r) => `<tr><td>${escapeHtml(r.name)}</td><td>${escapeHtml(r.slug)}</td></tr>`)
    .join("");
  setView(
    "角色",
    `<div class="card">
      <table>
        <thead><tr><th>名称</th><th>标识</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`
  );
}

async function renderAuthPermissions() {
  const data = await apiFetch("/api/auth/permissions/");
  const rows = normalizeList(data)
    .map((p) => `<tr><td>${escapeHtml(p.name)}</td><td>${escapeHtml(p.slug)}</td></tr>`)
    .join("");
  setView(
    "权限",
    `<div class="card">
      <table>
        <thead><tr><th>名称</th><th>标识</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`
  );
}

async function renderAccount() {
  setView(
    "修改密码",
    `<div class="card">
      <form id="change-password">
        <label>旧密码 <input type="password" name="old_password" required></label>
        <label>新密码 <input type="password" name="new_password" required></label>
        <button class="primary" type="submit">保存</button>
      </form>
    </div>`
  );
  document.getElementById("change-password").onsubmit = async (e) => {
    e.preventDefault();
    const form = new FormData(e.target);
    const payload = Object.fromEntries(form.entries());
    await apiFetch("/api/auth/users/change_password/", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    alert("密码修改成功");
  };
}

async function navigate() {
  const hash = location.hash.replace("#", "") || "dashboard";
  const route = routes[hash] ? hash : "dashboard";
  setActiveNav(route);
  const { title, render } = routes[route];
  await render();
  titleEl.textContent = title;
}

document.getElementById("logout-btn").onclick = logout;

loginForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  loginError.textContent = "";
  loginError.classList.add("hidden");
  const formData = new FormData(loginForm);
  try {
    await login(formData.get("username"), formData.get("password"));
    await navigate();
  } catch (err) {
    loginError.textContent = err.message || "登录失败";
    loginError.classList.remove("hidden");
  }
});

window.addEventListener("hashchange", navigate);

(async () => {
  if (!state.access) {
    showLogin(true);
    return;
  }
  await loadUser();
  await navigate();
})();

function initPetals() {
  const container = document.getElementById("petal-container");
  if (!container || container.children.length) return;
  const petalCount = 18;
  const colors = ["rgba(255,255,255,0.9)", "rgba(255,192,203,0.9)"];
  for (let i = 0; i < petalCount; i += 1) {
    const petal = document.createElement("div");
    petal.className = "petal";
    const size = 8 + Math.random() * 10;
    const left = Math.random() * 100;
    const duration = 22 + Math.random() * 20;
    const delay = -(Math.random() * duration);
    const drift = `${(Math.random() * 80 - 40).toFixed(1)}vw`;
    petal.style.left = `${left}vw`;
    petal.style.width = `${size}px`;
    petal.style.height = `${size * 0.6}px`;
    petal.style.background = colors[Math.floor(Math.random() * colors.length)];
    petal.style.animationDuration = `${duration}s`;
    petal.style.animationDelay = `${delay}s`;
    petal.style.setProperty("--drift", drift);
    container.appendChild(petal);
  }
}

initPetals();
