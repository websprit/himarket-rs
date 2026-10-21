const tokenKey = location.pathname.startsWith("/admin") ? "hm_admin_token" : "hm_dev_token";

async function api(path, options = {}) {
  const headers = { ...(options.headers || {}) };
  const token = localStorage.getItem(tokenKey);
  if (token) headers.Authorization = "Bearer " + token;
  if (options.body) headers["Content-Type"] = "application/json";
  const response = await fetch(path, { ...options, headers });
  const body = await response.json();
  if (!response.ok || body.code >= 400) throw new Error(body.message || response.statusText);
  return body.data;
}

function show(id) {
  document.querySelectorAll("section[data-panel]").forEach((node) => {
    node.hidden = node.dataset.panel !== id;
  });
  document.querySelectorAll("nav button").forEach((button) => {
    button.classList.toggle("active", button.dataset.panel === id);
  });
}

function table(columns, rows) {
  const head = "<tr>" + columns.map((column) => "<th>" + column[0] + "</th>").join("") + "</tr>";
  const body = rows.map((row) => "<tr>" + columns.map((column) => "<td>" + (column[1](row) ?? "") + "</td>").join("") + "</tr>").join("");
  return "<table>" + head + body + "</table>";
}
