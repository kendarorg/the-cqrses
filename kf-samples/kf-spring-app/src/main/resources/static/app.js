"use strict";

let currentUser = null;

const $ = (id) => document.getElementById(id);

async function api(method, path, body) {
    const opts = { method, headers: { "Content-Type": "application/json" } };
    if (body !== undefined) opts.body = JSON.stringify(body);
    const res = await fetch(path, opts);
    if (!res.ok) {
        let detail = res.statusText;
        try { detail = (await res.json()).message || detail; } catch (_) { /* ignore */ }
        throw new Error(detail);
    }
    return res.status === 204 ? null : res.json();
}

function setMsg(el, text, kind) {
    el.textContent = text || "";
    el.className = "msg" + (kind ? " " + kind : "");
}

$("login-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const username = $("username").value.trim();
    if (!username) return;
    try {
        const r = await api("POST", "/api/login", { username });
        currentUser = r.username;
        setMsg($("login-msg"), "", null);
        $("who").textContent = r.username;
        $("login-card").classList.add("hidden");
        $("app-card").classList.remove("hidden");
        await refresh();
    } catch (err) {
        setMsg($("login-msg"), err.message, "error");
    }
});

$("logout").addEventListener("click", () => {
    currentUser = null;
    $("app-card").classList.add("hidden");
    $("login-card").classList.remove("hidden");
    $("username").value = "";
});

$("op-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const op = {
        username: currentUser,
        type: $("op-type").value,
        amount: parseInt($("op-amount").value, 10),
        tag: $("op-tag").value.trim()
    };
    try {
        await api("POST", "/api/operations", op);
        $("op-amount").value = "";
        $("op-tag").value = "";
        setMsg($("op-msg"), "Recorded. (read model is eventually consistent — refreshing…)", "ok");
        // Re-fetch a few times to let the projection catch up.
        await pollRefresh();
    } catch (err) {
        setMsg($("op-msg"), err.message, "error");
    }
});

async function pollRefresh() {
    for (let i = 0; i < 6; i++) {
        await refresh();
        await new Promise((r) => setTimeout(r, 200));
    }
}

async function refresh() {
    if (!currentUser) return;
    const u = encodeURIComponent(currentUser);
    const [summary, byTag, ops] = await Promise.all([
        api("GET", `/api/summary?username=${u}`),
        api("GET", `/api/summary/by-tag?username=${u}`),
        api("GET", `/api/operations?username=${u}`)
    ]);

    $("t-in").textContent = summary.in;
    $("t-out").textContent = summary.out;
    $("t-net").textContent = summary.net;

    $("ops-table").querySelector("tbody").innerHTML = ops.map((o) =>
        `<tr><td>${o.type}</td><td>${o.amount}</td><td>${escapeHtml(o.tag)}</td>` +
        `<td>${new Date(o.ts).toLocaleString()}</td></tr>`).join("");

    $("tags-table").querySelector("tbody").innerHTML = byTag.map((t) =>
        `<tr><td>${escapeHtml(t.tag)}</td><td>${t.in}</td><td>${t.out}</td><td>${t.net}</td></tr>`).join("");
}

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) =>
        ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
