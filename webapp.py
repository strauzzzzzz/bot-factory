import asyncio
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
import urllib.parse
from collections import deque
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request
from telethon import TelegramClient
from telethon.errors import PhoneCodeInvalidError, SessionPasswordNeededError

from factory import (
    BASE_DIR,
    DATA_DIR,
    SESSIONS_DIR,
    factory_run,
    load_config,
    save_tokens,
    session_path,
)

PORT = int(os.environ.get("PORT", 5055))
LOG_RING = deque(maxlen=300)
LOG_LOCK = threading.Lock()
CONFIG_LOCK = threading.Lock()
CANCEL = {"set": False}
LOGINS = {}
CREATE_TASK = {"thread": None}
AUTH = {}
AUTH_TTL = 60 * 60 * 24


def log_line(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    with LOG_LOCK:
        LOG_RING.append(line)
    with open(DATA_DIR / "webapp.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)


def read_config():
    with CONFIG_LOCK:
        return json.loads((BASE_DIR / "config.json").read_text(encoding="utf-8"))


def write_config(cfg):
    with CONFIG_LOCK:
        (BASE_DIR / "config.json").write_text(
            json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
        )


class LoginSession:
    def __init__(self, phone):
        self.phone = phone
        self.loop = None
        self.queue = None
        self.status = "starting"
        self.error = ""
        self.client = None

    def submit(self, value):
        self.loop.call_soon_threadsafe(self.queue.put_nowait, value)


def run_login(phone, cfg):
    session = LOGINS[phone]
    session.status = "logging in"

    async def do_login():
        session.client = TelegramClient(
            str(session_path(phone)),
            cfg["api_id"],
            cfg["api_hash"],
            device_model="BotFactory",
        )
        client = session.client
        session.loop = asyncio.get_running_loop()
        session.queue = asyncio.Queue()
        await client.connect()
        if await client.is_user_authorized():
            me = await client.get_me()
            session.status = "done"
            log_line(f"{phone}: already logged in as @{me.username or me.first_name}")
            await client.disconnect()
            return
        try:
            sent = await client.send_code_request(phone)
        except Exception as e:
            session.status = "error"
            session.error = str(e)
            log_line(f"{phone}: code request failed - {e}")
            await client.disconnect()
            return
        session.status = "waiting_code"
        log_line(f"{phone}: code sent")
        while True:
            code = await session.queue.get()
            try:
                await client.sign_in(phone, code=code, phone_code_hash=sent.phone_code_hash)
                me = await client.get_me()
                session.status = "done"
                log_line(f"{phone}: logged in as @{me.username or me.first_name}")
                await client.disconnect()
                return
            except PhoneCodeInvalidError:
                session.status = "waiting_code"
                session.error = "invalid code, try again"
            except SessionPasswordNeededError:
                session.status = "waiting_password"
                log_line(f"{phone}: 2FA required")
                while True:
                    password = await session.queue.get()
                    try:
                        await client.sign_in(password=password)
                        me = await client.get_me()
                        session.status = "done"
                        log_line(f"{phone}: logged in as @{me.username or me.first_name}")
                        await client.disconnect()
                        return
                    except PhoneCodeInvalidError:
                        session.status = "waiting_code"
                        break
                    except Exception as e:
                        session.status = "waiting_password"
                        session.error = str(e)

    try:
        asyncio.run(do_login())
    except Exception as e:
        session.status = "error"
        session.error = str(e)
        log_line(f"{phone}: login error - {e}")


def run_create(cfg, count):
    phones = cfg["phone_numbers"]
    bases = cfg.get("username_bases") or [cfg["username_base"]] * len(phones)
    if len(bases) < len(phones):
        bases = bases + [cfg["username_base"]] * (len(phones) - len(bases))
    if count and count < len(phones):
        phones = phones[:count]
        bases = bases[:count]

    CANCEL["set"] = False
    log_line(f"=== create start: {len(phones)} account(s) ===")

    async def job():
        rows = await factory_run(
            cfg,
            phones,
            bases,
            progress=log_line,
            should_cancel=lambda: CANCEL["set"],
        )
        log_line(f"=== create finished: {len(rows)} bot(s) ===")

    asyncio.run(job())


app = Flask(__name__)


def initdata_secret(bot_token):
    return hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()


def validate_initdata(init_data, bot_token):
    try:
        pairs = urllib.parse.parse_qsl(init_data, keep_blank_values=True)
        data = {k: v for k, v in pairs}
        received = data.pop("hash", "")
        check_str = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
        calc = hmac.new(
            initdata_secret(bot_token), check_str.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(calc, received):
            return False, None
        if time.time() - int(data.get("auth_date", 0)) > AUTH_TTL:
            return False, None
        user = json.loads(data.get("user", "{}"))
        return True, user
    except Exception:
        return False, None


def is_admin_user(user_id):
    cfg = read_config()
    admins = cfg.get("admin_ids") or []
    return bool(admins) and user_id in admins


def auth_enabled():
    return bool(os.environ.get("WEB_PASSWORD") or read_config().get("control_bot_token"))


def issue_token():
    token = secrets.token_hex(16)
    AUTH[token] = time.time()
    return token


def check_auth():
    if not auth_enabled():
        return True
    token = request.cookies.get("botfactory_auth")
    if token and token in AUTH:
        if time.time() - AUTH[token] < AUTH_TTL:
            return True
        AUTH.pop(token, None)
    return False


@app.before_request
def gate():
    if request.path.startswith("/api/") and not check_auth():
        return jsonify({"ok": False, "error": "not authorized"}), 401


@app.route("/auth/password", methods=["POST"])
def auth_password():
    pw = (request.json or {}).get("password", "")
    expected = os.environ.get("WEB_PASSWORD", "")
    if expected and hmac.compare_digest(pw, expected):
        return jsonify({"ok": True, "token": issue_token()})
    return jsonify({"ok": False, "error": "wrong password"}), 403


@app.route("/auth/initdata", methods=["POST"])
def auth_initdata():
    init_data = (request.json or {}).get("initData", "")
    token = read_config().get("control_bot_token", "")
    if not token:
        return jsonify({"ok": False, "error": "initData auth not configured"}), 403
    ok, user = validate_initdata(init_data, token)
    if not ok:
        return jsonify({"ok": False, "error": "invalid initData"}), 403
    if not is_admin_user(user.get("id")):
        return jsonify({"ok": False, "error": "not an admin"}), 403
    return jsonify({"ok": True, "token": issue_token()})


@app.route("/")
def index():
    if check_auth():
        return PAGE
    return AUTH_PAGE


@app.route("/api/config", methods=["GET"])
def api_config_get():
    cfg = read_config()
    return jsonify(
        {
            "bot_name": cfg.get("bot_name", ""),
            "username_base": cfg.get("username_base", ""),
            "main_bot_username": cfg.get("main_bot_username", ""),
            "profile_pic": cfg.get("profile_pic", ""),
            "description": cfg.get("description", ""),
            "about_text": cfg.get("about_text", ""),
        }
    )


@app.route("/api/config", methods=["POST"])
def api_config_set():
    cfg = read_config()
    for key in (
        "bot_name",
        "username_base",
        "main_bot_username",
        "profile_pic",
        "description",
        "about_text",
    ):
        if key in request.json:
            cfg[key] = str(request.json[key]).strip()
    write_config(cfg)
    log_line("config saved")
    return jsonify({"ok": True})


@app.route("/api/phones", methods=["GET"])
def api_phones_get():
    cfg = read_config()
    phones = []
    for phone in cfg["phone_numbers"]:
        sess = LOGINS.get(phone)
        state = "session file" if session_path(phone).with_suffix(".session").exists() else "not logged in"
        if sess:
            state = sess.status
            if sess.error:
                state += f" ({sess.error})"
        elif state == "session file":
            state = "logged in (session)"
        phones.append({"phone": phone, "state": state})
    return jsonify({"phones": phones, "busy": CREATE_TASK["thread"] is not None and CREATE_TASK["thread"].is_alive()})


@app.route("/api/phones", methods=["POST"])
def api_phones_add():
    phone = (request.json or {}).get("phone", "").strip()
    if not phone.startswith("+"):
        phone = "+" + phone
    if not phone[1:].isdigit() or len(phone[1:]) < 5:
        return jsonify({"ok": False, "error": "invalid phone number"}), 400
    cfg = read_config()
    if phone not in cfg["phone_numbers"]:
        cfg["phone_numbers"].append(phone)
        write_config(cfg)
        log_line(f"added {phone}")
    return jsonify({"ok": True})


@app.route("/api/phones/<path:phone>", methods=["DELETE"])
def api_phones_del(phone):
    cfg = read_config()
    if phone in cfg["phone_numbers"]:
        cfg["phone_numbers"].remove(phone)
        write_config(cfg)
        log_line(f"removed {phone}")
    return jsonify({"ok": True})


@app.route("/api/login", methods=["POST"])
def api_login():
    phone = (request.json or {}).get("phone", "")
    cfg = read_config()
    if phone not in cfg["phone_numbers"]:
        return jsonify({"ok": False, "error": "add the phone first"}), 400
    if LOGINS.get(phone) and LOGINS[phone].status in ("logging in", "waiting_code", "waiting_password"):
        return jsonify({"ok": False, "error": "login already in progress"}), 400
    session = LoginSession(phone)
    LOGINS[phone] = session
    t = threading.Thread(target=run_login, args=(phone, cfg), daemon=True)
    t.start()
    return jsonify({"ok": True})


@app.route("/api/login/code", methods=["POST"])
def api_login_code():
    phone = (request.json or {}).get("phone", "")
    code = (request.json or {}).get("code", "").strip()
    sess = LOGINS.get(phone)
    if not sess or sess.status not in ("waiting_code",):
        return jsonify({"ok": False, "error": "no pending code"}), 400
    sess.error = ""
    sess.submit(code)
    return jsonify({"ok": True})


@app.route("/api/login/password", methods=["POST"])
def api_login_password():
    phone = (request.json or {}).get("phone", "")
    password = (request.json or {}).get("password", "")
    sess = LOGINS.get(phone)
    if not sess or sess.status != "waiting_password":
        return jsonify({"ok": False, "error": "no pending password"}), 400
    sess.error = ""
    sess.submit(password)
    return jsonify({"ok": True})


@app.route("/api/create", methods=["POST"])
def api_create():
    if CREATE_TASK["thread"] and CREATE_TASK["thread"].is_alive():
        return jsonify({"ok": False, "error": "a run is already active"}), 400
    cfg = read_config()
    if not cfg["phone_numbers"]:
        return jsonify({"ok": False, "error": "add phone numbers first"}), 400
    count = (request.json or {}).get("count")
    count = int(count) if count else 0
    t = threading.Thread(target=run_create, args=(cfg, count), daemon=True)
    CREATE_TASK["thread"] = t
    t.start()
    return jsonify({"ok": True})


@app.route("/api/cancel", methods=["POST"])
def api_cancel():
    CANCEL["set"] = True
    return jsonify({"ok": True})


@app.route("/api/logs")
def api_logs():
    with LOG_LOCK:
        return jsonify({"logs": list(LOG_RING)})


@app.route("/api/tokens")
def api_tokens():
    p = BASE_DIR / "created_bots.txt"
    if not p.exists():
        return jsonify({"tokens": ""})
    return jsonify({"tokens": p.read_text(encoding="utf-8")})


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bot factory</title>
<style>
  :root { --bg:#101418; --panel:#181e26; --line:#2a3340; --txt:#d7e0ea; --dim:#7d8a99; --acc:#4f8cff; --ok:#3ecf6e; --bad:#ff5c5c; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--txt); font:14px/1.5 system-ui, sans-serif; }
  header { padding:14px 22px; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; }
  h1 { font-size:16px; margin:0; } h1 span { color:var(--dim); font-weight:400; }
  main { max-width:980px; margin:0 auto; padding:20px; display:grid; grid-template-columns:1fr 1fr; gap:16px; }
  section { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:16px; }
  h2 { font-size:13px; text-transform:uppercase; letter-spacing:.08em; color:var(--dim); margin:0 0 12px; }
  label { display:block; margin:8px 0 3px; color:var(--dim); font-size:12px; }
  input, textarea { width:100%; background:#0d1117; border:1px solid var(--line); border-radius:6px; color:var(--txt); padding:7px 9px; font:inherit; }
  textarea { min-height:52px; resize:vertical; }
  button { background:var(--acc); border:0; color:#fff; border-radius:6px; padding:8px 14px; font:inherit; cursor:pointer; }
  button:hover { filter:brightness(1.1); }
  button.ghost { background:transparent; border:1px solid var(--line); color:var(--dim); }
  button.warn { background:var(--bad); }
  .row { display:flex; gap:8px; } .row > * { flex:1; }
  .phone { display:flex; justify-content:space-between; align-items:center; padding:7px 0; border-bottom:1px solid var(--line); }
  .phone:last-child { border-bottom:0; }
  .state { font-size:12px; color:var(--dim); }
  .state.ok { color:var(--ok); } .state.bad { color:var(--bad); }
  .login-box { display:none; gap:6px; margin-top:6px; }
  .login-box.show { display:flex; }
  #logs { background:#0d1117; border:1px solid var(--line); border-radius:6px; padding:10px; height:260px; overflow-y:auto; font-family:Consolas, monospace; font-size:12px; white-space:pre-wrap; word-break:break-all; }
  #tokens { background:#0d1117; border:1px solid var(--line); border-radius:6px; padding:10px; font-family:Consolas, monospace; font-size:12px; white-space:pre-wrap; max-height:180px; overflow:auto; }
  .full { grid-column:1 / -1; }
</style>
</head>
<body>
<header>
  <h1>Bot factory <span>| crypto bot creator</span></h1>
  <div id="busy" style="color:var(--dim);font-size:12px"></div>
</header>
<main>
  <section>
    <h2>Bot settings</h2>
    <label>Bot name (display name)</label><input id="bot_name">
    <label>Username base (e.g. PrimerBota &rarr; PrimerBota1bot, PrimerBotarobot, ...)</label><input id="username_base">
    <label>Main bot username (linked in description)</label><input id="main_bot_username">
    <label>Profile picture file (in this folder)</label><input id="profile_pic">
    <label>Description (shown on /start)</label><textarea id="description"></textarea>
    <label>About text</label><textarea id="about_text"></textarea>
    <div class="row" style="margin-top:10px"><button onclick="saveConfig()">Save settings</button></div>
  </section>

  <section>
    <h2>Accounts</h2>
    <div class="row">
      <input id="new_phone" placeholder="+15551234567">
      <button onclick="addPhone()">Add</button>
    </div>
    <div id="phones"></div>
  </section>

  <section class="full">
    <h2>Create bots</h2>
    <div class="row" style="max-width:420px">
      <input id="count" placeholder="count (empty = all accounts)" style="flex:2">
      <button onclick="startCreate()">Start</button>
      <button class="warn" onclick="cancelRun()">Cancel</button>
      <button class="ghost" onclick="loadTokens()">Tokens</button>
    </div>
    <div id="tokens" style="display:none"></div>
    <h2 style="margin-top:14px">Logs</h2>
    <div id="logs"></div>
  </section>
</main>
<script>
let busy = false;
async function j(url, opts) {
  const r = await fetch(url, opts);
  return r.json();
}
function el(id) { return document.getElementById(id); }
function esc(s) {
  const d = document.createElement("div"); d.textContent = s; return d.innerHTML;
}
function loadConfig() {
  j("/api/config").then(c => {
    el("bot_name").value = c.bot_name || "";
    el("username_base").value = c.username_base || "";
    el("main_bot_username").value = c.main_bot_username || "";
    el("profile_pic").value = c.profile_pic || "";
    el("description").value = c.description || "";
    el("about_text").value = c.about_text || "";
  });
}
function saveConfig() {
  j("/api/config", { method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({
      bot_name: el("bot_name").value,
      username_base: el("username_base").value,
      main_bot_username: el("main_bot_username").value,
      profile_pic: el("profile_pic").value,
      description: el("description").value,
      about_text: el("about_text").value
    })}).then(() => refresh());
}
function addPhone() {
  const v = el("new_phone").value.trim();
  if (!v) return;
  j("/api/phones", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({phone: v}) })
    .then(r => { if (!r.ok) alert(r.error || "failed"); el("new_phone").value = ""; refresh(); });
}
function removePhone(phone) {
  j("/api/phones/" + encodeURIComponent(phone), { method:"DELETE" }).then(refresh);
}
function login(phone) {
  j("/api/login", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({phone}) })
    .then(r => { if (!r.ok) alert(r.error || "failed"); refresh(); });
}
function submitLogin(phone) {
  const box = document.getElementById("login_" + phone.replace(/[^0-9]/g, ""));
  const input = box.querySelector("input");
  const btn = box.querySelector("button");
  const isPassword = btn.textContent.indexOf("password") !== -1;
  const path = isPassword ? "/api/login/password" : "/api/login/code";
  j(path, { method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({ phone, [isPassword ? "password" : "code"]: input.value }) })
    .then(() => { input.value = ""; refresh(); });
}
function startCreate() {
  const count = parseInt(el("count").value) || 0;
  j("/api/create", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({count}) })
    .then(r => { if (!r.ok) alert(r.error || "failed"); refresh(); });
}
function cancelRun() { j("/api/cancel", { method:"POST" }).then(refresh); }
function loadTokens() {
  j("/api/tokens").then(r => {
    const t = el("tokens");
    t.style.display = "block";
    t.textContent = r.tokens || "No bots created yet.";
  });
}
function renderPhones(phones, isBusy) {
  const wrap = el("phones");
  wrap.innerHTML = "";
  phones.forEach(p => {
    const div = document.createElement("div");
    div.className = "phone";
    const id = "login_" + p.phone.replace(/[^0-9]/g, "");
    let stateCls = "";
    let stateText = p.state;
    if (p.state.indexOf("logged") !== -1 || p.state === "done") { stateCls = "ok"; }
    if (p.state.indexOf("error") !== -1 || p.state.indexOf("FAILED") !== -1) { stateCls = "bad"; }
    let loginBtn = "";
    if (p.state.indexOf("waiting") === -1) {
      loginBtn = '<button class="ghost" onclick="login(\\'' + p.phone + '\\')">Login</button>';
    }
    const needsInput = p.state.indexOf("waiting") !== -1;
    const isPassword = p.state.indexOf("password") !== -1;
    const inputType = isPassword ? "password" : "text";
    const btnLabel = isPassword ? "Send password" : "Send code";
    div.innerHTML =
      '<div><div>' + esc(p.phone) + '</div>' +
      '<div class="state ' + stateCls + '">' + esc(stateText) + '</div>' +
      (needsInput
        ? '<div class="login-box show" id="' + id + '"><input type="' + inputType + '" placeholder="' + (isPassword ? "2FA password" : "code") + '"><button onclick="submitLogin(\\'' + p.phone + '\\')">' + btnLabel + '</button></div>'
        : '') +
      '</div><div>' + loginBtn + '<button class="ghost" style="margin-left:6px" onclick="removePhone(\\'' + p.phone + '\\')">Remove</button></div>';
    wrap.appendChild(div);
  });
  el("busy").textContent = isBusy ? "create run in progress..." : "";
  busy = isBusy;
}
function refresh() {
  j("/api/phones").then(r => renderPhones(r.phones, r.busy));
  if (!busy) {
    const logs = el("logs");
    j("/api/logs").then(r => {
      logs.innerHTML = r.logs.map(esc).join("<br>");
      logs.scrollTop = logs.scrollHeight;
    });
  }
}
setInterval(refresh, 1500);
loadConfig();
refresh();
</script>
</body>
</html>
"""


AUTH_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bot factory - login</title>
<style>
  body { margin:0; background:#101418; color:#d7e0ea; font:14px/1.5 system-ui, sans-serif;
         display:flex; align-items:center; justify-content:center; min-height:100vh; }
  .card { background:#181e26; border:1px solid #2a3340; border-radius:10px; padding:24px; width:300px; }
  h1 { font-size:15px; margin:0 0 14px; }
  input { width:100%; background:#0d1117; border:1px solid #2a3340; border-radius:6px; color:#d7e0ea; padding:8px 10px; margin-bottom:10px; }
  button { width:100%; background:#4f8cff; border:0; color:#fff; border-radius:6px; padding:9px; cursor:pointer; font:inherit; }
  .err { color:#ff5c5c; font-size:12px; min-height:16px; margin-top:8px; }
</style>
</head>
<body>
<div class="card">
  <h1>Bot factory</h1>
  <input type="password" id="pw" placeholder="password">
  <button onclick="submitPw()">Enter</button>
  <div class="err" id="err"></div>
</div>
<script>
async function submitPw() {
  const r = await fetch("/auth/password", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password: document.getElementById("pw").value })
  });
  if (r.ok) { location.reload(); } else { document.getElementById("err").textContent = "wrong password"; }
}
(async () => {
  let initData = "";
  try { initData = window.Telegram.WebApp.initData; } catch (e) {}
  if (!initData) {
    const q = new URLSearchParams(location.search);
    initData = q.get("initData") || "";
  }
  if (initData) {
    const r = await fetch("/auth/initdata", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ initData })
    });
    if (r.ok) { location.reload(); }
  }
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    log_line(f"webapp starting on port {PORT}")
    host = "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1"
    app.run(host=host, port=PORT, threaded=True)
