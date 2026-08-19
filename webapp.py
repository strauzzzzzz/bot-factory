import asyncio
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request
from telethon import TelegramClient
from telethon.errors import (
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)

from factory import (
    BASE_DIR,
    DATA_DIR,
    SESSIONS_DIR,
    factory_run,
    format_start_text,
    load_config,
    load_settings,
    save_settings,
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
BOOST = {"thread": None, "running": False, "rounds": 0, "done": 0, "total": 0, "error": ""}
BOT_POLL = {"running": True}
BOT_USERS = {}
BOT_USERS_LOCK = threading.Lock()
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
        return load_config(strict=False)


def save_ui_settings(fields):
    with CONFIG_LOCK:
        st = load_settings()
        st.update(fields)
        save_settings(st)


def get_phones():
    return list(read_config().get("phone_numbers") or [])


def set_phones(phones):
    with CONFIG_LOCK:
        st = load_settings()
        st["phone_numbers"] = phones
        save_settings(st)


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
            except (PhoneCodeExpiredError):
                session.error = ""
                try:
                    sent = await client.send_code_request(phone)
                except PhoneNumberFloodError:
                    session.status = "error"
                    session.error = "too many attempts - wait a few minutes"
                    break
                except Exception as e:
                    session.status = "error"
                    session.error = str(e)
                    break
                session.status = "waiting_code"
                session.error = "code expired - new code sent"
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


def read_tokens():
    p = DATA_DIR / "created_bots.txt"
    if not p.exists():
        return []
    rows = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [x.strip() for x in line.split(" | ")]
        if len(parts) >= 3:
            username = parts[1].lstrip("@")
            token = parts[2].strip()
            rows.append((username, token))
    return rows


def bot_api(token, method, params=None, timeout=15):
    url = f"https://api.telegram.org/bot{token}/{method}"
    try:
        data = urllib.parse.urlencode(params or {}).encode()
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"ok": False, "error": str(e)}


def bot_info(token):
    me = bot_api(token, "getMe")
    if not me.get("ok"):
        return None
    bot = me["result"]
    return {
        "username": bot.get("username", ""),
        "name": bot.get("first_name", ""),
    }


def bot_respond_worker():
    """Keeps every created bot alive: polls getUpdates and replies to /start
    with the configured start message, while counting unique users per bot."""
    offsets = {}
    while BOT_POLL["running"]:
        try:
            cfg = read_config(strict=False)
            start_text = cfg.get("start_message") or ""
            tokens = read_tokens()
            for username, token in tokens:
                params = {"timeout": 1, "limit": 50, "allowed_updates": ["message"]}
                if token in offsets:
                    params["offset"] = offsets[token]
                upd = bot_api(token, "getUpdates", params, timeout=10)
                if not upd.get("ok"):
                    continue
                for u in upd.get("result", []):
                    offsets[token] = u["update_id"] + 1
                    msg = u.get("message") or {}
                    text = (msg.get("text") or "").strip()
                    cid = msg.get("chat", {}).get("id")
                    if text == "/start" and cid is not None:
                        if cid > 0:
                            with BOT_USERS_LOCK:
                                BOT_USERS.setdefault(username, set()).add(cid)
                        if start_text:
                            bot_api(token, "sendMessage", {
                                "chat_id": cid,
                                "text": format_start_text(start_text),
                                "parse_mode": "HTML",
                            })
        except Exception:
            pass
        time.sleep(2)


def run_boost(cfg, count):
    BOOST["running"] = True
    BOOST["rounds"] = count
    BOOST["done"] = 0
    BOOST["error"] = ""
    tokens = read_tokens()
    phones = cfg["phone_numbers"]
    accounts = phones[:count] if (count and 0 < count < len(phones)) else phones
    BOOST["total"] = len(tokens) * len(accounts)
    log_line(f"=== boost start: {len(accounts)} account(s) x {len(tokens)} bot(s) ===")

    async def worker():
        for username, _token in tokens:
            if CANCEL["set"]:
                break
            for phone in accounts:
                if CANCEL["set"]:
                    break
                client = TelegramClient(
                    str(session_path(phone)),
                    cfg["api_id"],
                    cfg["api_hash"],
                    device_model="BotFactory",
                )
                try:
                    await client.connect()
                    if not await client.is_user_authorized():
                        log_line(f"boost: account {phone} NOT logged in - skipped")
                        continue
                    try:
                        await client.send_message(f"@{username}", "/start")
                        try:
                            from telethon.tl.functions.messages import EditPeerFolderRequest
                            await client(EditPeerFolderRequest(peer=f"@{username}", folder_id=1))
                        except Exception:
                            pass
                        BOOST["done"] += 1
                    except Exception as e:
                        log_line(f"boost: {phone} -> @{username} failed - {e}")
                        await asyncio.sleep(3)
                except Exception as e:
                    BOOST["error"] = str(e)
                finally:
                    await client.disconnect()
                await asyncio.sleep(2)
        log_line(f"=== boost finished: {BOOST['done']} /start sent ===")

    try:
        asyncio.run(worker())
    except Exception as e:
        BOOST["error"] = str(e)
    BOOST["running"] = False


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
        resp = jsonify({"ok": True})
        resp.set_cookie("botfactory_auth", issue_token(), httponly=True, samesite="Lax", max_age=AUTH_TTL)
        return resp
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
    resp = jsonify({"ok": True})
    resp.set_cookie("botfactory_auth", issue_token(), httponly=True, samesite="Lax", max_age=AUTH_TTL)
    return resp


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
            "start_message": cfg.get("start_message", ""),
        }
    )


@app.route("/api/config", methods=["POST"])
def api_config_set():
    fields = {}
    for key in (
        "bot_name",
        "username_base",
        "main_bot_username",
        "profile_pic",
        "description",
        "about_text",
        "start_message",
    ):
        if key in request.json:
            fields[key] = str(request.json[key]).strip()
    if fields:
        save_ui_settings(fields)
        log_line("settings saved")
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
    phones = get_phones()
    if phone not in phones:
        phones.append(phone)
        set_phones(phones)
        log_line(f"added {phone}")
    return jsonify({"ok": True})


@app.route("/api/phones/<path:phone>", methods=["DELETE"])
def api_phones_del(phone):
    phones = get_phones()
    if phone in phones:
        phones.remove(phone)
        set_phones(phones)
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
    p = DATA_DIR / "created_bots.txt"
    if not p.exists():
        return jsonify({"tokens": ""})
    return jsonify({"tokens": p.read_text(encoding="utf-8")})


@app.route("/api/stats")
def api_stats():
    rows = read_tokens()
    stats = []
    for username, token in rows:
        info = bot_info(token)
        with BOT_USERS_LOCK:
            users = len(BOT_USERS.get(username, set()))
        if info:
            stats.append({"username": info["username"], "name": info["name"], "users": users})
        else:
            stats.append({"username": username, "name": "", "users": users, "error": "invalid token"})
    return jsonify({"bots": stats, "boost": {
        "running": BOOST["running"],
        "rounds": BOOST["rounds"],
        "done": BOOST["done"],
        "total": BOOST["total"],
        "error": BOOST["error"],
    }})


@app.route("/api/boost", methods=["POST"])
def api_boost():
    if BOOST["running"]:
        return jsonify({"ok": False, "error": "boost already running"}), 400
    cfg = read_config()
    if not cfg["phone_numbers"]:
        return jsonify({"ok": False, "error": "add phone numbers first"}), 400
    if not read_tokens():
        return jsonify({"ok": False, "error": "no bots created yet"}), 400
    rounds = (request.json or {}).get("rounds")
    rounds = int(rounds) if rounds else 0
    CANCEL["set"] = False
    t = threading.Thread(target=run_boost, args=(cfg, rounds), daemon=True)
    BOOST["thread"] = t
    t.start()
    return jsonify({"ok": True})


PAGE = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bot Factory — панель управления</title>
<style>
  :root {
    --abyss:#012624; --deep:#011d1c; --kelp:#003734; --mist:#edfffe;
    --platinum:#ffffff; --silver:#bbc7c6; --slate:#707777; --pink:#fde9ff;
    --aurora:linear-gradient(90deg, #cbfffc 0%, #edfffe 26%, #fffdfa 47%, #fad1ff 89%);
    --ok:#2fbfa4; --bad:#ff6f6f; --warn:#ffd28a;
    --rcard:16px; --rinp:6px; --rbtn:6px;
    --font:'Matter','Inter',system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;
  }
  * { box-sizing:border-box; }
  html,body { margin:0; }
  body {
    background:var(--abyss); color:var(--silver); font-family:var(--font);
    font-size:16px; line-height:1.4; min-height:100vh;
    background-image:radial-gradient(1200px 500px at 50% -10%, rgba(0,130,124,.18), transparent 70%);
  }
  .wrap { max-width:1440px; margin:0 auto; padding:32px; }
  header {
    display:flex; justify-content:space-between; align-items:center;
    padding:20px 0; margin-bottom:28px;
  }
  .brand h1 {
    font-size:20px; font-weight:500; margin:0; letter-spacing:.08em; text-transform:uppercase;
    color:var(--platinum);
  }
  .brand p { margin:6px 0 0; color:var(--mist); font-size:13px; text-transform:uppercase; letter-spacing:.12em; }
  #busy {
    font-size:12px; text-transform:uppercase; letter-spacing:.1em; color:var(--pink);
    padding:8px 16px; border-radius:var(--rbtn); background:var(--kelp);
  }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:24px; }
  @media (max-width:880px){ .grid{grid-template-columns:1fr;} }
  .card {
    background:var(--kelp); border-radius:var(--rcard); padding:32px;
  }
  .eyebrow {
    font-size:12px; font-weight:500; text-transform:uppercase; letter-spacing:.12em;
    color:var(--silver); margin:0 0 4px;
  }
  h2 { margin:0 0 20px; font-size:24px; font-weight:500; color:var(--platinum); letter-spacing:-.02em; }
  label { display:block; margin:14px 0 6px; color:var(--mist); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }
  input, textarea {
    width:100%; background:var(--deep); color:var(--platinum);
    border:1px solid rgba(255,255,255,.08); border-radius:var(--rinp);
    padding:12px; font:inherit; outline:none; transition:border-color .15s;
  }
  input:focus, textarea:focus { border-color:var(--ok); }
  input::placeholder, textarea::placeholder { color:var(--slate); }
  textarea { min-height:80px; resize:vertical; }
  .row { display:flex; gap:10px; margin-top:10px; } .row > * { flex:1; }
  button {
    border:0; cursor:pointer; font:inherit; font-weight:500; border-radius:var(--rbtn);
    padding:12px 20px; font-size:13px; text-transform:uppercase; letter-spacing:.06em;
    transition:filter .15s, background .15s;
  }
  button:hover { filter:brightness(1.1); }
  .btn-primary { background:var(--aurora); color:#05201e; }
  .btn-ghost { background:transparent; color:var(--mist); border:1px solid rgba(255,255,255,.14); }
  .btn-ghost:hover { background:var(--kelp); }
  .btn-warn { background:var(--deep); color:var(--bad); border:1px solid rgba(255,111,111,.3); }
  .phone {
    display:flex; justify-content:space-between; align-items:center;
    padding:14px 0; border-bottom:1px solid rgba(255,255,255,.06);
  }
  .phone:last-child { border-bottom:0; }
  .phone .num { font-weight:500; color:var(--platinum); letter-spacing:-.01em; }
  .state { font-size:12px; text-transform:uppercase; letter-spacing:.06em; color:var(--silver); }
  .state.ok { color:var(--ok); } .state.bad { color:var(--bad); } .state.waiting { color:var(--warn); }
  .login-box { display:none; gap:8px; margin-top:10px; }
  .login-box.show { display:flex; }
  .login-box input { flex:1; }
  .tokens {
    background:var(--deep); border-radius:var(--rinp);
    padding:14px; font-family:'JetBrains Mono', Consolas, monospace; font-size:12px;
    white-space:pre-wrap; word-break:break-all; max-height:240px; overflow:auto; margin-top:16px; color:var(--mist);
  }
  #logs {
    background:var(--deep); border-radius:var(--rinp);
    padding:14px; height:300px; overflow-y:auto;
    font-family:'JetBrains Mono', Consolas, monospace; font-size:12px;
    white-space:pre-wrap; word-break:break-all; color:var(--mist);
  }
  .full { grid-column:1 / -1; }
  .accent-bar { height:2px; width:48px; background:var(--aurora); margin:0 0 20px; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="brand">
      <h1>Bot Factory</h1>
      <p>Панель управления · создание ботов через BotFather</p>
    </div>
    <div id="busy" style="display:none"></div>
  </header>

  <main class="grid">
    <section class="card">
      <div class="eyebrow">Настройки</div>
      <h2>Бот</h2>
      <div class="accent-bar"></div>
      <label>Имя бота (отображаемое)</label><input id="bot_name">
      <label>База юзернейма (напр. PrimerBota → PrimerBota1bot, PrimerBota2bot…)</label><input id="username_base">
      <label>Юзернейм главного бота (ссылка в описании)</label><input id="main_bot_username">
      <label>Файл аватарки (в этой папке)</label><input id="profile_pic">
      <label>Описание (в профиле бота)</label><textarea id="description"></textarea>
      <label>About — текст о боте</label><textarea id="about_text"></textarea>
      <label>Стартовое сообщение (ответ бота на /start)</label><textarea id="start_message"></textarea>
      <div class="row"><button class="btn-primary" onclick="saveConfig()">Сохранить</button></div>
    </section>

    <section class="card">
      <div class="eyebrow">Аккаунты</div>
      <h2>Telegram</h2>
      <div class="accent-bar"></div>
      <label>Добавить номер</label>
      <div class="row">
        <input id="new_phone" placeholder="+15551234567">
        <button class="btn-primary" onclick="addPhone()" style="flex:0 0 auto">Добавить</button>
      </div>
      <div id="phones" style="margin-top:8px"></div>
    </section>

    <section class="card full">
      <div class="eyebrow">Запуск</div>
      <h2>Создание ботов</h2>
      <div class="accent-bar"></div>
      <div class="row" style="max-width:560px">
        <input id="count" placeholder="Сколько ботов (пусто = все аккаунты)" style="flex:2">
        <button class="btn-primary" onclick="startCreate()">Запустить</button>
        <button class="btn-warn" onclick="cancelRun()">Стоп</button>
        <button class="btn-ghost" onclick="loadTokens()">Токены</button>
      </div>
      <div id="tokens" class="tokens" style="display:none"></div>
    </section>

    <section class="card full">
      <div class="eyebrow">Накрутка</div>
      <h2>Прогрев активности</h2>
      <div class="accent-bar"></div>
      <div class="row" style="max-width:560px">
        <input id="boost_rounds" type="number" min="1" placeholder="все" style="flex:0 0 120px">
        <button class="btn-primary" onclick="startBoost()">Запустить</button>
        <div id="boost_status" style="display:flex;align-items:center;color:var(--pink);font-size:13px;text-transform:uppercase;letter-spacing:.08em"></div>
      </div>
      <p style="color:var(--silver);font-size:13px;margin:10px 0 0">Каждый аккаунт шлёт боту 1 /start (1–2 на бота, но от разных аккаунтов). Пусто = все аккаунты. Аккаунт = отдельный юзер.</p>
    </section>

    <section class="card full">
      <div class="eyebrow">Статистика</div>
      <h2>Боты</h2>
      <div class="accent-bar"></div>
      <div class="row" style="max-width:300px">
        <button class="btn-ghost" onclick="loadStats()">Обновить</button>
      </div>
      <div id="stats" style="margin-top:14px"></div>
    </section>

    <section class="card full">
      <div class="eyebrow">Мониторинг</div>
      <h2>Логи</h2>
      <div class="accent-bar"></div>
      <div id="logs"></div>
    </section>
  </main>
</div>
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
    el("start_message").value = c.start_message || "";
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
      about_text: el("about_text").value,
      start_message: el("start_message").value
    })}).then(() => refresh());
}
function addPhone() {
  const v = el("new_phone").value.trim();
  if (!v) return;
  j("/api/phones", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({phone: v}) })
    .then(r => { if (!r.ok) alert(r.error || "Не удалось добавить"); el("new_phone").value = ""; refresh(); });
}
function removePhone(phone) {
  j("/api/phones/" + encodeURIComponent(phone), { method:"DELETE" }).then(refresh);
}
function login(phone) {
  j("/api/login", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({phone}) })
    .then(r => { if (!r.ok) alert(r.error || "Не удалось начать вход"); refresh(); });
}
function submitLogin(phone) {
  const box = document.getElementById("login_" + phone.replace(/[^0-9]/g, ""));
  const input = box.querySelector("input");
  const isPassword = input.type === "password";
  const path = isPassword ? "/api/login/password" : "/api/login/code";
  j(path, { method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({ phone, [isPassword ? "password" : "code"]: input.value }) })
    .then(() => { input.value = ""; refresh(); });
}
function startCreate() {
  const count = parseInt(el("count").value) || 0;
  j("/api/create", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({count}) })
    .then(r => { if (!r.ok) alert(r.error || "Не удалось запустить"); refresh(); });
}
function cancelRun() { j("/api/cancel", { method:"POST" }).then(refresh); }
function startBoost() {
  const rounds = parseInt(el("boost_rounds").value) || 1;
  j("/api/boost", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({rounds}) })
    .then(r => { if (!r.ok) alert(r.error || "Не удалось запустить"); });
}
function renderStats(data) {
  const wrap = el("stats");
  const b = data.boost || {};
  const bs = el("boost_status");
  if (b.running) {
    bs.textContent = "Идёт: " + b.done + "/" + (b.total || 0);
  } else {
    bs.textContent = b.done ? ("Готово: " + b.done + " /start") : (b.error ? ("Ошибка: " + b.error) : "");
  }
  const bots = data.bots || [];
  if (!bots.length) { wrap.innerHTML = '<div style="color:var(--silver)">Ботов пока нет.</div>'; return; }
  let h = '<table style="width:100%;border-collapse:collapse;font-size:14px">' +
    '<thead><tr style="color:var(--slate);text-align:left;text-transform:uppercase;font-size:11px;letter-spacing:.08em">' +
    '<th style="padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.08)">Бот</th>' +
    '<th style="padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.08)">Название</th>' +
    '<th style="padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.08)">Юзеров</th></tr></thead><tbody>';
  bots.forEach(bot => {
    h += '<tr><td style="padding:10px;border-bottom:1px solid rgba(255,255,255,.06);color:var(--platinum)">@' + esc(bot.username || "") + '</td>' +
      '<td style="padding:10px;border-bottom:1px solid rgba(255,255,255,.06);color:var(--silver)">' + esc(bot.name || "") + '</td>' +
      '<td style="padding:10px;border-bottom:1px solid rgba(255,255,255,.06);color:var(--pink);font-weight:500">' + (bot.users || 0) + '</td></tr>';
  });
  h += '</tbody></table>';
  wrap.innerHTML = h;
}
function loadStats() {
  j("/api/stats").then(renderStats);
}
function loadTokens() {
  j("/api/tokens").then(r => {
    const t = el("tokens");
    t.style.display = "block";
    t.textContent = r.tokens || "Ботов пока не создано.";
  });
}
function ruState(s) {
  if (s.indexOf("waiting_password") !== -1) return { t: "Ожидание 2FA-пароля", c: "waiting" };
  if (s.indexOf("waiting_code") !== -1) return { t: "Ожидание кода", c: "waiting" };
  if (s.indexOf("logging in") !== -1) return { t: "Вход…", c: "waiting" };
  if (s.indexOf("error") !== -1 || s.indexOf("FAILED") !== -1) return { t: "Ошибка", c: "bad" };
  if (s.indexOf("logged in") !== -1 || s === "done") return { t: "Вошли", c: "ok" };
  if (s.indexOf("not logged in") !== -1) return { t: "Не вошли", c: "bad" };
  return { t: s, c: "" };
}
function renderPhones(phones, isBusy) {
  const wrap = el("phones");
  const saved = {};
  const active = document.activeElement;
  wrap.querySelectorAll(".login-box input").forEach(inp => {
    const box = inp.closest(".login-box");
    if (box) saved[box.id] = inp.value;
  });
  wrap.innerHTML = "";
  phones.forEach(p => {
    const div = document.createElement("div");
    div.className = "phone";
    const id = "login_" + p.phone.replace(/[^0-9]/g, "");
    const st = ruState(p.state);
    let loginBtn = "";
    if (p.state.indexOf("waiting") === -1) {
      loginBtn = '<button class="btn-ghost" onclick="login(\\'' + p.phone + '\\')">Войти</button>';
    }
    const needsInput = p.state.indexOf("waiting") !== -1;
    const isPassword = p.state.indexOf("password") !== -1;
    const inputType = isPassword ? "password" : "text";
    const btnLabel = isPassword ? "Отправить пароль" : "Отправить код";
    div.innerHTML =
      '<div><div class="num">' + esc(p.phone) + '</div>' +
      '<div class="state ' + st.c + '">' + esc(st.t) + '</div>' +
      (needsInput
        ? '<div class="login-box show" id="' + id + '"><input type="' + inputType + '" placeholder="' + (isPassword ? "2FA-пароль" : "Код") + '"><button class="btn-primary" onclick="submitLogin(\\'' + p.phone + '\\')">' + btnLabel + '</button></div>'
        : '') +
      '</div><div>' + loginBtn + '<button class="btn-ghost" style="margin-left:6px" onclick="removePhone(\\'' + p.phone + '\\')">Удалить</button></div>';
    wrap.appendChild(div);
  });
  let restoreFocus = null;
  Object.keys(saved).forEach(id => {
    const box = document.getElementById(id);
    if (!box) return;
    const inp = box.querySelector("input");
    if (inp) inp.value = saved[id];
    if (active && active.closest && box.contains(active)) restoreFocus = inp;
  });
  if (restoreFocus) { try { restoreFocus.focus(); } catch (e) {} }
  const busyEl = el("busy");
  busyEl.textContent = isBusy ? "Идёт создание ботов…" : "";
  busyEl.style.display = isBusy ? "" : "none";
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
setInterval(loadStats, 5000);
loadConfig();
refresh();
loadStats();
</script>
</body>
</html>
"""


AUTH_PAGE = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bot Factory — вход</title>
<style>
  :root {
    --canvas:#012624; --deep:#011d1c; --kelp:#003734; --mist:#edfffe; --white:#ffffff; --silver:#bbc7c6; --slate:#707777; --pink:#fde9ff;
    --aurora:linear-gradient(90deg, #cbfffc 0%, #edfffe 26%, #fffdfa 47%, #fad1ff 89%);
    --edge:rgba(255,255,255,.08); --ok:#2fbfa4; --rinp:6px; --rcard:16px;
    --font:'Matter','Inter',system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;
  }
  * { box-sizing:border-box; }
  body {
    margin:0; background:var(--canvas); color:var(--silver); font-family:var(--font); font-size:14px;
    display:flex; align-items:center; justify-content:center; min-height:100vh;
    background-image:radial-gradient(900px 400px at 50% -10%, rgba(0,130,124,.18), transparent 70%);
  }
  .card {
    background:var(--kelp); border-radius:var(--rcard);
    padding:32px; width:320px;
  }
  h1 {
    font-size:24px; font-weight:500; margin:0 0 4px;
    letter-spacing:.08em; text-transform:uppercase; color:var(--white);
  }
  .sub { color:var(--mist); font-size:12px; margin:0 0 20px; text-transform:uppercase; letter-spacing:.12em; }
  input {
    width:100%; background:var(--deep); color:var(--white);
    border:1px solid var(--edge); border-radius:var(--rinp);
    padding:12px; font:inherit; outline:none; margin-bottom:12px;
    transition:border-color .15s;
  }
  input:focus { border-color:var(--ok); }
  button {
    width:100%; background:var(--aurora); color:#05201e; border:0; border-radius:var(--rinp);
    padding:12px; cursor:pointer; font:inherit; font-weight:500; text-transform:uppercase; letter-spacing:.06em;
    transition:filter .15s;
  }
  button:hover { filter:brightness(1.1); }
  .err { color:#ff6f6f; font-size:12px; min-height:16px; margin-top:10px; }
</style>
</head>
<body>
<div class="card">
  <h1>Bot Factory</h1>
  <p class="sub">Введите пароль для входа в панель</p>
  <input type="password" id="pw" placeholder="Пароль">
  <button onclick="submitPw()">Войти</button>
  <div class="err" id="err"></div>
</div>
<script>
async function submitPw() {
  const r = await fetch("/auth/password", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password: document.getElementById("pw").value })
  });
  if (r.ok) { location.reload(); } else { document.getElementById("err").textContent = "Неверный пароль"; }
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
    threading.Thread(target=bot_respond_worker, daemon=True).start()
    log_line(f"webapp starting on port {PORT}")
    host = "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1"
    app.run(host=host, port=PORT, threaded=True)
