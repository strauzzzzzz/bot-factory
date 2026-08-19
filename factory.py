import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR))
SESSIONS_DIR = DATA_DIR / "sessions"
SESSIONS_DIR.mkdir(exist_ok=True)

BOTFATHER = "BotFather"
TOKEN_RE = re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b")
CMD_DELAY = 2.0
ACCOUNT_DELAY = 15.0
REPLY_TIMEOUT = 90

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


SETTINGS_KEYS = (
    "phone_numbers",
    "admin_ids",
    "bot_name",
    "username_base",
    "main_bot_username",
    "profile_pic",
    "description",
    "about_text",
)


def load_settings():
    p = DATA_DIR / "settings.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_settings(settings):
    (DATA_DIR / "settings.json").write_text(
        json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def load_config(strict=True):
    cfg_path = BASE_DIR / "config.json"
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    env_map = {
        "api_id": ("API_ID", lambda v: int(v)),
        "api_hash": ("API_HASH", str),
        "control_bot_token": ("CONTROL_BOT_TOKEN", str),
        "bot_name": ("BOT_NAME", str),
        "username_base": ("USERNAME_BASE", str),
        "main_bot_username": ("MAIN_BOT_USERNAME", str),
        "profile_pic": ("PROFILE_PIC", str),
        "description": ("DESCRIPTION", str),
        "about_text": ("ABOUT_TEXT", str),
    }
    for key, (env, cast) in env_map.items():
        if os.getenv(env):
            cfg[key] = cast(os.getenv(env))
    if os.getenv("ADMIN_IDS"):
        cfg["admin_ids"] = [int(x.strip()) for x in os.getenv("ADMIN_IDS").split(",") if x.strip()]
    if os.getenv("PHONE_NUMBERS"):
        cfg["phone_numbers"] = [p.strip() for p in os.getenv("PHONE_NUMBERS").split(",") if p.strip()]
    st = load_settings()
    for key in SETTINGS_KEYS:
        if key in st:
            cfg[key] = st[key]
    if strict:
        if not cfg.get("api_id") or not cfg.get("api_hash"):
            raise SystemExit("config.json: set api_id and api_hash (from my.telegram.org)")
        if not isinstance(cfg.get("phone_numbers"), list) or not cfg["phone_numbers"]:
            raise SystemExit("config.json: phone_numbers must be a non-empty list, e.g. ['+15551234567', ...]")
    return cfg


def normalize_base(base: str) -> str:
    base = base.lower().strip()
    if base.endswith("robot"):
        base = base[:-5]
    elif base.endswith("bot"):
        base = base[:-3]
    return base


def build_candidates(base: str, limit: int = 200):
    base = normalize_base(base)
    candidates = [f"{base}1bot", f"{base}robot"]
    candidates += [f"{base}{n}bot" for n in range(2, limit + 1)]
    return candidates


def sanitize_phone(phone: str) -> str:
    return "".join(c for c in phone if c.isdigit())


def session_path(phone: str) -> Path:
    return SESSIONS_DIR / sanitize_phone(phone)


def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        with open(DATA_DIR / "botfather.log", "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def save_tokens(rows):
    with open(DATA_DIR / "created_bots.txt", "a", encoding="utf-8") as f:
        for username, token in rows:
            f.write(f"{datetime.now().isoformat()} | @{username} | {token}\n")


async def wait_reply(client, last_id, timeout=REPLY_TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msgs = await client.get_messages(BOTFATHER, limit=3)
        for m in msgs:
            if m.id > last_id:
                return (m.message or "").strip(), m.id
        await asyncio.sleep(1.5)
    return None, last_id


async def cmd(client, text, last_id):
    await client.send_message(BOTFATHER, text)
    return await wait_reply(client, last_id)


async def safe_cmd(client, text, last_id, what):
    reply, last_id = await cmd(client, text, last_id)
    if reply is None:
        log(f"no reply for '{what}' - sending /cancel and retrying once")
        await client.send_message(BOTFATHER, "/cancel")
        await asyncio.sleep(3)
        reply, last_id = await cmd(client, text, last_id)
        if reply is None:
            raise RuntimeError(f"BotFather not responding for '{what}' (timeout)")
    return reply, last_id


async def create_bot_for_account(client, cfg, candidates, progress=None):
    if progress is None:
        progress = lambda msg: log(msg)
    last_id = 0
    pic = BASE_DIR / cfg["profile_pic"]

    reply, last_id = await safe_cmd(client, "/newbot", last_id, "/newbot")
    progress(f"BotFather: {reply[:120]}")

    reply, last_id = await safe_cmd(client, cfg["bot_name"], last_id, "bot name")
    progress(f"BotFather: {reply[:120]}")

    token, used_username = None, None
    for i, username in enumerate(candidates):
        reply, last_id = await safe_cmd(client, username, last_id, f"username @{username}")
        low = reply.lower()
        if "already taken" in low or "taken" in low:
            progress(f"@{username} taken, trying next...")
            continue
        if "done!" in low or "congratulations" in low:
            used_username = username
            m = TOKEN_RE.search(reply)
            token = m.group(0) if m else "UNKNOWN"
            progress(f"bot created: @{used_username}")
            break
        raise RuntimeError(
            f"unexpected BotFather reply while choosing username @{username}: {reply[:200]}"
        )
    if token is None:
        raise RuntimeError(
            f"no free username found for base '{candidates[0]}' after {len(candidates)} tries"
        )

    if pic.exists():
        reply, last_id = await safe_cmd(client, "/setuserpic", last_id, "/setuserpic")
        progress(f"BotFather: {reply[:120]}")
        await client.send_file(BOTFATHER, str(pic), force_document=False)
        reply, last_id = await wait_reply(client, last_id)
        progress(f"BotFather (userpic): {reply[:120] if reply else 'no reply'}")
    else:
        progress(f"warning: profile picture not found at {pic} - skipping /setuserpic")

    reply, last_id = await safe_cmd(client, "/setdescription", last_id, "/setdescription")
    progress(f"BotFather: {reply[:120]}")
    reply, last_id = await safe_cmd(client, cfg["description"], last_id, "description")
    progress(f"BotFather: {reply[:120]}")

    reply, last_id = await safe_cmd(client, "/setabouttext", last_id, "/setabouttext")
    progress(f"BotFather: {reply[:120]}")
    reply, last_id = await safe_cmd(client, cfg["about_text"], last_id, "about text")
    progress(f"BotFather: {reply[:120]}")

    return used_username, token


async def factory_run(cfg, phones, bases, progress=None, should_cancel=None):
    if progress is None:
        progress = lambda msg: log(msg)
    if should_cancel is None:
        should_cancel = lambda: False

    rows = []
    for i, phone in enumerate(phones):
        if should_cancel():
            progress("cancel requested - stopping")
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
                progress(f"account {phone}: NOT logged in - skipping (use /login)")
                continue
            progress(f"account {phone}: creating bot...")
            username, token = await create_bot_for_account(
                client, cfg, build_candidates(bases[i]), progress=progress
            )
            rows.append((username, token))
            progress(f"account {phone}: done - @{username}")
            if i < len(phones) - 1 and not should_cancel():
                await asyncio.sleep(ACCOUNT_DELAY)
        except Exception as e:
            progress(f"account {phone}: FAILED - {e}")
        finally:
            await client.disconnect()

    if rows:
        save_tokens(rows)
    return rows
