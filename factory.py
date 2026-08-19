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
ALLOCATOR_FILE = DATA_DIR / "allocator.json"

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
    "start_message",
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
    cfg = {}
    if cfg_path.exists():
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
        "start_message": ("START_MESSAGE", str),
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
    if cfg.get("profile_pic") and Path(cfg["profile_pic"]).exists():
        pass
    elif (BASE_DIR / "profile.JPG").exists():
        cfg["profile_pic"] = "profile.JPG"
    if strict:
        if not cfg.get("api_id") or not cfg.get("api_hash"):
            raise SystemExit("config.json or env: set api_id and api_hash (from my.telegram.org)")
        if not isinstance(cfg.get("phone_numbers"), list) or not cfg["phone_numbers"]:
            raise SystemExit("config.json or env: phone_numbers must be a non-empty list, e.g. ['+15551234567', ...]")
    return cfg


def normalize_base(base: str) -> str:
    base = (base or "").lower().strip()
    if base.endswith("robot"):
        base = base[:-5]
    elif base.endswith("bot"):
        base = base[:-3]
    return base


def sanitize_phone(phone: str) -> str:
    return "".join(c for c in phone if c.isdigit())


def session_path(phone: str) -> Path:
    return SESSIONS_DIR / sanitize_phone(phone)


class NameAllocator:
    """Hands out the next free username for a base, remembering what was already used.

    Shared across accounts so the next account continues where the previous one
    stopped instead of starting over at base1bot.
    """

    def __init__(self, base, next_index=1, used=None):
        self.base = normalize_base(base)
        self.index = int(next_index)
        self.used = set(used or ())

    def next_candidate(self):
        while True:
            for suffix in ("bot", "robot"):
                cand = f"{self.base}{self.index}{suffix}"
                if cand not in self.used:
                    self.index += 1
                    return cand
            self.index += 1

    def mark_used(self, name):
        self.used.add(name)

    def to_dict(self):
        return {"base": self.base, "index": self.index, "used": sorted(self.used)}


def load_allocators():
    try:
        data = json.loads(ALLOCATOR_FILE.read_text(encoding="utf-8"))
        return {
            base: NameAllocator(base, a.get("index", 1), a.get("used", []))
            for base, a in data.items()
        }
    except Exception:
        return {}


def save_allocators(allocators):
    try:
        ALLOCATOR_FILE.write_text(
            json.dumps({b: a.to_dict() for b, a in allocators.items()}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        with open(DATA_DIR / "botfather.log", "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def save_tokens(rows):
    try:
        with open(DATA_DIR / "created_bots.txt", "a", encoding="utf-8") as f:
            for username, token in rows:
                f.write(f"{datetime.now().isoformat()} | @{username} | {token}\n")
    except Exception:
        pass


async def wait_reply(client, after_id, timeout=REPLY_TIMEOUT):
    """Wait for BotFather's next incoming message after the given message id.

    Uses `not m.out` so our own sent messages are never mistaken for the reply.
    Returns the Telethon Message object (None on timeout).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msgs = await client.get_messages(BOTFATHER, limit=6)
        for m in msgs:
            if m.id > after_id and not m.out:
                return m, m.id
        await asyncio.sleep(1.5)
    return None, after_id


async def cmd(client, text):
    sent = await client.send_message(BOTFATHER, text)
    return await wait_reply(client, sent.id)


async def safe_cmd(client, text, what):
    reply, _ = await cmd(client, text)
    if reply is None:
        log(f"no reply for '{what}' - sending /cancel and retrying once")
        await client.send_message(BOTFATHER, "/cancel")
        await asyncio.sleep(3)
        reply, _ = await cmd(client, text)
        if reply is None:
            raise RuntimeError(f"BotFather not responding for '{what}' (timeout)")
    return reply


async def select_bot(reply_msg, username):
    """Click the bot-selection button for the given username on BotFather's
    inline keyboard. Falls back to the first button (the newest bot) when the
    exact username isn't found. Returns True if a button was clicked.
    """
    buttons = getattr(reply_msg, "buttons", None) or []
    if not buttons:
        return False
    target = username.lstrip("@").lower()
    for row in buttons:
        for btn in row:
            if target in btn.text.lower().replace("@", ""):
                await btn.click()
                return True
    await buttons[0][0].click()
    return True


async def create_bot_for_account(client, cfg, allocator, progress=None):
    if progress is None:
        progress = lambda msg: log(msg)

    reply = await safe_cmd(client, "/newbot", "/newbot")
    progress(f"BotFather: {reply.message[:120]}")
    reply = await safe_cmd(client, cfg["bot_name"], "bot name")
    progress(f"BotFather: {reply.message[:120]}")

    token, used_username = None, None
    for _ in range(300):
        username = allocator.next_candidate()
        reply = await safe_cmd(client, username, f"username @{username}")
        low = reply.message.lower()
        m = TOKEN_RE.search(reply.message)
        if m or "done!" in low or "congratulations" in low:
            used_username = username
            allocator.mark_used(username)
            token = m.group(0) if m else "UNKNOWN"
            progress(f"bot created: @{used_username} | token={token}")
            break
        allocator.mark_used(username)
        progress(f"@{username} taken/unavailable, trying next...")
        continue
    if token is None:
        raise RuntimeError(
            f"no free username found for base '{allocator.base}' after 300 tries"
        )

    pic = BASE_DIR / cfg["profile_pic"]
    if pic.exists():
        reply = await safe_cmd(client, "/setuserpic", "/setuserpic")
        progress(f"BotFather: {reply.message[:120]}")
        await select_bot(reply, used_username)
        reply2, _ = await wait_reply(client, reply.id)
        progress(f"BotFather: {reply2.message[:120] if reply2 else 'no reply'}")
        sent_pic = await client.send_file(BOTFATHER, str(pic), force_document=False)
        reply3, _ = await wait_reply(client, sent_pic.id)
        progress(f"BotFather (userpic): {reply3.message[:120] if reply3 else 'no reply'}")
    else:
        progress(f"warning: profile picture not found at {pic} - skipping /setuserpic")

    reply = await safe_cmd(client, "/setdescription", "/setdescription")
    progress(f"BotFather: {reply.message[:120]}")
    await select_bot(reply, used_username)
    reply2, _ = await wait_reply(client, reply.id)
    progress(f"BotFather: {reply2.message[:120] if reply2 else 'no reply'}")
    reply = await safe_cmd(client, cfg["description"], "description")
    progress(f"BotFather: {reply.message[:120]}")

    reply = await safe_cmd(client, "/setabouttext", "/setabouttext")
    progress(f"BotFather: {reply.message[:120]}")
    await select_bot(reply, used_username)
    reply2, _ = await wait_reply(client, reply.id)
    progress(f"BotFather: {reply2.message[:120] if reply2 else 'no reply'}")
    reply = await safe_cmd(client, cfg["about_text"], "about text")
    progress(f"BotFather: {reply.message[:120]}")

    return used_username, token


async def factory_run(cfg, phones, bases, progress=None, should_cancel=None):
    if progress is None:
        progress = lambda msg: log(msg)
    if should_cancel is None:
        should_cancel = lambda: False

    allocators = load_allocators()
    for base in bases:
        key = normalize_base(base)
        if key not in allocators:
            allocators[key] = NameAllocator(base)

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
            allocator = allocators[normalize_base(bases[i])]
            username, token = await create_bot_for_account(client, cfg, allocator, progress=progress)
            rows.append((username, token))
            save_tokens([(username, token)])
            save_allocators(allocators)
            progress(f"account {phone}: done - @{username}")
            if i < len(phones) - 1 and not should_cancel():
                await asyncio.sleep(ACCOUNT_DELAY)
        except Exception as e:
            progress(f"account {phone}: FAILED - {e}")
        finally:
            await client.disconnect()

    save_allocators(allocators)
    return rows
