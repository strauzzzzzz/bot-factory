import asyncio
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient, events
from telethon.errors import (
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)

from factory import (
    BASE_DIR,
    SESSIONS_DIR,
    build_candidates,
    factory_run,
    load_config,
    log,
    session_path,
)

pending_logins = {}
cancel_flag = {"set": False}
factory_task = {"task": None}


def is_admin(cfg, e):
    uid = e.sender_id
    admins = cfg.get("admin_ids") or []
    if not admins:
        return False
    return uid in admins


async def deny(e):
    await e.respond("Not authorized.")


def running_report(cfg, e):
    async def report(msg):
        try:
            await e.respond(f"`{msg}`")
        except Exception:
            log(f"report failed: {msg}")

    return report


async def handle_login_message(client, e, text):
    parts = text.strip().split()
    if not parts:
        await e.respond("Usage: /login +15551234567")
        return
    phone = parts[0]
    if not phone.startswith("+"):
        phone = "+" + phone
    pending_logins[e.chat_id] = {
        "phone": phone,
        "client": client,
        "step": "code",
    }
    try:
        sent = await client.send_code_request(phone)
        pending_logins[e.chat_id]["code_hash"] = sent.phone_code_hash
        await e.respond(f"Code sent to {phone}. Reply with the login code.")
    except Exception as ex:
        await e.respond(f"Could not request code: {ex}")
        pending_logins.pop(e.chat_id, None)


async def handle_code_entry(e, cfg):
    state = pending_logins.get(e.chat_id)
    if not state:
        return
    phone = state["phone"]
    client = state["client"]
    text = e.message.message.strip()
    try:
        if state["step"] == "code":
            await client.sign_in(phone, code=text, phone_code_hash=state["code_hash"])
            me = await client.get_me()
            await e.respond(
                f"Logged in {phone} as @{me.username or me.first_name}. "
                f"Session saved - /create will use this account."
            )
            pending_logins.pop(e.chat_id, None)
        elif state["step"] == "password":
            await client.sign_in(password=text)
            me = await client.get_me()
            await e.respond(f"Logged in {phone} as @{me.username or me.first_name}.")
            pending_logins.pop(e.chat_id, None)
    except PhoneCodeInvalidError:
        await e.respond("Invalid code. Reply with the correct code.")
    except SessionPasswordNeededError:
        state["step"] = "password"
        await e.respond("That account has 2FA. Reply with the password.")
    except Exception as ex:
        await e.respond(f"Login failed: {ex}")
        pending_logins.pop(e.chat_id, None)


async def handle_create(e, cfg, arg):
    phones = cfg["phone_numbers"]
    bases = cfg.get("username_bases") or [cfg["username_base"]] * len(phones)
    if len(bases) < len(phones):
        bases = bases + [cfg["username_base"]] * (len(phones) - len(bases))

    arg = arg.strip().lower()
    if arg and arg != "all":
        try:
            count = int(arg)
        except ValueError:
            await e.respond("Usage: /create [n|all]")
            return
        if count <= 0 or count > len(phones):
            await e.respond(f"n must be between 1 and {len(phones)}")
            return
        phones = phones[:count]
        bases = bases[:count]

    cancel_flag["set"] = False
    await e.respond(f"Starting creation on {len(phones)} account(s)...")

    async def job():
        rows = await factory_run(
            cfg,
            phones,
            bases,
            progress=running_report(cfg, e),
            should_cancel=lambda: cancel_flag["set"],
        )
        if rows:
            await e.respond(f"Done - {len(rows)} bot(s) created. /tokens to list them.")
        else:
            await e.respond("Nothing created. Check logs above (maybe accounts not logged in).")

    factory_task["task"] = asyncio.create_task(job())


async def main():
    cfg = load_config()
    token = cfg.get("control_bot_token")
    if not token:
        raise SystemExit("config.json: set control_bot_token (create the bot via @BotFather first)")
    if not cfg.get("admin_ids"):
        raise SystemExit("config.json: set admin_ids - your Telegram numeric user IDs, or nobody can use the bot")

    client = TelegramClient(
        str(SESSIONS_DIR / "control_bot"),
        cfg["api_id"],
        cfg["api_hash"],
        device_model="BotFactory",
    )
    await client.start(bot_token=token)
    me = await client.get_me()
    log(f"control bot online: @{me.username}")

    @client.on(events.NewMessage(pattern=r"^/start"))
    async def on_start(e):
        if not is_admin(cfg, e):
            return await deny(e)
        await e.respond(
            "Bot factory control\n"
            "/login <phone> - log in an account (code arrives by SMS/Telegram)\n"
            "/create [n|all] - create bots on n accounts (default: all)\n"
            "/status - accounts state\n"
            "/tokens - list created bots\n"
            "/cancel - stop the current run"
        )

    @client.on(events.NewMessage(pattern=r"^/login(?:\s+(.*))?$"))
    async def on_login(e):
        if not is_admin(cfg, e):
            return await deny(e)
        arg = (e.pattern_match.group(1) or "").strip()
        if arg:
            await handle_login_message(client, e, arg)
        else:
            await e.respond("Usage: /login +15551234567")

    @client.on(events.NewMessage(pattern=r"^/create(?:\s+(.*))?$"))
    async def on_create(e):
        if not is_admin(cfg, e):
            return await deny(e)
        if factory_task["task"] and not factory_task["task"].done():
            await e.respond("A run is already in progress - /cancel it first.")
            return
        await handle_create(e, cfg, (e.pattern_match.group(1) or "").strip())

    @client.on(events.NewMessage(pattern=r"^/cancel"))
    async def on_cancel(e):
        if not is_admin(cfg, e):
            return await deny(e)
        cancel_flag["set"] = True
        await e.respond("Cancel requested - stopping after the current account.")

    @client.on(events.NewMessage(pattern=r"^/status"))
    async def on_status(e):
        if not is_admin(cfg, e):
            return await deny(e)
        lines = []
        for phone in cfg["phone_numbers"]:
            s = session_path(phone)
            status = "session exists" if s.with_suffix(".session").exists() else "not logged in"
            lines.append(f"{phone}: {status}")
        await e.respond("\n".join(lines))

    @client.on(events.NewMessage(pattern=r"^/tokens"))
    async def on_tokens(e):
        if not is_admin(cfg, e):
            return await deny(e)
        p = BASE_DIR / "created_bots.txt"
        if not p.exists():
            await e.respond("No bots created yet.")
            return
        data = p.read_text(encoding="utf-8").strip()
        await e.respond(f"```\n{data}\n```" if data else "No bots created yet.")

    @client.on(events.NewMessage())
    async def on_any(e):
        if not is_admin(cfg, e):
            return
        if e.chat_id in pending_logins:
            await handle_code_entry(e, cfg)

    log("waiting for commands...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
