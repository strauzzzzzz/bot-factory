import asyncio

from telethon import TelegramClient

from factory import factory_run, load_config, log, session_path


async def ensure_logged_in(cfg, phone):
    client = TelegramClient(
        str(session_path(phone)),
        cfg["api_id"],
        cfg["api_hash"],
        device_model="BotFactory",
    )
    try:
        await client.start(phone=phone)
        me = await client.get_me()
        log(f"{phone}: logged in as @{me.username or me.first_name}")
        return True
    except Exception as e:
        log(f"{phone}: login FAILED - {e}")
        return False
    finally:
        await client.disconnect()


async def main():
    cfg = load_config()
    phones = cfg["phone_numbers"]
    bases = cfg.get("username_bases") or [cfg["username_base"]] * len(phones)
    if len(bases) < len(phones):
        bases = bases + [cfg["username_base"]] * (len(phones) - len(bases))

    log("=== Bot factory start (console mode) ===")
    for phone in phones:
        await ensure_logged_in(cfg, phone)
    await factory_run(cfg, phones, bases)
    log("=== Finished ===")


if __name__ == "__main__":
    asyncio.run(main())
