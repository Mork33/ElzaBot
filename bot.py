import sys
import glob
import importlib
from pathlib import Path
from pyrogram import Client, idle, __version__
from pyrogram.raw.all import layer
import logging
import logging.config
import time
import asyncio
from datetime import date, datetime
import pytz
from aiohttp import web
from database.ia_filterdb import Media, Media2
from database.users_chats_db import db
from database import initialize_cache_on_startup
from info import *
from utils import temp
from Script import script
from plugins import web_server, check_expired_premium
from Lucia.Bot import SilentX
from Lucia.util.keepalive import ping_server
from Lucia.Bot.clients import initialize_clients
import pyrogram.utils
from PIL import Image
import threading, time, requests

logging.config.fileConfig('logging.conf')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("imdbpy").setLevel(logging.ERROR)
logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

botStartTime = time.time()
ppath = "plugins/*.py"
files = glob.glob(ppath)

pyrogram.utils.MIN_CHANNEL_ID = -1009147483647


# -------------------------------------------------
# 🔄 SERVER PING LOOP
# -------------------------------------------------
def ping_loop():
    while True:
        try:
            r = requests.get(URL, timeout=10)
            if r.status_code == 200:
                print("✅ Ping Successful")
            else:
                print(f"⚠️ Ping Failed: {r.status_code}")
        except Exception as e:
            print(f"❌ Exception During Ping: {e}")
        time.sleep(120)


threading.Thread(target=ping_loop, daemon=True).start()


# -------------------------------------------------
# 🔔 NEW CACHED INITIALIZATION (on_ready)
# -------------------------------------------------
@SilentX.on_ready()
async def startup():
    await initialize_cache_on_startup()
    print("Bot is ready!")


# -------------------------------------------------
# 🚀 BOT STARTUP
# -------------------------------------------------
async def SilentXBotz_start():
    print('Initalizing Your Bot!')
    await SilentX.start()
    bot_info = await SilentX.get_me()
    SilentX.username = bot_info.username

    # Initialize multi-clients
    await initialize_clients()

    # Load plugins
    for name in files:
        with open(name) as a:
            patt = Path(a.name)
            plugin_name = patt.stem.replace(".py", "")
            plugins_dir = Path(f"plugins/{plugin_name}.py")
            import_path = "plugins.{}".format(plugin_name)
            spec = importlib.util.spec_from_file_location(import_path, plugins_dir)
            load = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(load)
            sys.modules["plugins." + plugin_name] = load
            print("Import Plugins - " + plugin_name)

    # Keepalive ping for Heroku
    if ON_HEROKU:
        asyncio.create_task(ping_server())

    # Banned users
    b_users, b_chats = await db.get_banned()
    temp.BANNED_USERS = b_users
    temp.BANNED_CHATS = b_chats

    # Create DB indexes
    await Media.ensure_indexes()
    if MULTIPLE_DB:
        await Media2.ensure_indexes()
        print("Multiple Database Mode On. Files Will Go To Second DB If First DB Is Full")
    else:
        print("Single DB Mode: Using Primary Database")

    # Bot Info
    me = await SilentX.get_me()
    temp.ME = me.id
    temp.U_NAME = me.username
    temp.B_NAME = me.first_name
    temp.B_LINK = me.mention
    SilentX.username = '@' + me.username

    # Premium Expiry Checker
    SilentX.loop.create_task(check_expired_premium(SilentX))

    logging.info(f"{me.first_name} with Pyrogram v{__version__} (Layer {layer}) started on {me.username}.")
    logging.info(script.LOGO)

    # Restart Log Message
    tz = pytz.timezone('Asia/Kolkata')
    today = date.today()
    now = datetime.now(tz)
    time_now = now.strftime("%H:%M:%S %p")

    await SilentX.send_message(
        chat_id=LOG_CHANNEL,
        text=script.RESTART_TXT.format(temp.B_LINK, today, time_now)
    )

    # Notify Admins
    try:
        for admin in ADMINS:
            await SilentX.send_message(
                chat_id=admin,
                text=f"<b>๏[-ิ_•ิ]๏ {me.mention} Restarted ✅</b>"
            )
    except:
        pass

    # Start Web Server
    app = web.AppRunner(await web_server())
    await app.setup()
    bind_address = "0.0.0.0"
    await web.TCPSite(app, bind_address, PORT).start()

    await idle()


# -------------------------------------------------
# 🟢 MAIN ENTRY
# -------------------------------------------------
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(SilentXBotz_start())
    except KeyboardInterrupt:
        logging.info('Service Stopped Bye 👋')
