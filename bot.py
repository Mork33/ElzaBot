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
from database.ia_filterdb import Media, Media2, initialize_database_cache, is_cache_ready, get_cache_info
from database.users_chats_db import db
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

# ============================================================================
# IMPORT CACHE INITIALIZATION FUNCTIONS
# ============================================================================
from database.ia_filterdb import (
    initialize_database_cache, 
    is_cache_ready, 
    get_cache_info
)

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

async def SilentXBotz_start():
    print('Initalizing Your Bot!')
    await SilentX.start()
    bot_info = await SilentX.get_me()
    SilentX.username = bot_info.username
    await initialize_clients()
    
    # ========================================================================
    # INITIALIZE DATABASE CACHE - LOAD ALL FILES INTO MEMORY
    # ========================================================================
    print("\n" + "=" * 70)
    print("🚀 INITIALIZING PERSISTENT DATABASE CACHE")
    print("=" * 70)
    
    try:
        # Load entire database into memory
        await initialize_database_cache()
        
        # Verify cache loaded successfully
        if is_cache_ready():
            print("\n" + "=" * 70)
            print("✅ DATABASE CACHE INITIALIZED SUCCESSFULLY!")
            print("=" * 70)
            
            # Display cache statistics
            cache_info = get_cache_info()
            print(f"📊 Total Files Cached: {cache_info['total_files']:,}")
            print(f"💾 Memory Usage: {cache_info['memory_mb']} MB")
            print(f"🔍 Search Index Terms: {cache_info['search_index_terms']:,}")
            print(f"📅 Cache Updated: {cache_info['last_updated']}")
            print("=" * 70)
            print("⚡ All searches will now be INSTANT from memory cache!")
            print("=" * 70 + "\n")
            
            # Log to channel
            await SilentX.send_message(
                chat_id=LOG_CHANNEL, 
                text=f"<b>✅ DATABASE CACHE LOADED SUCCESSFULLY!</b>\n\n"
                     f"📊 Total Files: <code>{cache_info['total_files']:,}</code>\n"
                     f"💾 Memory: <code>{cache_info['memory_mb']} MB</code>\n"
                     f"🔍 Index Terms: <code>{cache_info['search_index_terms']:,}</code>\n\n"
                     f"⚡ Bot is ready with instant search!"
            )
        else:
            print("\n" + "=" * 70)
            print("❌ CACHE INITIALIZATION FAILED!")
            print("=" * 70)
            print("⚠️ Bot will continue but searches may be slow")
            print("=" * 70 + "\n")
            
            # Log failure to channel
            await SilentX.send_message(
                chat_id=LOG_CHANNEL, 
                text="<b>⚠️ Cache initialization failed! Bot running with database queries.</b>"
            )
    except Exception as e:
        print("\n" + "=" * 70)
        print(f"❌ CACHE INITIALIZATION ERROR: {e}")
        print("=" * 70)
        print("⚠️ Bot will continue but searches may be slow")
        print("=" * 70 + "\n")
        logging.error(f"Cache initialization error: {e}")
        
        # Log error to channel
        try:
            await SilentX.send_message(
                chat_id=LOG_CHANNEL, 
                text=f"<b>❌ Cache initialization error:</b>\n<code>{e}</code>"
            )
        except:
            pass
    
    # ========================================================================
    # CONTINUE WITH NORMAL BOT INITIALIZATION
    # ========================================================================
    
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
    
    if ON_HEROKU:
        asyncio.create_task(ping_server()) 
    
    b_users, b_chats = await db.get_banned()
    temp.BANNED_USERS = b_users
    temp.BANNED_CHATS = b_chats
    
    await Media.ensure_indexes()
    if MULTIPLE_DB:
        await Media2.ensure_indexes()
        print("Multiple Database Mode On. Now Files Will Be Save In Second DB If First DB Is Full")
    else:
        print("Single DB Mode On ! Files Will Be Save In First Database")
    
    me = await SilentX.get_me()
    temp.ME = me.id
    temp.U_NAME = me.username
    temp.B_NAME = me.first_name
    temp.B_LINK = me.mention
    SilentX.username = '@' + me.username
    SilentX.loop.create_task(check_expired_premium(SilentX))
    
    logging.info(f"{me.first_name} with Pyrogram v{__version__} (Layer {layer}) started on {me.username}.")
    logging.info(script.LOGO)
    
    tz = pytz.timezone('Asia/Kolkata')
    today = date.today()
    now = datetime.now(tz)
    time = now.strftime("%H:%M:%S %p")
    
    await SilentX.send_message(
        chat_id=LOG_CHANNEL, 
        text=script.RESTART_TXT.format(temp.B_LINK, today, time)
    )
    
    try:
        for admin in ADMINS:
            await SilentX.send_message(
                chat_id=admin, 
                text=f"<b>๏[-ิ_•ิ]๏ {me.mention} Restarted ✅</code></b>"
            )
    except:
        pass
    
    app = web.AppRunner(await web_server())
    await app.setup()
    bind_address = "0.0.0.0"
    await web.TCPSite(app, bind_address, PORT).start()
    
    await idle()
    
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(SilentXBotz_start())
    except KeyboardInterrupt:
        logging.info('Service Stopped Bye 👋')
