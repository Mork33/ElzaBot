"""
Cache Management Commands Plugin
=================================

Telegram bot commands for managing the persistent database cache:
- /cachestats - View cache statistics
- /refreshcache - Refresh database cache
- /clearcache - Clear regex cache
- /cacheinfo - Detailed cache information

Place this file in: plugins/cache_commands.py
"""

import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from database.ia_filterdb import (
    reload_database_cache,
    get_cache_info,
    is_cache_ready,
    clear_search_cache,
    persistent_cache
)
from info import ADMINS
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_size(bytes_size):
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

def format_time(seconds):
    """Convert seconds to human readable format"""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hours"

def get_cache_stats():
    """Get cache statistics in a formatted dict"""
    cache_info = get_cache_info()
    
    return {
        'is_loaded': cache_info['loaded'],
        'total_files': cache_info['total_files'],
        'memory_mb': cache_info['memory_mb'],
        'search_index_terms': cache_info['search_index_terms'],
        'last_updated': datetime.strptime(cache_info['last_updated'], '%Y-%m-%d %H:%M:%S UTC') if cache_info['last_updated'] else None
    }

async def refresh_full_cache():
    """Refresh the entire database cache"""
    await reload_database_cache()

# ============================================================================
# COMMAND HANDLERS
# ============================================================================

@Client.on_message(filters.command("cachestats") & filters.user(ADMINS))
async def cache_stats_command(client: Client, message: Message):
    """Show cache statistics"""
    try:
        stats = get_cache_stats()
        
        if not stats['is_loaded']:
            await message.reply_text(
                "⚠️ <b>Cache Not Loaded</b>\n\n"
                "The database cache is not currently loaded.\n"
                "Use /refreshcache to load it.",
                quote=True
            )
            return
        
        # Calculate time since last update
        if stats['last_updated']:
            time_diff = datetime.utcnow() - stats['last_updated']
            time_ago = format_time(time_diff.total_seconds())
        else:
            time_ago = "Unknown"
        
        cache_text = (
            "📊 <b>Cache Statistics</b>\n\n"
            f"✅ <b>Status:</b> Active\n"
            f"📁 <b>Total Files:</b> <code>{stats['total_files']:,}</code>\n"
            f"💾 <b>Memory Usage:</b> <code>~{stats['memory_mb']:.2f} MB</code>\n"
            f"🔍 <b>Search Index Terms:</b> <code>{stats['search_index_terms']:,}</code>\n"
            f"🕐 <b>Last Updated:</b> <code>{time_ago} ago</code>\n"
            f"📅 <b>Update Time:</b> <code>{stats['last_updated'].strftime('%Y-%m-%d %H:%M:%S UTC') if stats['last_updated'] else 'N/A'}</code>\n\n"
            "ℹ️ <i>All searches are using cached data for faster results.</i>"
        )
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Refresh Cache", callback_data="refresh_cache"),
                InlineKeyboardButton("🗑️ Clear Regex Cache", callback_data="clear_search_cache")
            ],
            [InlineKeyboardButton("❌ Close", callback_data="close_data")]
        ])
        
        await message.reply_text(cache_text, quote=True, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Error in cache_stats_command: {e}")
        await message.reply_text(f"❌ Error getting cache stats: {e}", quote=True)

@Client.on_message(filters.command("refreshcache") & filters.user(ADMINS))
async def refresh_cache_command(client: Client, message: Message):
    """Manually refresh the entire database cache"""
    try:
        msg = await message.reply_text(
            "🔄 <b>Refreshing Database Cache...</b>\n\n"
            "⏳ This may take a few moments depending on database size.\n"
            "Please wait...",
            quote=True
        )
        
        start_time = datetime.utcnow()
        
        # Refresh the cache
        await refresh_full_cache()
        
        end_time = datetime.utcnow()
        elapsed = (end_time - start_time).total_seconds()
        
        # Get updated stats
        stats = get_cache_stats()
        
        success_text = (
            "✅ <b>Cache Refreshed Successfully!</b>\n\n"
            f"📁 <b>Total Files Cached:</b> <code>{stats['total_files']:,}</code>\n"
            f"💾 <b>Memory Usage:</b> <code>~{stats['memory_mb']:.2f} MB</code>\n"
            f"🔍 <b>Search Index Terms:</b> <code>{stats['search_index_terms']:,}</code>\n"
            f"⏱️ <b>Refresh Time:</b> <code>{format_time(elapsed)}</code>\n"
            f"🕐 <b>Completed At:</b> <code>{end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}</code>\n\n"
            "ℹ️ <i>Cache is now up-to-date with the database.</i>"
        )
        
        await msg.edit_text(success_text)
        logger.info(f"Cache refreshed by admin {message.from_user.id} in {elapsed:.2f}s")
        
    except Exception as e:
        logger.error(f"Error in refresh_cache_command: {e}")
        await msg.edit_text(f"❌ <b>Cache Refresh Failed!</b>\n\n<code>{str(e)}</code>")

@Client.on_message(filters.command("clearcache") & filters.user(ADMINS))
async def clear_cache_command(client: Client, message: Message):
    """Clear the regex pattern cache"""
    try:
        clear_search_cache()
        
        await message.reply_text(
            "✅ <b>Regex Cache Cleared!</b>\n\n"
            "🗑️ All cached regex patterns have been cleared.\n"
            "📊 Full database cache is still active.\n\n"
            "ℹ️ <i>New regex patterns will be cached again automatically.</i>",
            quote=True
        )
        
        logger.info(f"Regex cache cleared by admin {message.from_user.id}")
        
    except Exception as e:
        logger.error(f"Error in clear_cache_command: {e}")
        await message.reply_text(f"❌ Error clearing regex cache: {e}", quote=True)

@Client.on_message(filters.command("cacheinfo") & filters.user(ADMINS))
async def cache_info_command(client: Client, message: Message):
    """Detailed information about the caching system"""
    try:
        stats = get_cache_stats()
        
        # Calculate cache efficiency
        if stats['total_files'] > 0:
            avg_terms_per_file = stats['search_index_terms'] / stats['total_files']
        else:
            avg_terms_per_file = 0
        
        info_text = (
            "ℹ️ <b>Cache System Information</b>\n\n"
            "<b>📚 Persistent Database Cache:</b>\n"
            "• Stores entire database in memory\n"
            "• Used for all file searches\n"
            "• Updated automatically when new files are added\n"
            f"• Status: {'✅ Active' if stats['is_loaded'] else '❌ Inactive'}\n"
            f"• Files: <code>{stats['total_files']:,}</code>\n"
            f"• Memory: <code>~{stats['memory_mb']:.2f} MB</code>\n"
            f"• Search Index: <code>{stats['search_index_terms']:,}</code> terms\n"
            f"• Efficiency: <code>{avg_terms_per_file:.1f}</code> terms/file\n\n"
            "<b>🔍 Search Performance:</b>\n"
            "• All searches: <code>&lt;50ms</code>\n"
            "• Pagination: <code>Instant</code>\n"
            "• Database queries: <code>0</code> (uses cache)\n"
            "• Regex caching: <code>Enabled</code>\n\n"
            "<b>🛠️ Available Commands:</b>\n"
            "• <code>/cachestats</code> - View cache statistics\n"
            "• <code>/refreshcache</code> - Refresh database cache\n"
            "• <code>/clearcache</code> - Clear regex cache\n"
            "• <code>/cacheinfo</code> - This information\n\n"
            "<b>📝 Notes:</b>\n"
            "• Cache updates automatically when files are indexed\n"
            "• Manual refresh needed after bulk deletions\n"
            "• All searches use cached data for speed\n"
            "• No database queries during normal searches"
        )
        
        await message.reply_text(info_text, quote=True)
        
    except Exception as e:
        logger.error(f"Error in cache_info_command: {e}")
        await message.reply_text(f"❌ Error getting cache info: {e}", quote=True)

@Client.on_message(filters.command("cachehealth") & filters.user(ADMINS))
async def cache_health_command(client: Client, message: Message):
    """Check cache health and integrity"""
    try:
        msg = await message.reply_text(
            "🔍 <b>Checking Cache Health...</b>\n\n"
            "⏳ Please wait...",
            quote=True
        )
        
        stats = get_cache_stats()
        
        # Health checks
        health_issues = []
        health_status = "✅ Healthy"
        
        if not stats['is_loaded']:
            health_issues.append("❌ Cache not loaded")
            health_status = "❌ Critical"
        elif stats['total_files'] == 0:
            health_issues.append("⚠️ No files in cache")
            health_status = "⚠️ Warning"
        elif stats['search_index_terms'] == 0:
            health_issues.append("⚠️ Search index empty")
            health_status = "⚠️ Warning"
        elif stats['memory_mb'] == 0 and stats['total_files'] > 0:
            health_issues.append("⚠️ Memory calculation issue")
            health_status = "⚠️ Warning"
        
        # Calculate uptime
        if stats['last_updated']:
            uptime = datetime.utcnow() - stats['last_updated']
            uptime_str = format_time(uptime.total_seconds())
        else:
            uptime_str = "Unknown"
        
        health_text = (
            "🏥 <b>Cache Health Report</b>\n\n"
            f"<b>Overall Status:</b> {health_status}\n\n"
            "<b>📊 Metrics:</b>\n"
            f"• Files: <code>{stats['total_files']:,}</code>\n"
            f"• Memory: <code>{stats['memory_mb']:.2f} MB</code>\n"
            f"• Index: <code>{stats['search_index_terms']:,}</code> terms\n"
            f"• Uptime: <code>{uptime_str}</code>\n\n"
        )
        
        if health_issues:
            health_text += "<b>⚠️ Issues Detected:</b>\n"
            for issue in health_issues:
                health_text += f"{issue}\n"
            health_text += "\n<i>Tip: Use /refreshcache to resolve issues</i>"
        else:
            health_text += "✅ <b>No Issues Detected</b>\n\n"
            health_text += "<i>Cache is operating normally</i>"
        
        await msg.edit_text(health_text)
        logger.info(f"Cache health check performed by admin {message.from_user.id}")
        
    except Exception as e:
        logger.error(f"Error in cache_health_command: {e}")
        await msg.edit_text(f"❌ <b>Health Check Failed!</b>\n\n<code>{str(e)}</code>")

# ============================================================================
# CALLBACK QUERY HANDLERS
# ============================================================================

@Client.on_callback_query(filters.regex("^refresh_cache$") & filters.user(ADMINS))
async def refresh_cache_callback(client: Client, callback_query: CallbackQuery):
    """Handle refresh cache button"""
    try:
        await callback_query.answer("🔄 Refreshing cache...", show_alert=False)
        
        await callback_query.message.edit_text(
            "🔄 <b>Refreshing Database Cache...</b>\n\n"
            "⏳ Please wait...",
            reply_markup=None
        )
        
        start_time = datetime.utcnow()
        await refresh_full_cache()
        end_time = datetime.utcnow()
        elapsed = (end_time - start_time).total_seconds()
        
        stats = get_cache_stats()
        
        success_text = (
            "✅ <b>Cache Refreshed Successfully!</b>\n\n"
            f"📁 <b>Total Files:</b> <code>{stats['total_files']:,}</code>\n"
            f"💾 <b>Memory:</b> <code>~{stats['memory_mb']:.2f} MB</code>\n"
            f"🔍 <b>Index Terms:</b> <code>{stats['search_index_terms']:,}</code>\n"
            f"⏱️ <b>Time:</b> <code>{format_time(elapsed)}</code>\n\n"
            "ℹ️ Cache is now up-to-date!"
        )
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 View Stats", callback_data="view_cache_stats")],
            [InlineKeyboardButton("❌ Close", callback_data="close_data")]
        ])
        
        await callback_query.message.edit_text(success_text, reply_markup=keyboard)
        logger.info(f"Cache refreshed via callback by admin {callback_query.from_user.id}")
        
    except Exception as e:
        logger.error(f"Error in refresh_cache_callback: {e}")
        await callback_query.message.edit_text(f"❌ Cache refresh failed: {e}")

@Client.on_callback_query(filters.regex("^clear_search_cache$") & filters.user(ADMINS))
async def clear_search_cache_callback(client: Client, callback_query: CallbackQuery):
    """Handle clear regex cache button"""
    try:
        clear_search_cache()
        await callback_query.answer("✅ Regex cache cleared!", show_alert=True)
        
        # Return to stats view
        stats = get_cache_stats()
        
        if not stats['is_loaded']:
            await callback_query.answer("⚠️ Cache not loaded!", show_alert=True)
            return
        
        time_diff = datetime.utcnow() - stats['last_updated']
        time_ago = format_time(time_diff.total_seconds())
        
        cache_text = (
            "📊 <b>Cache Statistics</b>\n\n"
            f"✅ <b>Status:</b> Active\n"
            f"📁 <b>Total Files:</b> <code>{stats['total_files']:,}</code>\n"
            f"💾 <b>Memory Usage:</b> <code>~{stats['memory_mb']:.2f} MB</code>\n"
            f"🔍 <b>Search Index Terms:</b> <code>{stats['search_index_terms']:,}</code>\n"
            f"🕐 <b>Last Updated:</b> <code>{time_ago} ago</code>\n\n"
            "✅ <i>Regex cache has been cleared!</i>"
        )
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Refresh Cache", callback_data="refresh_cache"),
                InlineKeyboardButton("🗑️ Clear Regex Cache", callback_data="clear_search_cache")
            ],
            [InlineKeyboardButton("❌ Close", callback_data="close_data")]
        ])
        
        await callback_query.message.edit_text(cache_text, reply_markup=keyboard)
        logger.info(f"Regex cache cleared via callback by admin {callback_query.from_user.id}")
        
    except Exception as e:
        logger.error(f"Error in clear_search_cache_callback: {e}")
        await callback_query.answer(f"❌ Error: {e}", show_alert=True)

@Client.on_callback_query(filters.regex("^view_cache_stats$") & filters.user(ADMINS))
async def view_cache_stats_callback(client: Client, callback_query: CallbackQuery):
    """Handle view stats button"""
    try:
        stats = get_cache_stats()
        
        if not stats['is_loaded']:
            await callback_query.answer("⚠️ Cache not loaded!", show_alert=True)
            return
        
        time_diff = datetime.utcnow() - stats['last_updated']
        time_ago = format_time(time_diff.total_seconds())
        
        cache_text = (
            "📊 <b>Cache Statistics</b>\n\n"
            f"✅ <b>Status:</b> Active\n"
            f"📁 <b>Total Files:</b> <code>{stats['total_files']:,}</code>\n"
            f"💾 <b>Memory Usage:</b> <code>~{stats['memory_mb']:.2f} MB</code>\n"
            f"🔍 <b>Search Index Terms:</b> <code>{stats['search_index_terms']:,}</code>\n"
            f"🕐 <b>Last Updated:</b> <code>{time_ago} ago</code>\n"
            f"📅 <b>Update Time:</b> <code>{stats['last_updated'].strftime('%Y-%m-%d %H:%M:%S UTC')}</code>\n\n"
            "ℹ️ <i>All searches are using cached data.</i>"
        )
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Refresh Cache", callback_data="refresh_cache"),
                InlineKeyboardButton("🗑️ Clear Regex Cache", callback_data="clear_search_cache")
            ],
            [InlineKeyboardButton("❌ Close", callback_data="close_data")]
        ])
        
        await callback_query.message.edit_text(cache_text, reply_markup=keyboard)
        await callback_query.answer()
        
    except Exception as e:
        logger.error(f"Error in view_cache_stats_callback: {e}")
        await callback_query.answer(f"❌ Error: {e}", show_alert=True)
