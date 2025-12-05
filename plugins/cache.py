import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from database.ia_filterdb import (
    refresh_full_cache, 
    get_cache_stats, 
    clear_search_cache,
    db_cache
)
from info import ADMINS
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Helper function to format bytes
def format_size(bytes_size):
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

# Helper function to format time
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
            f"🕐 <b>Last Updated:</b> <code>{time_ago} ago</code>\n"
            f"📅 <b>Update Time:</b> <code>{stats['last_updated'].strftime('%Y-%m-%d %H:%M:%S UTC') if stats['last_updated'] else 'N/A'}</code>\n\n"
            "ℹ️ <i>All searches are using cached data for faster results.</i>"
        )
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Refresh Cache", callback_data="refresh_cache"),
                InlineKeyboardButton("🗑️ Clear Search Cache", callback_data="clear_search_cache")
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
    """Clear the search result cache (not the full database cache)"""
    try:
        clear_search_cache()
        
        await message.reply_text(
            "✅ <b>Search Cache Cleared!</b>\n\n"
            "🗑️ All cached search results have been cleared.\n"
            "📊 Full database cache is still active.\n\n"
            "ℹ️ <i>New searches will be cached again automatically.</i>",
            quote=True
        )
        
        logger.info(f"Search cache cleared by admin {message.from_user.id}")
        
    except Exception as e:
        logger.error(f"Error in clear_cache_command: {e}")
        await message.reply_text(f"❌ Error clearing search cache: {e}", quote=True)

@Client.on_message(filters.command("cacheinfo") & filters.user(ADMINS))
async def cache_info_command(client: Client, message: Message):
    """Detailed information about the caching system"""
    try:
        stats = get_cache_stats()
        
        info_text = (
            "ℹ️ <b>Cache System Information</b>\n\n"
            "<b>📚 Database Cache:</b>\n"
            "• Stores entire database in memory\n"
            "• Used for all file searches\n"
            "• Updated automatically when new files are added\n"
            f"• Status: {'✅ Active' if stats['is_loaded'] else '❌ Inactive'}\n"
            f"• Files: {stats['total_files']:,}\n"
            f"• Memory: ~{stats['memory_mb']:.2f} MB\n\n"
            "<b>🔍 Search Result Cache:</b>\n"
            "• Stores recent search results\n"
            "• TTL: 5 minutes\n"
            "• Max size: 1000 queries\n"
            "• Cleared automatically when full\n\n"
            "<b>🛠️ Available Commands:</b>\n"
            "• /cachestats - View cache statistics\n"
            "• /refreshcache - Refresh database cache\n"
            "• /clearcache - Clear search result cache\n"
            "• /cacheinfo - This information\n\n"
            "<b>📝 Notes:</b>\n"
            "• Cache updates automatically when files are indexed\n"
            "• Manual refresh needed after bulk deletions\n"
            "• All searches use cached data for speed\n"
            "• Fallback to database if cache fails"
        )
        
        await message.reply_text(info_text, quote=True)
        
    except Exception as e:
        logger.error(f"Error in cache_info_command: {e}")
        await message.reply_text(f"❌ Error getting cache info: {e}", quote=True)

# Callback query handlers for inline buttons
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
    """Handle clear search cache button"""
    try:
        clear_search_cache()
        await callback_query.answer("✅ Search cache cleared!", show_alert=True)
        
        # Return to stats view
        stats = get_cache_stats()
        
        if not stats['is_loaded']:
            return
        
        time_diff = datetime.utcnow() - stats['last_updated']
        time_ago = format_time(time_diff.total_seconds())
        
        cache_text = (
            "📊 <b>Cache Statistics</b>\n\n"
            f"✅ <b>Status:</b> Active\n"
            f"📁 <b>Total Files:</b> <code>{stats['total_files']:,}</code>\n"
            f"💾 <b>Memory Usage:</b> <code>~{stats['memory_mb']:.2f} MB</code>\n"
            f"🕐 <b>Last Updated:</b> <code>{time_ago} ago</code>\n\n"
            "✅ <i>Search cache has been cleared!</i>"
        )
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Refresh Cache", callback_data="refresh_cache"),
                InlineKeyboardButton("🗑️ Clear Search Cache", callback_data="clear_search_cache")
            ],
            [InlineKeyboardButton("❌ Close", callback_data="close_data")]
        ])
        
        await callback_query.message.edit_text(cache_text, reply_markup=keyboard)
        logger.info(f"Search cache cleared via callback by admin {callback_query.from_user.id}")
        
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
            f"🕐 <b>Last Updated:</b> <code>{time_ago} ago</code>\n"
            f"📅 <b>Update Time:</b> <code>{stats['last_updated'].strftime('%Y-%m-%d %H:%M:%S UTC')}</code>\n\n"
            "ℹ️ <i>All searches are using cached data.</i>"
        )
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Refresh Cache", callback_data="refresh_cache"),
                InlineKeyboardButton("🗑️ Clear Search Cache", callback_data="clear_search_cache")
            ],
            [InlineKeyboardButton("❌ Close", callback_data="close_data")]
        ])
        
        await callback_query.message.edit_text(cache_text, reply_markup=keyboard)
        await callback_query.answer()
        
    except Exception as e:
        logger.error(f"Error in view_cache_stats_callback: {e}")
        await callback_query.answer(f"❌ Error: {e}", show_alert=True)
