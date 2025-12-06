import logging
from struct import pack
import re
import base64
import asyncio
from typing import Dict, List, Tuple, Optional
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow.exceptions import ValidationError
from info import *
from utils import get_settings, save_group_settings
from collections import defaultdict
from logging_helper import LOGGER
from datetime import datetime, timedelta
from functools import lru_cache

# Cache configuration
_db_stats_cache_primary = {
    "timestamp": None,
    "primary_size": 0
}
_db_stats_cache_secondary = {
    "timestamp": None,
    "primary_size": 0
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

processed_movies = set()

MONGODB_SIZE_LIMIT = (512 * 1024 * 1024) - (80 * 1024 * 1024) 

# Optimized MongoDB connections with connection pooling
client = AsyncIOMotorClient(
    DATABASE_URI,
    maxPoolSize=50,
    minPoolSize=10,
    maxIdleTimeMS=45000,
    connectTimeoutMS=10000,
    serverSelectionTimeoutMS=10000
)
db = client[DATABASE_NAME]
instance = Instance.from_db(db)

client2 = AsyncIOMotorClient(
    DATABASE_URI2,
    maxPoolSize=50,
    minPoolSize=10,
    maxIdleTimeMS=45000,
    connectTimeoutMS=10000,
    serverSelectionTimeoutMS=10000
)
db2 = client2[DATABASE_NAME]
instance2 = Instance.from_db(db2)

# Pre-compiled regex patterns for file name cleaning
SPECIAL_CHARS_PATTERN = re.compile(r"[@\-\.#+$%^*()~`,;:\"?/<>\[\]{}=|\\]")
KEYWORDS_PATTERN = re.compile(
    "|".join(re.escape(keyword) for keyword in [
        "_", "Adrama_lovers", "DA_Rips", "DramaPz", "ADL_Drama", "KDL", 
        "ADL", "KncKorean", "YDF", "The_request_group", "The_Drama_arena", 
        "RFT", "kdramaforyouall"
    ]),
    flags=re.IGNORECASE
)
SPACES_PATTERN = re.compile(r"\s+")

# ============================================================================
# PERSISTENT IN-MEMORY DATABASE CACHE
# ============================================================================

class PersistentDatabaseCache:
    """
    Complete in-memory database cache - loads ALL files on bot startup
    
    Features:
    - Loads entire MongoDB database into memory on initialization
    - All searches done in memory (NO database queries)
    - Instant pagination (data already in memory)
    - Auto-refresh when new files added
    - Thread-safe operations
    - NEWEST FILES FIRST (reversed order for pagination)
    """
    
    def __init__(self):
        self.all_files = []  # Complete list of all files from DB (newest first)
        self.total_files = 0
        self.cache_loaded = False
        self.last_updated = None
        self.loading_lock = asyncio.Lock()
        self._search_index = {}  # Pre-built search index for faster lookups
        
    async def initialize_cache(self):
        """
        Load ALL files from MongoDB into memory on bot startup
        Files are stored in REVERSE order (newest first, oldest last)
        This is called once when bot starts
        """
        async with self.loading_lock:
            if self.cache_loaded:
                logger.info("⚠️ Cache already loaded, skipping initialization")
                return
            
            logger.info("=" * 70)
            logger.info("🚀 INITIALIZING PERSISTENT DATABASE CACHE")
            logger.info("=" * 70)
            logger.info("📥 Loading ALL files from MongoDB into server memory...")
            logger.info("📌 Order: NEWEST FIRST → OLDEST LAST")
            
            start_time = datetime.utcnow()
            
            try:
                # Projection for data retrieval
                projection = {
                    'file_id': 1,
                    'file_ref': 1,
                    'file_name': 1,
                    'file_size': 1,
                    'file_type': 1,
                    'caption': 1,
                    'mime_type': 1
                }
                
                if MULTIPLE_DB:
                    # Load from both databases (newest first - reversed)
                    logger.info("🔄 Loading from PRIMARY database...")
                    count1 = await Media.count_documents({})
                    cursor1 = Media.find({}, projection).sort('$natural', -1)  # -1 = descending (newest first)
                    files1 = await cursor1.to_list(length=count1) if count1 > 0 else []
                    
                    logger.info(f"✅ Loaded {len(files1)} files from PRIMARY database (newest first)")
                    
                    logger.info("🔄 Loading from SECONDARY database...")
                    count2 = await Media2.count_documents({})
                    cursor2 = Media2.find({}, projection).sort('$natural', -1)  # -1 = descending (newest first)
                    files2 = await cursor2.to_list(length=count2) if count2 > 0 else []
                    
                    logger.info(f"✅ Loaded {len(files2)} files from SECONDARY database (newest first)")
                    
                    # Combine files (newest from both databases first)
                    self.all_files = files1 + files2
                else:
                    # Load from single database (newest first - reversed)
                    logger.info("🔄 Loading from database...")
                    self.total_files = await Media.count_documents({})
                    cursor = Media.find({}, projection).sort('$natural', -1)  # -1 = descending (newest first)
                    self.all_files = await cursor.to_list(length=self.total_files) if self.total_files > 0 else []
                    
                    logger.info(f"✅ Loaded {len(self.all_files)} files from database (newest first)")
                
                self.total_files = len(self.all_files)
                self.cache_loaded = True
                self.last_updated = datetime.utcnow()
                
                # Build search index for faster lookups
                await self._build_search_index()
                
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                memory_mb = (self.total_files * 2) / 1024  # Rough estimate: 2KB per file
                
                logger.info("=" * 70)
                logger.info("✅ CACHE IMPORTED SUCCESSFULLY!")
                logger.info("=" * 70)
                logger.info(f"📊 Total Files Cached: {self.total_files:,}")
                logger.info(f"💾 Estimated Memory Usage: {memory_mb:.2f} MB")
                logger.info(f"⏱️ Load Time: {elapsed:.2f} seconds")
                logger.info(f"🔍 Search Index Built: {len(self._search_index):,} entries")
                logger.info(f"📅 Cache Timestamp: {self.last_updated.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                logger.info(f"📌 File Order: NEWEST FIRST → OLDEST LAST")
                logger.info("=" * 70)
                logger.info("🎯 Bot is now ready to serve searches from memory!")
                logger.info("⚡ All searches and pagination will be INSTANT!")
                logger.info("=" * 70)
                
            except Exception as e:
                logger.error("=" * 70)
                logger.error(f"❌ CACHE INITIALIZATION FAILED: {e}")
                logger.error("=" * 70)
                self.cache_loaded = False
                raise
    
    async def _build_search_index(self):
        """Build search index for faster file lookups"""
        logger.info("🔨 Building search index...")
        
        for idx, file in enumerate(self.all_files):
            file_name = getattr(file, 'file_name', '').lower()
            caption = getattr(file, 'caption', '')
            
            if caption:
                caption = caption.lower()
            
            # Index by file name words
            words = file_name.split()
            for word in words:
                if len(word) >= 2:  # Only index words 2+ chars
                    if word not in self._search_index:
                        self._search_index[word] = []
                    self._search_index[word].append(idx)
            
            # Index by caption words if caption filtering enabled
            if USE_CAPTION_FILTER and caption:
                caption_words = caption.split()
                for word in caption_words:
                    if len(word) >= 2:
                        if word not in self._search_index:
                            self._search_index[word] = []
                        if idx not in self._search_index[word]:
                            self._search_index[word].append(idx)
        
        logger.info(f"✅ Search index built with {len(self._search_index):,} unique terms")
    
    async def reload_cache(self):
        """
        Reload entire cache from database
        Use this after bulk file uploads or major database changes
        """
        logger.info("🔄 Reloading entire cache from database...")
        self.cache_loaded = False
        self.all_files.clear()
        self._search_index.clear()
        await self.initialize_cache()
    
    def add_file_to_cache_sync(self, file_dict: dict):
        """
        Synchronously add a file to cache (for bulk indexing)
        New files are added at the BEGINNING (newest first)
        """
        if not self.cache_loaded:
            return
        
        # Insert at the beginning (index 0) to maintain newest-first order
        self.all_files.insert(0, type('obj', (object,), file_dict)())
        self.total_files += 1
        self.last_updated = datetime.utcnow()
        
        # Update search index
        file_name = file_dict.get('file_name', '').lower()
        caption = file_dict.get('caption', '') or ''
        idx = 0  # New file is at index 0 (first position)
        
        # Shift all existing indices by 1
        for word_indices in self._search_index.values():
            for i in range(len(word_indices)):
                word_indices[i] += 1
        
        if caption:
            caption = caption.lower()
        
        words = file_name.split()
        for word in words:
            if len(word) >= 2:
                if word not in self._search_index:
                    self._search_index[word] = []
                self._search_index[word].insert(0, idx)  # Insert at beginning
        
        if USE_CAPTION_FILTER and caption:
            caption_words = caption.split()
            for word in caption_words:
                if len(word) >= 2:
                    if word not in self._search_index:
                        self._search_index[word] = []
                    if idx not in self._search_index[word]:
                        self._search_index[word].insert(0, idx)  # Insert at beginning
    
    async def add_file_to_cache(self, file_data: dict):
        """
        Add a single new file to the cache (async version)
        New files are added at the BEGINNING (newest first)
        Called when a new file is saved to database
        """
        if not self.cache_loaded:
            logger.warning("⚠️ Cache not loaded, skipping file addition")
            return
        
        self.add_file_to_cache_sync(file_data)
        logger.info(f"➕ Added file to cache (at top): {file_data.get('file_name', 'Unknown')} (Total: {self.total_files:,})")
    
    def search_in_cache(self, query: str, file_type: Optional[str] = None) -> List:
        """
        Search files in memory cache (NO DATABASE QUERY)
        Returns results with NEWEST files first, OLDEST last
        
        Args:
            query: Search query (e.g., "my demon")
            file_type: Filter by file type (optional)
            
        Returns:
            List of matching files (newest first, oldest last)
        """
        if not self.cache_loaded:
            logger.warning("⚠️ Cache not loaded yet!")
            return []
        
        start_time = datetime.utcnow()
        
        # Compile regex pattern
        regex = get_compiled_regex(query)
        if not regex:
            return []
        
        matching_files = []
        
        # Search through all cached files (already in newest-first order)
        for file in self.all_files:
            file_name = getattr(file, 'file_name', '')
            caption = getattr(file, 'caption', '')
            current_file_type = getattr(file, 'file_type', None)
            
            # Check file type filter
            if file_type and current_file_type != file_type:
                continue
            
            # Check if file name matches
            if regex.search(file_name):
                matching_files.append(file)
                continue
            
            # Check caption if caption filtering enabled
            if USE_CAPTION_FILTER and caption and regex.search(caption):
                matching_files.append(file)
        
        elapsed = (datetime.utcnow() - start_time).total_seconds() * 1000  # Convert to ms
        
        logger.info(f"🔍 Memory search for '{query}' completed in {elapsed:.2f}ms - Found {len(matching_files)} files (newest first)")
        
        return matching_files
    
    def get_cache_info(self) -> Dict:
        """Get current cache information"""
        memory_mb = (self.total_files * 2) / 1024 if self.total_files > 0 else 0
        
        return {
            'loaded': self.cache_loaded,
            'total_files': self.total_files,
            'memory_mb': round(memory_mb, 2),
            'last_updated': self.last_updated.strftime('%Y-%m-%d %H:%M:%S UTC') if self.last_updated else None,
            'search_index_terms': len(self._search_index),
            'sort_order': 'newest_first'
        }

# Initialize global persistent cache
persistent_cache = PersistentDatabaseCache()

# Optimized regex pattern caching
@lru_cache(maxsize=1000)
def get_compiled_regex(query: str):
    """Cache compiled regex patterns to avoid recompilation"""
    query = query.strip()
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    else:
        raw_pattern = query.replace(' ', r'.*[\s\.\+\-_()]')
    
    try:
        return re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        return None

@instance.register
class Media(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    
    class Meta:
        indexes = (
            '$file_name',
            [('file_name', 1), ('file_type', 1)],
            [('caption', 1)],
            [('file_type', 1)],
        )
        collection_name = COLLECTION_NAME

@instance2.register
class Media2(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    
    class Meta:
        indexes = (
            '$file_name',
            [('file_name', 1), ('file_type', 1)],
            [('caption', 1)],
            [('file_type', 1)],
        )
        collection_name = COLLECTION_NAME

async def check_db_size(db, cache):
    try:
        now = datetime.utcnow()
        cache_stale = cache["timestamp"] is None or \
                      (now - cache["timestamp"] > timedelta(minutes=10))
        if not cache_stale:
            return cache["primary_size"]
        dbstats = await db.command("dbStats")
        db_size = dbstats['dataSize']
        db_size_mb = db_size / (1024 * 1024) 
        cache["primary_size"] = db_size_mb
        cache["timestamp"] = now
        return db_size_mb
    except Exception as e:
        LOGGER.error(f"Error Checking Database Size: {e}")
        return 0

async def save_file(media):
    """Save file to database and add to memory cache (at the top - newest first)"""
    file_id, file_ref = unpack_new_file_id(media.file_id)
    
    file_name = SPECIAL_CHARS_PATTERN.sub(" ", str(media.file_name))
    file_name = KEYWORDS_PATTERN.sub(" ", file_name)
    file_name = SPACES_PATTERN.sub(" ", file_name).strip()

    primary_db_size = await check_db_size(db, _db_stats_cache_primary)
    use_secondary = False
    saveMedia = Media

    if MULTIPLE_DB and primary_db_size >= DB_CHANGE_LIMIT:
        LOGGER.info("Primary Database Is Low On Space. Switching To Secondary DB.")
        saveMedia = Media2
        use_secondary = True
            
    try:
        file = saveMedia(
            file_id=file_id,
            file_ref=file_ref,
            file_name=file_name,
            file_size=media.file_size,
            file_type=media.file_type,
            mime_type=media.mime_type,
            caption=media.caption.html if media.caption else None,
        )
        await file.commit()
        
        # Add to memory cache (at the top - newest first) - SYNCHRONOUS for speed
        file_data = {
            'file_id': file_id,
            'file_ref': file_ref,
            'file_name': file_name,
            'file_size': media.file_size,
            'file_type': media.file_type,
            'mime_type': media.mime_type,
            'caption': media.caption.html if media.caption else None
        }
        # Use sync version to avoid async overhead during bulk indexing
        persistent_cache.add_file_to_cache_sync(file_data)
        
    except DuplicateKeyError:
        LOGGER.error(f'{file_name} Is Already Saved In {"Secondary" if use_secondary else "Primary"} Database')
        return False, 0
    except ValidationError as e:
        LOGGER.error(f'Validation Error While Saving File: {e}')
        return False, 2
    else:
        return True, 1

async def get_search_results(chat_id, query, file_type=None, max_results=10, offset=0, filter=False):
    """
    Search files using in-memory cache (NO DATABASE QUERIES)
    All searches are instant because data is already in memory
    Results are returned with NEWEST files first, OLDEST files last
    
    Args:
        chat_id: Chat ID
        query: Search query (e.g., "my demon")
        file_type: Filter by file type
        max_results: Number of results per page
        offset: Pagination offset
        
    Returns:
        Tuple of (page_files, next_offset, total_results)
    """
    
    # Check if cache is loaded
    if not persistent_cache.cache_loaded:
        logger.warning("⚠️ Cache not loaded yet! Please wait for initialization to complete.")
        return [], '', 0
    
    # Get settings
    if chat_id is not None:
        settings = await get_settings(int(chat_id))
        try:
            max_results = 10 if settings.get('max_btn') else int(MAX_B_TN)
        except KeyError:
            await save_group_settings(int(chat_id), 'max_btn', False)
            settings = await get_settings(int(chat_id))
            max_results = 10 if settings.get('max_btn') else int(MAX_B_TN)

    # Ensure max_results is even
    if max_results % 2 != 0:
        max_results += 1

    # Search in memory cache (NO DATABASE QUERY!) - Results already in newest-first order
    all_matching_files = persistent_cache.search_in_cache(query, file_type)
    total_results = len(all_matching_files)
    
    # Get requested page (files already sorted newest first)
    page_files = all_matching_files[offset:offset + max_results]
    
    # Calculate next offset
    next_offset = offset + len(page_files)
    if next_offset >= total_results:
        next_offset = ''
    
    return page_files, next_offset, total_results

async def get_bad_files(query, file_type=None):
    """Get files for deletion/management from memory cache (newest first)"""
    if not persistent_cache.cache_loaded:
        logger.warning("⚠️ Cache not loaded yet!")
        return [], 0
    
    matching_files = persistent_cache.search_in_cache(query, file_type)
    return matching_files, len(matching_files)

async def get_file_details(query):
    """Get details of a specific file by file_id from memory cache"""
    if not persistent_cache.cache_loaded:
        logger.warning("⚠️ Cache not loaded yet! Falling back to database query...")
        filter_query = {'file_id': query}
        
        if MULTIPLE_DB:
            cursor1 = Media.find(filter_query)
            cursor2 = Media2.find(filter_query)
            
            results = await asyncio.gather(
                cursor1.to_list(length=1),
                cursor2.to_list(length=1)
            )
            
            return results[0] if results[0] else results[1]
        else:
            cursor = Media.find(filter_query)
            return await cursor.to_list(length=1)
    
    # Search in cache
    for file in persistent_cache.all_files:
        if getattr(file, 'file_id', None) == query:
            return [file]
    
    return []

def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref

async def siletxbotz_fetch_media(limit: int) -> List[dict]:
    """Fetch media from memory cache instead of database (newest first)"""
    try:
        if not persistent_cache.cache_loaded:
            logger.warning("⚠️ Cache not loaded, returning empty list")
            return []
        
        # Return latest files from cache (first N files since they're newest)
        return persistent_cache.all_files[:limit]
        
    except Exception as e:
        LOGGER.error(f"Error in siletxbotz_fetch_media: {e}")
        return []

async def silentxbotz_clean_title(filename: str, is_series: bool = False) -> str:
    try:
        year_match = re.search(r"^(.*?(\d{4}|\(\d{4}\)))", filename, re.IGNORECASE)
        if year_match:
            title = year_match.group(1).replace('(', '').replace(')', '') 
            return re.sub(r"[._\-\[\]@()]+", " ", title).strip().title()
        
        if is_series:
            season_match = re.search(r"(.*?)(?:S(\d{1,2})|Season\s*(\d+)|Season(\d+))(?:\s*Combined)?", filename, re.IGNORECASE)
            if season_match:
                title = season_match.group(1).strip()
                season = season_match.group(2) or season_match.group(3) or season_match.group(4)
                title = re.sub(r"[._\-\[\]@()]+", " ", title).strip().title()
                return f"{title} S{int(season):02}"
        
        return re.sub(r"[._\-\[\]@()]+", " ", filename).strip().title()
    except Exception as e:
        logger.error(f"Error in truncate_title: {e}")
        return filename

async def siletxbotz_get_movies(limit: int = 20) -> List[str]:
    try:
        cursor = await siletxbotz_fetch_media(limit * 2)
        results = set()
        pattern = r"(?:s\d{1,2}|season\s*\d+|season\d+)(?:\s*combined)?(?:e\d{1,2}|episode\s*\d+)?\b"
        
        for file in cursor:
            file_name = getattr(file, "file_name", "")
            caption = getattr(file, "caption", "")
            
            if not (re.search(pattern, file_name, re.IGNORECASE) or re.search(pattern, caption, re.IGNORECASE)):
                title = await silentxbotz_clean_title(file_name)
                results.add(title)
            
            if len(results) >= limit:
                break
        
        return sorted(list(results))[:limit]
    except Exception as e:
        logger.error(f"Error in siletxbotz_get_movies: {e}")
        return []

async def siletxbotz_get_series(limit: int = 30) -> Dict[str, List[int]]:
    try:
        cursor = await siletxbotz_fetch_media(limit * 5)
        grouped = defaultdict(list)
        pattern = r"(.*?)(?:S(\d{1,2})|Season\s*(\d+)|Season(\d+))(?:\s*Combined)?(?:E(\d{1,2})|Episode\s*(\d+))?\b"
        
        for file in cursor:
            file_name = getattr(file, "file_name", "")
            caption = getattr(file, "caption", "")
            
            match = None
            if file_name:
                match = re.search(pattern, file_name, re.IGNORECASE)
            if not match and caption:
                match = re.search(pattern, caption, re.IGNORECASE)
            
            if match:
                title = await silentxbotz_clean_title(match.group(1), is_series=True)
                season = int(match.group(2) or match.group(3) or match.group(4))
                grouped[title].append(season)
        
        return {title: sorted(set(seasons))[:10] for title, seasons in grouped.items() if seasons}
    except Exception as e:
        logger.error(f"Error in siletxbotz_get_series: {e}")
        return []

# ============================================================================
# CACHE MANAGEMENT FUNCTIONS
# ============================================================================

async def initialize_database_cache():
    """
    Initialize the persistent database cache
    MUST BE CALLED ON BOT STARTUP!
    Files are loaded in NEWEST FIRST order
    """
    await persistent_cache.initialize_cache()

async def reload_database_cache():
    """
    Reload entire cache from database
    Use after bulk file uploads or major database changes
    Files are reloaded in NEWEST FIRST order
    """
    logger.info("🔄 Reloading database cache...")
    await persistent_cache.reload_cache()

def get_cache_info() -> Dict:
    """
    Get current cache information and statistics
    
    Returns:
        Dict with cache status, file count, memory usage, sort order, etc.
    """
    info = persistent_cache.get_cache_info()
    return info

def is_cache_ready() -> bool:
    """Check if cache is loaded and ready"""
    return persistent_cache.cache_loaded

def clear_search_cache():
    """Clear regex cache"""
    get_compiled_regex.cache_clear()
    logger.info("🧹 Regex cache cleared")

# ============================================================================
# SORT ORDER EXPLANATION
# ============================================================================

"""
FILE ORDERING IN CACHE:
========================

✅ NEWEST FILES FIRST → OLDEST FILES LAST

When cache is loaded from database:
- Files are fetched with .sort('$natural', -1)
- This returns newest files first
- Cache maintains this order

When new file is added:
- Inserted at index 0 (first position)
- All existing files shift down
- Newest file stays at top

When searching:
- Results maintain cache order (newest first)
- Page 1 shows newest matching files
- Page 2 shows older matching files
- Last page shows oldest matching files

Example:
--------
Database has:
1. Movie_2025.mkv (newest - uploaded today)
2. Movie_2024.mkv 
3. Movie_2023.mkv (oldest - uploaded months ago)

Search results show:
Page 1: Movie_2025.mkv (newest)
Page 2: Movie_2024.mkv
Page 3: Movie_2023.mkv (oldest)

This ensures users always see the latest content first!
"""
