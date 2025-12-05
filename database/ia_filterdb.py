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

# Complete Result Cache - Stores ALL results for a search query
class CompleteResultCache:
    def __init__(self, ttl_minutes=15, max_queries=100):
        """
        Cache complete search results for instant pagination without any DB queries
        
        When user searches "my demon":
        1. First time: Fetch ALL "my demon" results from DB and cache them
        2. All pagination: Use cached data (NO DB queries)
        
        Args:
            ttl_minutes: Cache validity period (default: 15 minutes)
            max_queries: Maximum search queries to cache (default: 100)
        """
        self.cache = {}  # Stores complete results
        self.ttl = timedelta(minutes=ttl_minutes)
        self.max_queries = max_queries
        self.access_times = {}  # Track last access for LRU eviction
        
    def _generate_cache_key(self, chat_id, query: str, file_type: Optional[str]) -> str:
        """Generate unique cache key for the search"""
        normalized_query = query.lower().strip()
        file_type_key = file_type or "all"
        return f"{chat_id}:{normalized_query}:{file_type_key}"
    
    def has_cache(self, chat_id, query: str, file_type: Optional[str] = None) -> bool:
        """Check if complete results are cached and valid"""
        cache_key = self._generate_cache_key(chat_id, query, file_type)
        
        if cache_key not in self.cache:
            return False
        
        # Check if cache expired
        cached_entry = self.cache[cache_key]
        age = datetime.utcnow() - cached_entry['cached_at']
        
        if age > self.ttl:
            # Cache expired, remove it
            logger.info(f"Cache EXPIRED for '{query}' (age: {age})")
            self._remove_cache_entry(cache_key)
            return False
        
        return True
    
    def get_full_results(self, chat_id, query: str, file_type: Optional[str] = None) -> Optional[Dict]:
        """
        Get complete cached results
        
        Returns:
            Dict with 'all_files' (complete list), 'total_count', 'cached_at'
            or None if not cached
        """
        cache_key = self._generate_cache_key(chat_id, query, file_type)
        
        if not self.has_cache(chat_id, query, file_type):
            logger.info(f"❌ Cache MISS for '{query}' in chat {chat_id}")
            return None
        
        # Update access time
        self.access_times[cache_key] = datetime.utcnow()
        
        logger.info(f"✅ Cache HIT for '{query}' in chat {chat_id} - {len(self.cache[cache_key]['all_files'])} files cached")
        return self.cache[cache_key]
    
    def cache_complete_results(self, chat_id, query: str, file_type: Optional[str], 
                              all_files: List, total_count: int):
        """
        Cache ALL search results for instant pagination
        
        Args:
            chat_id: Chat/user ID
            query: Search query (e.g., "my demon")
            file_type: File type filter
            all_files: Complete list of ALL matching files from DB
            total_count: Total number of files
        """
        cache_key = self._generate_cache_key(chat_id, query, file_type)
        
        # Evict old entries if cache is full
        if len(self.cache) >= self.max_queries:
            self._evict_old_entries()
        
        self.cache[cache_key] = {
            'all_files': all_files,
            'total_count': total_count,
            'cached_at': datetime.utcnow(),
            'query': query,
            'file_type': file_type
        }
        self.access_times[cache_key] = datetime.utcnow()
        
        logger.info(f"📦 CACHED complete results for '{query}' - {total_count} files stored in memory")
    
    def get_page(self, chat_id, query: str, file_type: Optional[str], 
                 offset: int, max_results: int) -> Optional[Tuple[List, str, int]]:
        """
        Get a specific page from cached complete results (NO DB QUERY)
        
        Args:
            chat_id: Chat/user ID
            query: Search query
            file_type: File type filter
            offset: Starting position
            max_results: Number of results per page
            
        Returns:
            Tuple of (page_files, next_offset, total_count) or None if not cached
        """
        cached_data = self.get_full_results(chat_id, query, file_type)
        
        if cached_data is None:
            return None
        
        all_files = cached_data['all_files']
        total_count = cached_data['total_count']
        
        # Extract the requested page
        start_idx = offset
        end_idx = offset + max_results
        page_files = all_files[start_idx:end_idx]
        
        # Calculate next offset
        next_offset = end_idx if end_idx < total_count else ''
        
        logger.info(f"📄 Serving page from cache: offset={offset}, results={len(page_files)}/{total_count} for '{query}'")
        
        return (page_files, next_offset, total_count)
    
    def _evict_old_entries(self):
        """Remove 20% oldest entries when cache is full"""
        if not self.cache:
            return
        
        evict_count = max(1, int(self.max_queries * 0.2))
        
        # Sort by last access time (oldest first)
        sorted_keys = sorted(
            self.access_times.keys(),
            key=lambda k: self.access_times.get(k, datetime.min)
        )
        
        for cache_key in sorted_keys[:evict_count]:
            self._remove_cache_entry(cache_key)
        
        logger.info(f"🗑️ Evicted {evict_count} old cache entries")
    
    def _remove_cache_entry(self, cache_key: str):
        """Remove a single cache entry"""
        if cache_key in self.cache:
            del self.cache[cache_key]
        if cache_key in self.access_times:
            del self.access_times[cache_key]
    
    def clear_all(self):
        """Clear entire cache"""
        self.cache.clear()
        self.access_times.clear()
        logger.info("🧹 Complete cache cleared")
    
    def clear_chat(self, chat_id):
        """Clear all cache entries for a specific chat"""
        keys_to_remove = [k for k in self.cache.keys() if k.startswith(f"{chat_id}:")]
        for key in keys_to_remove:
            self._remove_cache_entry(key)
        logger.info(f"🧹 Cleared cache for chat {chat_id} ({len(keys_to_remove)} entries)")
    
    def get_cache_stats(self) -> Dict:
        """Get detailed cache statistics"""
        total_files_cached = sum(len(entry['all_files']) for entry in self.cache.values())
        
        return {
            'cached_queries': len(self.cache),
            'max_queries': self.max_queries,
            'total_files_cached': total_files_cached,
            'cache_ttl_minutes': self.ttl.total_seconds() / 60,
            'memory_usage_estimate_mb': total_files_cached * 0.002  # Rough estimate
        }

# Initialize global cache
complete_cache = CompleteResultCache(ttl_minutes=15, max_queries=100)

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
        
        # Clear cache when new file is added to ensure fresh results
        complete_cache.clear_all()
        logger.info("Cache cleared due to new file addition")
        
    except DuplicateKeyError:
        LOGGER.error(f'{file_name} Is Already Saved In {"Secondary" if use_secondary else "Primary"} Database')
        return False, 0
    except ValidationError as e:
        LOGGER.error(f'Validation Error While Saving File: {e}')
        return False, 2
    else:
        LOGGER.info(f'{file_name} Saved Successfully In {"Secondary" if use_secondary else "Primary"} Database')
        return True, 1

async def get_search_results(chat_id, query, file_type=None, max_results=10, offset=0, filter=False):
    """
    ENHANCED SEARCH WITH COMPLETE RESULT CACHING
    
    Flow:
    1. User searches "my demon"
    2. Check if complete results are cached
    3. If YES: Return page from cache (instant, no DB query)
    4. If NO: Fetch ALL matching results from DB, cache them, return first page
    5. Next page requests use cached data (instant)
    
    Example:
    - Search "my demon" -> Fetches all 50 files from DB, caches them, shows page 1
    - Click next page -> Gets page 2 from cache (instant, no DB query)
    - Click next page -> Gets page 3 from cache (instant, no DB query)
    """
    
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
        logger.info(f"Since max_results Is An Odd Number ({max_results}), Bot Will Use {max_results + 1} As max_results To Make It Even.")
        max_results += 1

    # STEP 1: Try to get page from cache (NO DB QUERY if cached)
    cached_page = complete_cache.get_page(chat_id, query, file_type, offset, max_results)
    
    if cached_page is not None:
        # SUCCESS! Return page from cache - super fast, no DB query
        logger.info(f"⚡ INSTANT page delivery from cache for '{query}'")
        return cached_page

    # STEP 2: Cache miss - fetch ALL results from database and cache them
    logger.info(f"🔍 Cache miss for '{query}' - Fetching ALL results from database...")
    
    regex = get_compiled_regex(query)
    if not regex:
        return [], '', 0

    # Build filter query
    if USE_CAPTION_FILTER:
        filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}
    else:
        filter_query = {'file_name': regex}
    
    if file_type:
        filter_query['file_type'] = file_type

    # Projection for faster data retrieval
    projection = {
        'file_id': 1,
        'file_ref': 1,
        'file_name': 1,
        'file_size': 1,
        'file_type': 1,
        'caption': 1,
        'mime_type': 1
    }

    # STEP 3: Fetch ALL matching results from database (not just current page)
    try:
        if MULTIPLE_DB:
            # Count total in both databases
            count_task1 = Media.count_documents(filter_query)
            count_task2 = Media2.count_documents(filter_query)
            
            counts = await asyncio.gather(count_task1, count_task2)
            total_count = counts[0] + counts[1]
            
            # Fetch ALL files from both databases
            logger.info(f"📚 Fetching {total_count} files from dual databases for '{query}'...")
            
            cursor1 = Media.find(filter_query, projection).sort('$natural', -1)
            cursor2 = Media2.find(filter_query, projection).sort('$natural', -1)
            
            files1 = await cursor1.to_list(length=counts[0]) if counts[0] > 0 else []
            files2 = await cursor2.to_list(length=counts[1]) if counts[1] > 0 else []
            
            all_files = files1 + files2
            
        else:
            # Single database
            total_count = await Media.count_documents(filter_query)
            
            logger.info(f"📚 Fetching {total_count} files from database for '{query}'...")
            
            cursor = Media.find(filter_query, projection).sort('$natural', -1)
            all_files = await cursor.to_list(length=total_count) if total_count > 0 else []
        
        # STEP 4: Cache ALL results for future pagination
        complete_cache.cache_complete_results(chat_id, query, file_type, all_files, total_count)
        
        # STEP 5: Return the requested page from the complete results
        page_files = all_files[offset:offset + max_results]
        next_offset = offset + len(page_files)
        if next_offset >= total_count:
            next_offset = ''
        
        logger.info(f"✅ Fetched and cached {total_count} results for '{query}', returning page with {len(page_files)} files")
        logger.info(f"💡 Next pagination will be instant (from cache)!")
        
        return page_files, next_offset, total_count
        
    except Exception as e:
        logger.error(f"❌ Error fetching results for '{query}': {e}")
        return [], '', 0

async def get_bad_files(query, file_type=None):
    """Get files for deletion/management - not cached"""
    regex = get_compiled_regex(query)
    if not regex:
        return [], 0
    
    if USE_CAPTION_FILTER:
        filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}
    else:
        filter_query = {'file_name': regex}
    
    if file_type:
        filter_query['file_type'] = file_type
    
    if MULTIPLE_DB:
        cursor1 = Media.find(filter_query).sort('$natural', -1)
        cursor2 = Media2.find(filter_query).sort('$natural', -1)
        
        count_task1 = Media.count_documents(filter_query)
        count_task2 = Media2.count_documents(filter_query)
        
        results = await asyncio.gather(
            count_task1,
            count_task2,
            cursor1.to_list(length=await count_task1),
            cursor2.to_list(length=await count_task2)
        )
        
        files = results[2] + results[3]
    else:
        count = await Media.count_documents(filter_query)
        cursor1 = Media.find(filter_query).sort('$natural', -1)
        files = await cursor1.to_list(length=count)
    
    total_results = len(files)
    return files, total_results

async def get_file_details(query):
    """Get details of a specific file by file_id"""
    filter_query = {'file_id': query}
    
    if MULTIPLE_DB:
        cursor1 = Media.find(filter_query)
        cursor2 = Media2.find(filter_query)
        
        results = await asyncio.gather(
            cursor1.to_list(length=1),
            cursor2.to_list(length=1)
        )
        
        filedetails = results[0] if results[0] else results[1]
    else:
        cursor = Media.find(filter_query)
        filedetails = await cursor.to_list(length=1)
    
    return filedetails

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
    try:
        projection = {'file_name': 1, 'caption': 1}
        
        if MULTIPLE_DB:
            db_size = await check_db_size(db, _db_stats_cache_primary)
            if db_size > DB_CHANGE_LIMIT:
                cursor = Media2.find({}, projection).sort("$natural", -1).limit(limit)
                files = await cursor.to_list(length=limit)
                return files
        
        cursor = Media.find({}, projection).sort("$natural", -1).limit(limit)
        files = await cursor.to_list(length=limit)
        return files
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

def clear_search_cache():
    """
    Clear entire search cache
    Use after: Bulk file uploads, database maintenance, major updates
    """
    complete_cache.clear_all()
    get_compiled_regex.cache_clear()
    logger.info("🧹 All search caches cleared successfully")

def clear_chat_cache(chat_id: int):
    """
    Clear cache for specific chat
    Use when: User wants fresh results, chat-specific issues
    """
    complete_cache.clear_chat(chat_id)
    logger.info(f"🧹 Cache cleared for chat: {chat_id}")

def get_cache_stats() -> Dict:
    """
    Get detailed cache statistics for monitoring
    
    Returns:
        Dict with cache metrics: queries cached, files cached, memory usage
    """
    stats = complete_cache.get_cache_stats()
    logger.info(f"📊 Cache Stats: {stats}")
    return stats

async def warmup_cache(popular_queries: List[str], chat_id: int = 0):
    """
    Pre-cache popular search queries for better user experience
    
    Args:
        popular_queries: List of common search terms (e.g., ["avengers", "spider man", "my demon"])
        chat_id: Chat ID to cache for (default: 0 for global)
    
    Usage:
        await warmup_cache(["my demon", "avengers", "game of thrones"])
    """
    logger.info(f"🔥 Warming up cache with {len(popular_queries)} popular queries...")
    
    for query in popular_queries:
        try:
            await get_search_results(chat_id, query, file_type=None, max_results=10, offset=0)
            logger.info(f"✅ Pre-cached: {query}")
        except Exception as e:
            logger.error(f"❌ Failed to pre-cache '{query}': {e}")
    
    stats = get_cache_stats()
    logger.info(f"🔥 Cache warmup complete! Stats: {stats}")

# ============================================================================
# MONITORING FUNCTION
# ============================================================================

def log_cache_performance():
    """Log current cache performance metrics"""
    stats = get_cache_stats()
    logger.info("=" * 60)
    logger.info("CACHE PERFORMANCE REPORT")
    logger.info("=" * 60)
    logger.info(f"Cached Queries: {stats['cached_queries']}/{stats['max_queries']}")
    logger.info(f"Total Files Cached: {stats['total_files_cached']}")
    logger.info(f"Estimated Memory: {stats['memory_usage_estimate_mb']:.2f} MB")
    logger.info(f"Cache TTL: {stats['cache_ttl_minutes']} minutes")
    logger.info("=" * 60)

# ============================================================================
# EXAMPLE USAGE GUIDE
# ============================================================================

"""
HOW THE COMPLETE CACHING WORKS:
================================

SCENARIO 1: User searches "my demon"
-------------------------------------
1. First Search (offset=0):
   - Bot checks cache for "my demon" → NOT FOUND
   - Bot queries MongoDB for ALL files matching "my demon"
   - Found 50 files total
   - Bot caches ALL 50 files in memory
   - Returns first 10 files (page 1)
   - Database queries: 1

2. User clicks Next Page (offset=10):
   - Bot checks cache for "my demon" → FOUND!
   - Bot gets files 10-20 from cached data
   - Returns page 2
   - Database queries: 0 (INSTANT!)

3. User clicks Next Page (offset=20):
   - Bot checks cache for "my demon" → FOUND!
   - Bot gets files 20-30 from cached data
   - Returns page 3
   - Database queries: 0 (INSTANT!)

4. User clicks Next Page (offset=30):
   - Bot checks cache for "my demon" → FOUND!
   - Bot gets files 30-40 from cached data
   - Returns page 4
   - Database queries: 0 (INSTANT!)

5. User clicks Next Page (offset=40):
   - Bot checks cache for "my demon" → FOUND!
   - Bot gets files 40-50 from cached data
   - Returns page 5 (last page)
   - Database queries: 0 (INSTANT!)

TOTAL DATABASE QUERIES: 1 (only first search)
PAGINATION SPEED: Instant (all from cache)


SCENARIO 2: Different user searches "my demon" in same chat
------------------------------------------------------------
- Bot checks cache → FOUND (from previous user)
- Returns page 1 from cache
- Database queries: 0 (INSTANT!)


SCENARIO 3: User searches "avengers"
-------------------------------------
- Bot checks cache for "avengers" → NOT FOUND
- Bot queries MongoDB for ALL "avengers" files
- Caches ALL results
- Returns page 1
- Next pages will be instant


CACHE MANAGEMENT:
-----------------
- Cache expires after 15 minutes (configurable)
- Holds up to 100 different search queries
- Automatically clears old entries when full
- Clears on new file uploads to ensure fresh data


CONFIGURATION:
--------------
To adjust cache settings, modify these parameters:

complete_cache = CompleteResultCache(
    ttl_minutes=15,      # How long to keep cached results
    max_queries=100      # Maximum different searches to cache
)


FUNCTIONS FOR ADMINS:
---------------------

1. Clear all cache:
   clear_search_cache()

2. Clear cache for specific chat:
   clear_chat_cache(chat_id=12345)

3. View cache statistics:
   stats = get_cache_stats()
   print(stats)

4. Pre-cache popular searches:
   await warmup_cache(["my demon", "avengers", "game of thrones"])

5. Monitor performance:
   log_cache_performance()


MEMORY CONSIDERATIONS:
----------------------
- Each file entry ≈ 2KB
- 1000 files ≈ 2MB memory
- 100 searches × 50 files avg = 5000 files ≈ 10MB
- Very efficient for most bots


BENEFITS:
---------
✅ First search: Normal speed (fetches from DB)
✅ All pagination: INSTANT (no DB queries)
✅ Reduces MongoDB load by 90%+
✅ Better user experience
✅ Handles multiple users efficiently
✅ Automatic cache management


WHEN CACHE IS CLEARED:
-----------------------
1. New file added to database (ensures fresh results)
2. Cache entry expires (after ttl_minutes)
3. Manual cache clear by admin
4. Cache full and entry is least recently used
"""
