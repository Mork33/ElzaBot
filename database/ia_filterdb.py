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

# Enhanced Full Result Cache for pagination
class FullResultCache:
    def __init__(self, ttl_minutes=15, max_cache_size=100):
        """
        Cache complete search results for instant pagination
        
        Args:
            ttl_minutes: Time to live for cache entries (default: 15 minutes)
            max_cache_size: Maximum number of search queries to cache (default: 100)
        """
        self.cache = {}
        self.ttl = timedelta(minutes=ttl_minutes)
        self.max_cache_size = max_cache_size
        self.access_count = {}  # Track access frequency for smart eviction
        
    def _generate_key(self, chat_id, query: str, file_type: Optional[str]) -> str:
        """Generate a unique cache key"""
        return f"{chat_id}:{query.lower().strip()}:{file_type or 'all'}"
    
    def get(self, chat_id, query: str, file_type: Optional[str] = None) -> Optional[Dict]:
        """
        Get cached full results
        
        Returns:
            Dict with 'files' (list), 'total_results' (int), 'timestamp' (datetime)
            or None if not cached or expired
        """
        key = self._generate_key(chat_id, query, file_type)
        
        if key in self.cache:
            cached_data = self.cache[key]
            timestamp = cached_data['timestamp']
            
            # Check if cache is still valid
            if datetime.utcnow() - timestamp < self.ttl:
                # Update access count for LRU tracking
                self.access_count[key] = self.access_count.get(key, 0) + 1
                logger.info(f"Cache HIT for query: {query} (chat: {chat_id})")
                return cached_data
            else:
                # Cache expired, remove it
                logger.info(f"Cache EXPIRED for query: {query} (chat: {chat_id})")
                self._remove_entry(key)
        
        logger.info(f"Cache MISS for query: {query} (chat: {chat_id})")
        return None
    
    def set(self, chat_id, query: str, file_type: Optional[str], 
            files: List, total_results: int):
        """
        Cache complete search results
        
        Args:
            chat_id: Chat ID
            query: Search query
            file_type: File type filter
            files: Complete list of all files matching the query
            total_results: Total count of results
        """
        key = self._generate_key(chat_id, query, file_type)
        
        # Evict entries if cache is full
        if len(self.cache) >= self.max_cache_size:
            self._evict_lru()
        
        self.cache[key] = {
            'files': files,
            'total_results': total_results,
            'timestamp': datetime.utcnow(),
            'query': query,
            'file_type': file_type
        }
        self.access_count[key] = 1
        
        logger.info(f"Cached {total_results} results for query: {query} (chat: {chat_id})")
    
    def get_page(self, chat_id, query: str, file_type: Optional[str], 
                 offset: int, limit: int) -> Optional[Tuple[List, str, int]]:
        """
        Get a specific page from cached results
        
        Returns:
            Tuple of (files_page, next_offset, total_results) or None if not cached
        """
        cached_data = self.get(chat_id, query, file_type)
        
        if cached_data is None:
            return None
        
        all_files = cached_data['files']
        total_results = cached_data['total_results']
        
        # Calculate page slice
        start_idx = offset
        end_idx = offset + limit
        
        files_page = all_files[start_idx:end_idx]
        
        # Calculate next offset
        next_offset = end_idx if end_idx < total_results else ''
        
        logger.info(f"Serving page (offset: {offset}, limit: {limit}) from cache for query: {query}")
        
        return (files_page, next_offset, total_results)
    
    def _evict_lru(self):
        """Evict least recently/frequently used entries (10% of cache)"""
        if not self.cache:
            return
        
        # Calculate number of entries to evict
        evict_count = max(1, int(self.max_cache_size * 0.1))
        
        # Sort by access count (ascending) to remove least used
        sorted_keys = sorted(
            self.access_count.keys(),
            key=lambda k: self.access_count.get(k, 0)
        )
        
        for key in sorted_keys[:evict_count]:
            self._remove_entry(key)
        
        logger.info(f"Evicted {evict_count} entries from cache")
    
    def _remove_entry(self, key: str):
        """Remove a single cache entry"""
        if key in self.cache:
            del self.cache[key]
        if key in self.access_count:
            del self.access_count[key]
    
    def clear(self):
        """Clear all cache entries"""
        self.cache.clear()
        self.access_count.clear()
        logger.info("Full result cache cleared")
    
    def clear_chat(self, chat_id):
        """Clear cache entries for a specific chat"""
        keys_to_remove = [k for k in self.cache.keys() if k.startswith(f"{chat_id}:")]
        for key in keys_to_remove:
            self._remove_entry(key)
        logger.info(f"Cleared cache for chat: {chat_id}")
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            'total_entries': len(self.cache),
            'max_size': self.max_cache_size,
            'ttl_minutes': self.ttl.total_seconds() / 60,
            'total_files_cached': sum(len(v['files']) for v in self.cache.values())
        }

# Initialize the full result cache
# TTL: 15 minutes (adjustable based on your needs)
# Max cache size: 100 queries (adjustable based on memory)
full_result_cache = FullResultCache(ttl_minutes=15, max_cache_size=100)

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
        
        # Clear cache after new file is added to ensure fresh results
        full_result_cache.clear()
        
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
    Enhanced search with full result caching for instant pagination
    
    First search: Fetches all results and caches them
    Subsequent pages: Served instantly from cache
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

    # Try to get page from cache first
    cached_page = full_result_cache.get_page(chat_id, query, file_type, offset, max_results)
    if cached_page is not None:
        return cached_page

    # Cache miss - need to fetch all results from database
    logger.info(f"Fetching ALL results from database for query: {query}")
    
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

    # Fetch ALL results (no limit) to cache them
    if MULTIPLE_DB:
        # Parallel queries for both databases
        total_results_task1 = Media.count_documents(filter_query)
        total_results_task2 = Media2.count_documents(filter_query)
        
        # Fetch ALL results from both databases
        cursor1 = Media.find(filter_query, projection).sort('$natural', -1)
        cursor2 = Media2.find(filter_query, projection).sort('$natural', -1)
        
        # Execute all queries in parallel
        results = await asyncio.gather(
            total_results_task1,
            total_results_task2
        )
        
        total_results = results[0] + results[1]
        
        # Fetch all files
        files1 = await cursor1.to_list(length=results[0])
        files2 = await cursor2.to_list(length=results[1])
        
        all_files = files1 + files2
    else:
        total_results = await Media.count_documents(filter_query)
        cursor1 = Media.find(filter_query, projection).sort('$natural', -1)
        all_files = await cursor1.to_list(length=total_results)
    
    # Cache all results for future pagination
    full_result_cache.set(chat_id, query, file_type, all_files, total_results)
    
    # Return the requested page
    files_page = all_files[offset:offset + max_results]
    next_offset = offset + len(files_page)
    if next_offset >= total_results:
        next_offset = ''
    
    logger.info(f"Fetched and cached {total_results} results, returning page with {len(files_page)} files")
    
    return files_page, next_offset, total_results

async def get_bad_files(query, file_type=None):
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

# Utility functions for cache management
def clear_search_cache():
    """Clear the full result cache - useful after bulk updates"""
    full_result_cache.clear()
    get_compiled_regex.cache_clear()
    logger.info("All search caches cleared successfully")

def clear_chat_cache(chat_id):
    """Clear cache for a specific chat"""
    full_result_cache.clear_chat(chat_id)
    logger.info(f"Cache cleared for chat: {chat_id}")

def get_cache_stats() -> Dict:
    """Get cache statistics for monitoring"""
    return full_result_cache.get_stats()

async def cache_popular_searches(queries: List[Tuple[str, Optional[str]]], chat_id: int = 0):
    """
    Pre-cache popular searches for better performance
    
    Args:
        queries: List of (query, file_type) tuples to pre-cache
        chat_id: Chat ID to cache for (default: 0 for global)
    """
    logger.info(f"Pre-caching {len(queries)} popular searches...")
    
    for query, file_type in queries:
        try:
            # This will fetch all results and cache them
            await get_search_results(chat_id, query, file_type, max_results=10, offset=0)
            logger.info(f"Pre-cached query: {query}")
        except Exception as e:
            logger.error(f"Error pre-caching query '{query}': {e}")
    
    logger.info("Pre-caching complete!")
