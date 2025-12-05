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

# ==================== FULL DATABASE CACHE ====================
class DatabaseCache:
    """Cache entire database in memory for faster searches"""
    def __init__(self):
        self.media_cache = []  # List of all media documents
        self.is_loaded = False
        self.last_updated = None
        self.total_files = 0
        self.cache_lock = asyncio.Lock()
        
    async def initialize(self):
        """Load entire database into memory"""
        async with self.cache_lock:
            if self.is_loaded:
                LOGGER.info("Cache already loaded, skipping initialization")
                return
            
            LOGGER.info("Starting full database cache import...")
            start_time = datetime.utcnow()
            
            try:
                # Projection for optimal memory usage
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
                    # Load from both databases
                    LOGGER.info("Loading from primary database...")
                    cursor1 = Media.find({}, projection)
                    files1 = await cursor1.to_list(length=None)
                    
                    LOGGER.info("Loading from secondary database...")
                    cursor2 = Media2.find({}, projection)
                    files2 = await cursor2.to_list(length=None)
                    
                    self.media_cache = files1 + files2
                else:
                    # Load from single database
                    LOGGER.info("Loading from database...")
                    cursor = Media.find({}, projection)
                    self.media_cache = await cursor.to_list(length=None)
                
                self.total_files = len(self.media_cache)
                self.last_updated = datetime.utcnow()
                self.is_loaded = True
                
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                
                LOGGER.info("="*60)
                LOGGER.info("✓ CACHE IMPORTED SUCCESSFULLY")
                LOGGER.info(f"✓ Total Files Cached: {self.total_files:,}")
                LOGGER.info(f"✓ Cache Load Time: {elapsed:.2f} seconds")
                LOGGER.info(f"✓ Memory Usage: ~{self._estimate_memory_mb():.2f} MB")
                LOGGER.info("="*60)
                
            except Exception as e:
                LOGGER.error(f"❌ Failed to initialize cache: {e}")
                self.is_loaded = False
                raise
    
    def _estimate_memory_mb(self):
        """Estimate memory usage of cache"""
        if not self.media_cache:
            return 0
        # Rough estimate: ~500 bytes per document on average
        return (self.total_files * 500) / (1024 * 1024)
    
    async def add_to_cache(self, file_data: dict):
        """Add new file to cache"""
        async with self.cache_lock:
            self.media_cache.append(file_data)
            self.total_files += 1
            LOGGER.debug(f"Added file to cache. Total files: {self.total_files}")
    
    async def search(self, regex, file_type=None, use_caption=False, offset=0, limit=10):
        """Search through cached data"""
        if not self.is_loaded:
            LOGGER.warning("Cache not loaded yet, searching database directly")
            return None
        
        results = []
        
        # Search through cache
        for file in self.media_cache:
            # Check file type filter
            if file_type and file.get('file_type') != file_type:
                continue
            
            # Check file name match
            file_name = file.get('file_name', '')
            if regex.search(file_name):
                results.append(file)
                continue
            
            # Check caption match if enabled
            if use_caption:
                caption = file.get('caption', '')
                if caption and regex.search(caption):
                    results.append(file)
        
        total_results = len(results)
        
        # Apply pagination
        paginated_results = results[offset:offset + limit]
        
        next_offset = offset + len(paginated_results)
        if next_offset >= total_results:
            next_offset = ''
        
        return paginated_results, next_offset, total_results
    
    async def get_file_by_id(self, file_id: str):
        """Get file from cache by file_id"""
        if not self.is_loaded:
            return None
        
        for file in self.media_cache:
            if file.get('file_id') == file_id:
                return [file]
        return []
    
    async def refresh_cache(self):
        """Refresh the entire cache"""
        LOGGER.info("Refreshing database cache...")
        self.is_loaded = False
        self.media_cache.clear()
        await self.initialize()
    
    def get_stats(self):
        """Get cache statistics"""
        return {
            "is_loaded": self.is_loaded,
            "total_files": self.total_files,
            "last_updated": self.last_updated,
            "memory_mb": self._estimate_memory_mb()
        }

# Global cache instance
db_cache = DatabaseCache()

# Search result cache (for frequently searched queries)
class SearchCache:
    def __init__(self, ttl_minutes=5, max_size=1000):
        self.cache = {}
        self.ttl = timedelta(minutes=ttl_minutes)
        self.max_size = max_size
    
    def get(self, key: str) -> Optional[Tuple]:
        if key in self.cache:
            result, timestamp = self.cache[key]
            if datetime.utcnow() - timestamp < self.ttl:
                return result
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Tuple):
        # Keep cache size manageable
        if len(self.cache) >= self.max_size:
            # Remove oldest 10% of entries
            oldest_keys = sorted(
                self.cache.keys(), 
                key=lambda k: self.cache[k][1]
            )[:int(self.max_size * 0.1)]
            for k in oldest_keys:
                del self.cache[k]
        
        self.cache[key] = (value, datetime.utcnow())
    
    def clear(self):
        self.cache.clear()

search_cache = SearchCache(ttl_minutes=5, max_size=1000)

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
        # Optimized indexes for faster queries
        indexes = (
            '$file_name',  # Text index for search
            [('file_name', 1), ('file_type', 1)],  # Compound index
            [('caption', 1)],  # Caption search index
            [('file_type', 1)],  # File type filtering
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
        # Optimized indexes for faster queries
        indexes = (
            '$file_name',  # Text index for search
            [('file_name', 1), ('file_type', 1)],  # Compound index
            [('caption', 1)],  # Caption search index
            [('file_type', 1)],  # File type filtering
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
    
    # Optimized file name cleaning with pre-compiled patterns
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
        
        # Add to cache
        file_data = {
            'file_id': file_id,
            'file_ref': file_ref,
            'file_name': file_name,
            'file_size': media.file_size,
            'file_type': media.file_type,
            'mime_type': media.mime_type,
            'caption': media.caption.html if media.caption else None
        }
        await db_cache.add_to_cache(file_data)
        
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
    # Get settings
    if chat_id is not None:
        settings = await get_settings(int(chat_id))
        try:
            max_results = 10 if settings.get('max_btn') else int(MAX_B_TN)
        except KeyError:
            await save_group_settings(int(chat_id), 'max_btn', False)
            settings = await get_settings(int(chat_id))
            max_results = 10 if settings.get('max_btn') else int(MAX_B_TN)

    # Check cache first
    cache_key = f"{chat_id}:{query}:{file_type}:{max_results}:{offset}"
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return cached_result

    # Use cached compiled regex
    regex = get_compiled_regex(query)
    if not regex:
        return [], '', 0

    # Ensure max_results is even
    if max_results % 2 != 0:
        logger.info(f"Since max_results Is An Odd Number ({max_results}), Bot Will Use {max_results + 1} As max_results To Make It Even.")
        max_results += 1

    # Try to search from cache first
    cache_result = await db_cache.search(
        regex=regex,
        file_type=file_type,
        use_caption=USE_CAPTION_FILTER,
        offset=offset,
        limit=max_results
    )
    
    if cache_result is not None:
        # Cache hit - return results from memory
        files, next_offset, total_results = cache_result
        result = (files, next_offset, total_results)
        search_cache.set(cache_key, result)
        return result
    
    # Fallback to database search if cache not ready
    LOGGER.warning("Cache miss - falling back to database search")
    
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

    if MULTIPLE_DB:
        # Parallel queries for both databases
        total_results_task1 = Media.count_documents(filter_query)
        total_results_task2 = Media2.count_documents(filter_query)
        
        cursor1 = Media.find(filter_query, projection).sort('$natural', -1).skip(offset).limit(max_results)
        cursor2 = Media2.find(filter_query, projection).sort('$natural', -1).skip(offset).limit(max_results)
        
        # Execute all queries in parallel
        results = await asyncio.gather(
            total_results_task1,
            total_results_task2,
            cursor1.to_list(length=max_results),
            cursor2.to_list(length=max_results)
        )
        
        total_results = results[0] + results[1]
        files1 = results[2]
        files2 = results[3]
        
        # Merge results and limit to max_results
        files = (files1 + files2)[:max_results]
    else:
        total_results = await Media.count_documents(filter_query)
        cursor1 = Media.find(filter_query, projection).sort('$natural', -1).skip(offset).limit(max_results)
        files = await cursor1.to_list(length=max_results)
    
    next_offset = offset + len(files)
    if next_offset >= total_results:
        next_offset = ''
    
    result = (files, next_offset, total_results)
    
    # Cache the result
    search_cache.set(cache_key, result)
    
    return result

async def get_bad_files(query, file_type=None):
    # Use cached compiled regex
    regex = get_compiled_regex(query)
    if not regex:
        return [], 0
    
    # Try cache first
    cache_result = await db_cache.search(
        regex=regex,
        file_type=file_type,
        use_caption=USE_CAPTION_FILTER,
        offset=0,
        limit=999999  # Get all results
    )
    
    if cache_result is not None:
        files, _, total_results = cache_result
        return files, total_results
    
    # Fallback to database
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
    # Try cache first
    cached_file = await db_cache.get_file_by_id(query)
    if cached_file:
        return cached_file
    
    # Fallback to database
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
        # Use cache if available
        if db_cache.is_loaded:
            return db_cache.media_cache[:limit]
        
        # Fallback to database
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
            file_name = getattr(file, "file_name", "") if hasattr(file, "file_name") else file.get("file_name", "")
            caption = getattr(file, "caption", "") if hasattr(file, "caption") else file.get("caption", "")
            
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
            file_name = getattr(file, "file_name", "") if hasattr(file, "file_name") else file.get("file_name", "")
            caption = getattr(file, "caption", "") if hasattr(file, "caption") else file.get("caption", "")
            
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

# Utility functions
def clear_search_cache():
    """Clear the search cache - useful after bulk updates"""
    search_cache.clear()
    get_compiled_regex.cache_clear()
    logger.info("Search cache cleared successfully")

async def refresh_full_cache():
    """Refresh the full database cache"""
    await db_cache.refresh_cache()

def get_cache_stats():
    """Get cache statistics"""
    return db_cache.get_stats()

# Initialize cache on module import (call this from your main bot file)
async def initialize_cache():
    """Initialize the database cache - call this when bot starts"""
    await db_cache.initialize()
