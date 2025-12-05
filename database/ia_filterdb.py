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
# PERSISTENT IN-MEMORY CACHE SYSTEM
# ============================================================================

class PersistentMediaCache:
    """
    Persistent in-memory cache that loads ALL database content on startup
    and serves all search/pagination requests from memory (NO MongoDB queries)
    """
    
    def __init__(self):
        """Initialize persistent cache"""
        self.all_files = []  # Complete list of ALL files from both DBs
        self.total_count = 0
        self.is_loaded = False
        self.load_timestamp = None
        self.file_index_by_name = {}  # Fast lookup index
        self.file_index_by_caption = {}  # Caption search index
        self.file_index_by_type = defaultdict(list)  # Type-based index
        
    async def load_complete_database(self):
        """
        Load ALL data from MongoDB into memory on bot startup
        This runs ONCE when bot starts
        """
        try:
            logger.info("=" * 80)
            logger.info("🚀 INITIALIZING PERSISTENT CACHE - LOADING ALL DATABASE CONTENT")
            logger.info("=" * 80)
            
            start_time = datetime.utcnow()
            
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
                # Load from both databases
                logger.info("📂 Loading from PRIMARY database...")
                count1 = await Media.count_documents({})
                cursor1 = Media.find({}, projection).sort('$natural', -1)
                files1 = await cursor1.to_list(length=count1) if count1 > 0 else []
                logger.info(f"✅ Loaded {count1:,} files from PRIMARY database")
                
                logger.info("📂 Loading from SECONDARY database...")
                count2 = await Media2.count_documents({})
                cursor2 = Media2.find({}, projection).sort('$natural', -1)
                files2 = await cursor2.to_list(length=count2) if count2 > 0 else []
                logger.info(f"✅ Loaded {count2:,} files from SECONDARY database")
                
                self.all_files = files1 + files2
                self.total_count = count1 + count2
            else:
                # Load from single database
                logger.info("📂 Loading from database...")
                self.total_count = await Media.count_documents({})
                cursor = Media.find({}, projection).sort('$natural', -1)
                self.all_files = await cursor.to_list(length=self.total_count) if self.total_count > 0 else []
                logger.info(f"✅ Loaded {self.total_count:,} files from database")
            
            # Build search indexes
            logger.info("🔍 Building search indexes...")
            self._build_indexes()
            
            self.is_loaded = True
            self.load_timestamp = datetime.utcnow()
            
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("=" * 80)
            logger.info("✅ CACHE IMPORT SUCCESSFUL!")
            logger.info("=" * 80)
            logger.info(f"📊 Total Files Cached: {self.total_count:,}")
            logger.info(f"⚡ Load Time: {elapsed:.2f} seconds")
            logger.info(f"💾 Estimated Memory: {(self.total_count * 0.002):.2f} MB")
            logger.info(f"🕐 Cache Initialized At: {self.load_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info("🎯 All searches will now use in-memory cache (NO MongoDB queries)")
            logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error(f"❌ CACHE INITIALIZATION FAILED: {e}")
            logger.error("=" * 80)
            self.is_loaded = False
            return False
    
    def _build_indexes(self):
        """Build fast lookup indexes for searching"""
        logger.info("🔨 Building file name index...")
        for idx, file in enumerate(self.all_files):
            # File name index (lowercase for case-insensitive search)
            file_name = getattr(file, 'file_name', '').lower()
            if file_name:
                if file_name not in self.file_index_by_name:
                    self.file_index_by_name[file_name] = []
                self.file_index_by_name[file_name].append(idx)
            
            # Caption index
            caption = getattr(file, 'caption', '')
            if caption:
                caption_lower = caption.lower()
                if caption_lower not in self.file_index_by_caption:
                    self.file_index_by_caption[caption_lower] = []
                self.file_index_by_caption[caption_lower].append(idx)
            
            # File type index
            file_type = getattr(file, 'file_type', None)
            if file_type:
                self.file_index_by_type[file_type].append(idx)
        
        logger.info(f"✅ Indexed {len(self.file_index_by_name)} unique file names")
        logger.info(f"✅ Indexed {len(self.file_index_by_caption)} unique captions")
        logger.info(f"✅ Indexed {len(self.file_index_by_type)} file types")
    
    def search_in_cache(self, query: str, file_type: Optional[str] = None) -> List:
        """
        Search cached files (NO MongoDB query)
        
        Args:
            query: Search query
            file_type: Optional file type filter
            
        Returns:
            List of matching file indices
        """
        if not self.is_loaded:
            logger.warning("⚠️ Cache not loaded! Returning empty results.")
            return []
        
        # Compile regex for search
        regex = get_compiled_regex(query)
        if not regex:
            return []
        
        matching_indices = []
        
        # Search through all files
        for idx, file in enumerate(self.all_files):
            # File type filter
            if file_type:
                current_file_type = getattr(file, 'file_type', None)
                if current_file_type != file_type:
                    continue
            
            # Search in file name
            file_name = getattr(file, 'file_name', '')
            if regex.search(file_name):
                matching_indices.append(idx)
                continue
            
            # Search in caption if enabled
            if USE_CAPTION_FILTER:
                caption = getattr(file, 'caption', '')
                if caption and regex.search(caption):
                    matching_indices.append(idx)
        
        return matching_indices
    
    def get_page(self, matching_indices: List[int], offset: int, max_results: int) -> Tuple[List, str, int]:
        """
        Get a specific page from search results
        
        Args:
            matching_indices: List of matching file indices from search
            offset: Starting position
            max_results: Number of results per page
            
        Returns:
            Tuple of (page_files, next_offset, total_count)
        """
        total_count = len(matching_indices)
        
        # Extract indices for current page
        page_indices = matching_indices[offset:offset + max_results]
        
        # Get actual file objects
        page_files = [self.all_files[idx] for idx in page_indices]
        
        # Calculate next offset
        next_offset = offset + len(page_files)
        if next_offset >= total_count:
            next_offset = ''
        
        return (page_files, next_offset, total_count)
    
    def add_file_to_cache(self, file_data: dict):
        """
        Add newly saved file to cache (keeps cache in sync)
        
        Args:
            file_data: File document to add
        """
        if not self.is_loaded:
            return
        
        self.all_files.insert(0, file_data)  # Add to beginning (newest first)
        self.total_count += 1
        
        # Update indexes
        file_name = getattr(file_data, 'file_name', '').lower()
        if file_name:
            if file_name not in self.file_index_by_name:
                self.file_index_by_name[file_name] = []
            self.file_index_by_name[file_name].insert(0, 0)
        
        # Increment all other indices by 1
        for indices in self.file_index_by_name.values():
            for i in range(len(indices)):
                if indices[i] > 0:
                    indices[i] += 1
        
        logger.info(f"➕ Added new file to cache. Total: {self.total_count:,}")
    
    def get_cache_info(self) -> Dict:
        """Get cache status information"""
        return {
            'is_loaded': self.is_loaded,
            'total_files': self.total_count,
            'load_timestamp': self.load_timestamp.isoformat() if self.load_timestamp else None,
            'memory_estimate_mb': self.total_count * 0.002,
            'indexed_names': len(self.file_index_by_name),
            'indexed_captions': len(self.file_index_by_caption),
            'indexed_types': len(self.file_index_by_type)
        }

# Initialize global persistent cache
persistent_cache = PersistentMediaCache()

# ============================================================================
# BOT STARTUP INITIALIZATION
# ============================================================================

async def initialize_cache_on_startup():
    """
    MUST BE CALLED when bot starts!
    Add this to your bot's startup sequence:
    
    Example in main.py:
        from database import initialize_cache_on_startup
        
        @bot.on_message(...)
        async def start():
            await initialize_cache_on_startup()
            # rest of your code
    """
    global persistent_cache
    
    if not persistent_cache.is_loaded:
        success = await persistent_cache.load_complete_database()
        if not success:
            logger.error("❌ Failed to initialize cache! Bot will not function properly.")
            return False
    else:
        logger.info("✅ Cache already loaded, skipping re-initialization")
    
    return True

# ============================================================================
# OPTIMIZED REGEX CACHING
# ============================================================================

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

# ============================================================================
# DATABASE MODELS
# ============================================================================

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

# ============================================================================
# DATABASE UTILITY FUNCTIONS
# ============================================================================

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
    """Save file to MongoDB and update cache"""
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
        
        # Add to persistent cache
        persistent_cache.add_file_to_cache(file)
        
    except DuplicateKeyError:
        LOGGER.error(f'{file_name} Is Already Saved In {"Secondary" if use_secondary else "Primary"} Database')
        return False, 0
    except ValidationError as e:
        LOGGER.error(f'Validation Error While Saving File: {e}')
        return False, 2
    else:
        LOGGER.info(f'{file_name} Saved Successfully In {"Secondary" if use_secondary else "Primary"} Database')
        return True, 1

# ============================================================================
# MAIN SEARCH FUNCTION (USES PERSISTENT CACHE)
# ============================================================================

async def get_search_results(chat_id, query, file_type=None, max_results=10, offset=0, filter=False):
    """
    SEARCH USING PERSISTENT IN-MEMORY CACHE (NO MongoDB QUERIES)
    
    All searches happen in memory - super fast!
    
    Args:
        chat_id: Chat ID for settings
        query: Search query (e.g., "my demon")
        file_type: Optional file type filter
        max_results: Results per page
        offset: Pagination offset
        filter: Additional filter flag
        
    Returns:
        Tuple of (files, next_offset, total_count)
    """
    
    # Check if cache is loaded
    if not persistent_cache.is_loaded:
        logger.error("❌ Cache not loaded! Cannot perform search.")
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
        logger.info(f"Since max_results Is An Odd Number ({max_results}), Bot Will Use {max_results + 1} As max_results To Make It Even.")
        max_results += 1

    # Search in persistent cache (NO MongoDB query!)
    logger.info(f"🔍 Searching in cache for '{query}' (type: {file_type})")
    matching_indices = persistent_cache.search_in_cache(query, file_type)
    
    # Get paginated results
    page_files, next_offset, total_count = persistent_cache.get_page(
        matching_indices, offset, max_results
    )
    
    logger.info(f"✅ Found {total_count} results for '{query}', returning {len(page_files)} files (offset: {offset})")
    
    return page_files, next_offset, total_count

# ============================================================================
# OTHER DATABASE FUNCTIONS
# ============================================================================

async def get_bad_files(query, file_type=None):
    """Get files for deletion/management - uses cache"""
    if not persistent_cache.is_loaded:
        logger.error("❌ Cache not loaded!")
        return [], 0
    
    matching_indices = persistent_cache.search_in_cache(query, file_type)
    files = [persistent_cache.all_files[idx] for idx in matching_indices]
    
    return files, len(files)

async def get_file_details(query):
    """Get details of a specific file by file_id - uses MongoDB for precise lookup"""
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

# ============================================================================
# MOVIE/SERIES FUNCTIONS
# ============================================================================

async def siletxbotz_fetch_media(limit: int) -> List[dict]:
    """Fetch recent media from cache"""
    if not persistent_cache.is_loaded:
        return []
    
    # Return first N files from cache (already sorted by newest)
    return persistent_cache.all_files[:limit]

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

async def reload_cache():
    """
    Reload entire cache from database
    Use after: Bulk updates, database maintenance
    """
    logger.info("🔄 Reloading cache from database...")
    global persistent_cache
    persistent_cache = PersistentMediaCache()
    success = await persistent_cache.load_complete_database()
    return success

def get_cache_stats() -> Dict:
    """
    Get detailed cache statistics for monitoring
    
    Returns:
        Dict with cache metrics
    """
    stats = persistent_cache.get_cache_info()
    logger.info(f"📊 Cache Stats: {stats}")
    return stats

def log_cache_performance():
    """Log current cache performance metrics"""
    stats = get_cache_stats()
    logger.info("=" * 80)
    logger.info("CACHE PERFORMANCE REPORT")
    logger.info("=" * 80)
    logger.info(f"Cache Loaded: {stats['is_loaded']}")
    logger.info(f"Total Files Cached: {stats['total_files']:,}")
    logger.info(f"Indexed File Names: {stats['indexed_names']:,}")
    logger.info(f"Indexed Captions: {stats['indexed_captions']:,}")
    logger.info(f"Indexed Types: {stats['indexed_types']}")
    logger.info(f"Estimated Memory: {stats['memory_estimate_mb']:.2f} MB")
    if stats['load_timestamp']:
        logger.info(f"Loaded At: {stats['load_timestamp']}")
    logger.info("=" * 80)
