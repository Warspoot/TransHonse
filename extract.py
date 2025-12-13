import sys
import os
import json
import logging
import argparse
import ctypes
import faulthandler
faulthandler.enable()
from datetime import datetime, timezone
from pathlib import Path, PurePath
from typing import Union, Optional
from functools import cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from ctypes import c_char_p, c_int, c_void_p, POINTER

# Third-party imports
import apsw
import UnityPy

# ==========================================
# CONSTANTS & CONFIG
# ==========================================
IS_WIN = os.name == "nt"
DLL_NAME = "sqlite3mc_x64.dll"

# Standard Game Paths
ENV_GAME_ROOT = os.environ.get("UMA_DATA_DIR")
if IS_WIN:
    if ENV_GAME_ROOT:
        GAME_ROOT = Path(ENV_GAME_ROOT)
    else:
        import winreg
        try:
            key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Valve\Steam")
            steam_path = winreg.QueryValueEx(key, "InstallPath")[0]
            winreg.CloseKey(key)
            steam_game_path = Path(steam_path, "steamapps", "common", "UmamusumePrettyDerby_Jpn", "UmamusumePrettyDerby_Jpn_Data", "Persistent")
            GAME_ROOT = steam_game_path if steam_game_path.exists() else None
        except OSError:
            GAME_ROOT = None
        
        if not GAME_ROOT:
            GAME_ROOT = Path(os.environ["LOCALAPPDATA"], "..", "LocalLow", "Cygames", "umamusume").resolve()
    
    GAME_ASSET_ROOT = GAME_ROOT.joinpath("dat")
    GAME_META_FILE = GAME_ROOT.joinpath("meta")
else:
    GAME_ROOT = Path(".").resolve()
    GAME_ASSET_ROOT = GAME_ROOT.joinpath("dat")
    GAME_META_FILE = GAME_ROOT.joinpath("meta")

SUPPORTED_TYPES = ["story", "home", "race", "lyrics", "preview", "ruby", "mdb"]
TARGET_TYPES = ["story", "home", "lyrics", "preview"]
TYPE_CONFIG = {
    "story": {
        "no_wrap": False,
        "pattern": lambda s: f"story/data/{s.group}/{s.id}/storytimeline%{s.idx}",
        "filename": lambda s, _: f"storytimeline_{s}.json",
    },
    "home": {
        "no_wrap": True,
        "pattern": lambda s: f"home/data/{s.set}/{s.group}/hometimeline_{s.set}_{s.group}_{s.id}{s.idx}%",
        "filename": lambda s, _: f"hometimeline_{s.set}_{s.group}_{s.id}{s.idx}.json",
    },
    "lyrics": {
        "no_wrap": False,
        "pattern": lambda s: f"live/musicscores/m{s.id}/m{s.id}_lyrics",
        "filename": lambda s, _: f"{s.id}.json",
    },
    "preview": {
        "no_wrap": False,
        "pattern": lambda s: f"outgame/announceevent/loguiasset/ast_announce_event_log_ui_asset_0{s.id}",
        "filename": lambda s, title: f"{s.id} ({title}).json" if title else f"{s.id}.json",
    },
}

# Keys
DB_KEY = "9c2bab97bcf8c0c4f1a9ea7881a213f6c9ebf9d8d4c6a8e43ce5a259bde7e9fd"
BUNDLE_BASE_KEY = "532b4631e4a7b9473e7cfb"
META_DECRYPT_KEY = "9C2BAB97BCF8C0C4F1A9EA7881A213F6C9EBF9D8D4C6A8E43CE5A259BDE7E9FD"

DB_OPEN_MODE = apsw.SQLITE_OPEN_URI | apsw.SQLITE_OPEN_READONLY

# ==========================================
# LOGGER
# ==========================================
_FORMATTER = logging.Formatter("[$levelname] $filename: $message", style="$")
_STDOUT_HANDLER = logging.StreamHandler(sys.stdout)
_STDOUT_HANDLER.setFormatter(_FORMATTER)
_LOGGER = logging.getLogger("UmaTL_Shared")
_LOGGER.setLevel(logging.DEBUG)
_LOGGER.addHandler(_STDOUT_HANDLER)

def log_setup(args):
    if getattr(args, "verbose", False):
        _STDOUT_HANDLER.setLevel(logging.INFO)
    elif getattr(args, "debug", False):
        _STDOUT_HANDLER.setLevel(logging.DEBUG)
    else:
        _STDOUT_HANDLER.setLevel(logging.WARNING)

# ==========================================
# UTILS
# ==========================================
def readJson(file: Union[str, PurePath]) -> Union[dict, list]:
    with open(file, "r", encoding="utf8") as f:
        return json.load(f)

def writeJson(file: Union[str, Path], data, indent=4):
    if not isinstance(file, Path):
        file = Path(file)
    file.parent.mkdir(parents=True, exist_ok=True)
    with open(file, "w", encoding="utf8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)

def sanitizeFilename(fn: str):
    delSet = {34, 42, 47, 58, 60, 62, 63, 92, 124}
    return "".join(c for c in fn if ord(c) > 31 and ord(c) not in delSet)

def isJson(f: str):
    return f.endswith(".json")

# ==========================================
# DECRYPTION (SQLite3MC)
# ==========================================
SQLITE_OK = 0
SQLITE_DONE = 101
SQLITE_OPEN_READWRITE = 0x00000002
SQLITE_OPEN_CREATE = 0x00000004

class SQLite3MC:
    def __init__(self, dll_path: str):
        self.lib = ctypes.CDLL(dll_path)
        self.sqlite3_open_v2 = self.lib.sqlite3_open_v2
        self.sqlite3_open_v2.argtypes = [c_char_p, POINTER(c_void_p), c_int, c_void_p]
        self.sqlite3_open_v2.restype = c_int
        self.sqlite3_close = self.lib.sqlite3_close
        self.sqlite3_close.argtypes = [c_void_p]
        self.sqlite3_close.restype = c_int
        self.sqlite3_errmsg = self.lib.sqlite3_errmsg
        self.sqlite3_errmsg.argtypes = [c_void_p]
        self.sqlite3_errmsg.restype = c_char_p
        self.sqlite3mc_config = self.lib.sqlite3mc_config
        self.sqlite3mc_config.argtypes = [c_void_p, c_char_p, c_int]
        self.sqlite3mc_config.restype = c_int
        self.sqlite3_key = self.lib.sqlite3_key
        self.sqlite3_key.argtypes = [c_void_p, c_void_p, c_int]
        self.sqlite3_key.restype = c_int
        self.sqlite3_backup_init = self.lib.sqlite3_backup_init
        self.sqlite3_backup_init.argtypes = [c_void_p, c_char_p, c_void_p, c_char_p]
        self.sqlite3_backup_init.restype = c_void_p
        self.sqlite3_backup_step = self.lib.sqlite3_backup_step
        self.sqlite3_backup_step.argtypes = [c_void_p, c_int]
        self.sqlite3_backup_step.restype = c_int
        self.sqlite3_backup_finish = self.lib.sqlite3_backup_finish
        self.sqlite3_backup_finish.argtypes = [c_void_p]
        self.sqlite3_backup_finish.restype = c_int

    def errmsg(self, db: c_void_p) -> str:
        p = self.sqlite3_errmsg(db)
        return p.decode("utf-8", errors="replace") if p else ""

    def open(self, path: str) -> c_void_p:
        db = c_void_p()
        rc = self.sqlite3_open_v2(path.encode("utf-8"), ctypes.byref(db), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, None)
        if rc != SQLITE_OK or not db:
            raise RuntimeError(f"sqlite3_open_v2 failed rc={rc}")
        return db

    def close(self, db: c_void_p):
        if db:
            self.sqlite3_close(db)

    def mc_config(self, db: c_void_p, name: str, val: int) -> int:
        return self.sqlite3mc_config(db, name.encode("utf-8"), val)

    def key(self, db: c_void_p, key_bytes: bytes) -> int:
        buf = ctypes.create_string_buffer(key_bytes)
        return self.sqlite3_key(db, ctypes.cast(buf, c_void_p), len(key_bytes))

    def backup_to_file(self, src_db: c_void_p, dst_path: str, pages_per_step: int = 5) -> int:
        dst_db = c_void_p()
        rc = self.sqlite3_open_v2(dst_path.encode("utf-8"), ctypes.byref(dst_db), SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, None)
        if rc != SQLITE_OK or not dst_db:
            raise RuntimeError(f"open dst failed rc={rc}")
        try:
            backup = self.sqlite3_backup_init(dst_db, b"main", src_db, b"main")
            if not backup:
                return 1
            try:
                while True:
                    rc = self.sqlite3_backup_step(backup, pages_per_step)
                    if rc == SQLITE_DONE:
                        self.sqlite3_backup_finish(backup)
                        return SQLITE_OK
                    elif rc == SQLITE_OK:
                        continue
                    else:
                        self.sqlite3_backup_finish(backup)
                        return rc
            finally:
                pass
        finally:
            self.sqlite3_close(dst_db)

def hex_to_bytes(hex_str: str) -> bytes:
    hex_str = hex_str.strip()
    if hex_str.lower().startswith("0x"):
        hex_str = hex_str[2:]
    return bytes.fromhex(hex_str)

def decrypt_meta_file(src_path: Path, dst_path: Path):
    script_dir = Path(__file__).parent.resolve()
    dll_path = script_dir.joinpath(DLL_NAME)
    
    if not dll_path.exists():
        _LOGGER.error(f"Cannot decrypt DB: {DLL_NAME} not found in {script_dir}")
        return False

    try:
        api = SQLite3MC(str(dll_path))
        print(f"Attempting decryption of {src_path}...")
        
        db = api.open(str(src_path))
        try:
            api.mc_config(db, "cipher", 3)
            key_bytes = hex_to_bytes(META_DECRYPT_KEY)
            rc_key = api.key(db, key_bytes)
            if rc_key != SQLITE_OK:
                 raise RuntimeError(f"Key failed: {api.errmsg(db)}")

            rc_bk = api.backup_to_file(db, str(dst_path))
            if rc_bk != SQLITE_OK:
                raise RuntimeError(f"Backup/Decrypt failed: {rc_bk}")
                
            print(f"Decrypted database created at: {dst_path}")
            return True
        finally:
            api.close(db)
    except Exception as e:
        _LOGGER.error(f"Decryption error: {e}")
        return False

# ==========================================
# TYPES & BUNDLE HANDLING
# ==========================================
class StoryId:
    def __init__(self, type="story", set=None, group=None, id=None, idx=None):
        self.type = type
        self.set = set
        self.group = group
        self.id = id
        self.idx = idx
        
        if self.type in ("lyrics", "preview"):
            if not self.id and self.idx:
                self.id = self.idx
            self.idx = None
            self.group = None
            self.set = None

    def __str__(self) -> str:
        parts = [self.set, self.group, self.id, self.idx]
        return "".join(x for x in parts if x is not None)

    @classmethod
    def parse(cls, text_type, s):
        if text_type in ("lyrics", "preview"):
            return cls(type=text_type, id=s)
        elif len(s) > 9 and text_type == "home":
            return cls(type=text_type, set=s[:5], group=s[5:7], id=s[7:11], idx=s[11:])
        else:
            return cls(type=text_type, group=s[:2], id=s[2:6], idx=s[6:])

    @classmethod
    def parseFromPath(cls, text_type: str, path: str):
        if text_type == "home":
            path = path[-16:]
            return cls(type=text_type, set=path[:5], group=path[6:8], id=path[9:13], idx=path[13:])
        elif text_type == "lyrics":
            return cls(type=text_type, id=path[-11:-7])
        elif text_type == "preview":
            return cls(type=text_type, id=path[-4:])
        else:
            path = path[-9:]
            return cls(type=text_type, group=path[:2], id=path[2:6], idx=path[6:9])

    @classmethod
    def queryfy(cls, storyId):
        s = StoryId(storyId.type, storyId.set, storyId.group, storyId.id, storyId.idx)
        if s.set is None and s.type == 'home': s.set = "_____"
        if s.group is None and s.type != 'lyrics': s.group = "__"
        if s.id is None: s.id = "____"
        if s.idx is None and s.type not in ('lyrics', 'preview'): s.idx = "___"
        return s

    def asPath(self):
        parts = [x for x in [self.set, self.group, self.id] if x is not None]
        return Path().joinpath(*parts)

    def getFilenameIdx(self):
        if self.type in ("lyrics", "preview"):
            return self.id
        elif self.idx:
            return self.idx
        raise AttributeError("No Index available")

class GameBundle:
    @staticmethod
    def is_patched(path: Path) -> bool:
        try:
            with open(path, "rb") as f:
                f.seek(-2, os.SEEK_END)
                return f.read(2) == GameBundle.editMark
        except Exception:
            return False

    editMark = b"\x08\x04"

    def __init__(self, path, load=False, bType="story", bundle_key=0) -> None:
        self.bundlePath = Path(path)
        self.bundleName = self.bundlePath.stem
        self.bundleType = bType
        self.data = None
        self.bundle_key = bundle_key
        self._autoloaded = load
        self.exists = self.bundlePath.exists()

        if load and self.exists:
            self.load()

    def load(self):
        if not self.exists:
            return
        if self.bundle_key == 0:
            self.data = UnityPy.load(str(self.bundlePath))
        else:
            file_data = self.bundlePath.read_bytes()
            if len(file_data) > 256:
                file_data = self._decrypt(file_data)
            self.data = UnityPy.load(file_data)
        
        # Get first object
        self.rootAsset = None

        # UnityPy 2.0+ compatibility: objects is a list, not a dict
        objects = self.data.objects
        if isinstance(objects, dict):
            objects = objects.values()

        for obj in objects:
            try:
                if not hasattr(obj, "serialized_type"):
                    continue
                if not obj.serialized_type or not obj.serialized_type.nodes:
                    continue

                tree = obj.read_typetree()
                if "BlockList" in tree or "TextTrack" in tree:
                    self.rootAsset = obj
                    break
            except Exception:
                continue
        # Set assets map
        self.assets = self._resolve_assets()

    
    def _resolve_assets(self):
        root = self.rootAsset
        if not root:
            return {}
        return (
            getattr(getattr(root, "assets_file", None), "files", None)
            or getattr(getattr(root, "file", None), "files", {})
        )

    def _decrypt(self, data: bytes):
        final_key = self._create_final_key()
        decrypted_data = bytearray(data)
        for i in range(256, len(decrypted_data)):
            decrypted_data[i] ^= final_key[i % len(final_key)]
        return bytes(decrypted_data)

    def _create_final_key(self):
        base_key = bytes.fromhex(BUNDLE_BASE_KEY)
        bundle_key = self.bundle_key.to_bytes(8, byteorder="little", signed=True)
        base_len = len(base_key)
        final_key = bytearray(base_len * 8)
        for i, b in enumerate(base_key):
            baseOffset = i << 3
            for j, k in enumerate(bundle_key):
                final_key[baseOffset + j] = b ^ k
        return final_key

    @property
    def isPatched(self):
        try:
            with open(self.bundlePath, "rb") as f:
                f.seek(-2, os.SEEK_END)
                return f.read(2) == self.editMark
        except Exception:
            return False

    @classmethod
    def fromName(cls, name, **kwargs):
        bundlePath = PurePath(GAME_ASSET_ROOT, name[0:2], name)
        return cls(bundlePath, **kwargs)

    @staticmethod
    def createPath(root, name):
         return PurePath(root, name[0:2], name)

# ==========================================
# PATCH LOGIC
# ==========================================
class Args(argparse.ArgumentParser):
    def __init__(self, desc, **kwargs) -> None:
        super().__init__(description=desc, conflict_handler="resolve", **kwargs)
        self.add_argument("-t", "--type", choices=TARGET_TYPES, default="story", help="Asset type.")
        self.add_argument("-s", "--set", help="The set to process")
        self.add_argument("-g", "--group", help="The group to process")
        self.add_argument("-id", help="The id (subgroup) to process")
        self.add_argument("-idx", help="The specific asset index to process")
        self.add_argument("-sid", "--story", help="The storyid to process")
        self.add_argument("-dst", type=Path, default=Path("raw"), help="Output directory")
        self.add_argument("-O", "--overwrite", action="store_true", help="Overwrite existing files")
        self.add_argument("-w", "--workers", type=int, default=4, help="Parallel workers")
        self.add_argument("-meta", type=Path, default=None, help="Explicit path to decrypted meta")
        self.add_argument("-vb", "--verbose", action="store_true")
        self.add_argument("-dbg", "--debug", action="store_true")
        self.add_argument("-upd", "--update", action="store_true")
        self.add_argument("-upg", "--upgrade", action="store_true")
        self.add_argument("-nomtl", "--skip-mtl", action="store_true")

    def parse_args(self, *args, **kwargs):
        a = super().parse_args(*args, **kwargs)
        if a.story:
            s = StoryId.parse(a.type, a.story)
            a.set = a.set or s.set
            a.group = a.group or s.group
            a.id = a.id or s.id
            a.idx = a.idx or s.idx
        log_setup(a)
        return a

# ==========================================
# EXTRACTION LOGIC
# ==========================================
def queryDB(db, storyId: StoryId):
    cfg = TYPE_CONFIG.get(storyId.type)
    if not cfg:
        return []

    qid = StoryId.queryfy(storyId)
    pattern = cfg["pattern"](qid)

    return db.execute(
        "SELECT h, n, e FROM a WHERE n LIKE ?;",
        (pattern,),
    ).fetchall()


def extractText(assetType, obj):
    if assetType == "race":
        return {"jpText": obj["text"], "enText": ""}
    elif assetType == "lyrics":
        time, text, *_ = obj
        return {"jpText": text, "enText": "", "time": time}
    elif assetType == "preview":
        return {"jpName": obj["Name"], "jpText": obj["Text"]}
    elif obj.serialized_type.nodes:
        tree = obj.read_typetree()
        o = {
            "jpName": tree["Name"],
            "jpText": tree["Text"],
            "nextBlock": tree["NextBlock"],
        }
        if "ChoiceDataList" in tree and tree["ChoiceDataList"]:
            o["choices"] = [{"jpText": c["Text"]} for c in tree["ChoiceDataList"]]
        return o if o["jpText"] else None
    return None

def normalize_block(data):
    block = {
        "name": data.get("jpName", ""),
        "text": data.get("jpText", ""),
    }

    if "choices" in data:
        choices = [c["jpText"] for c in data["choices"] if c.get("jpText")]
        if choices:
            block["choice_data_list"] = choices

    return block


def extractAsset(asset: GameBundle, storyId: StoryId, current_args) -> Union[None, str]:
    asset.load()
    
    # Ensure we have a valid Reader
    if not hasattr(asset, "rootAsset") or not asset.rootAsset:
        return None
    
    # Check if Reader has serialized_type (it should!)
    if not hasattr(asset.rootAsset, "serialized_type") or not asset.rootAsset.serialized_type.nodes:
        return None

    tree = asset.rootAsset.read_typetree()
    
    export = {
        "title": tree.get("Title", ""),
        "no_wrap": TYPE_CONFIG[current_args.type]["no_wrap"],
        "text_block_list": []
    }

    if current_args.type in ("story", "home"):
        for block in tree["BlockList"]:
            for clip in block["TextTrack"]["ClipList"]:
                pathId = clip["m_PathID"]
                if pathId not in asset.assets:
                    continue
                
                # Fetch object reader safely
                obj = asset.assets[pathId]
                
                # IMPORTANT: For dependent objects (clips), we MIGHT need to read them if they are PPtrs
                # But extractText expects an ObjectReader to call read_typetree()
                # Newer UnityPy handles this mostly automatically.
                
                textData = extractText(current_args.type, obj)
                if not textData:
                    continue

                h_block = {
                    "name": textData.get("jpName", ""),
                    "text": textData.get("jpText", "")
                }
                
                if "choices" in textData:
                    choices = [c["jpText"] for c in textData["choices"] if c.get("jpText")]
                    if choices:
                        h_block["choice_data_list"] = choices
                
                export["text_block_list"].append(h_block)
    
    elif current_args.type == "preview":
         for block in tree["DataArray"]:
            textData = extractText("preview", block)
            if textData:
                export["text_block_list"].append({
                    "name": textData.get("jpName", ""),
                    "text": textData.get("jpText", "")
                })

    if not export["text_block_list"]:
        return None
    
    if current_args.type == "story":
        filename = f"storytimeline_{storyId}.json"
    elif current_args.type == "home":
        filename = f"hometimeline_{storyId.set}_{storyId.group}_{storyId.id}{storyId.idx}.json"
    else:
        title = sanitizeFilename(export.get("title", ""))
        idxString = f"{storyId.idx} ({title})" if title else storyId.getFilenameIdx()
        filename = f"{idxString}.json"
        
    return export, filename

def exportAsset(bundle_hash, unity_path, bundle_key, current_args):
    print(f"Processing: {bundle_hash}", flush=True)

    storyId = StoryId.parseFromPath(current_args.type, unity_path)

    # -------------------------
    # Resolve export directory
    # -------------------------
    exportDir = current_args.dst.joinpath(current_args.type)
    if current_args.type not in ("lyrics", "preview"):
        exportDir = exportDir.joinpath(storyId.asPath())

    # -------------------------
    # Skip if output exists
    # -------------------------
    if not current_args.overwrite:
        if current_args.type == "story":
            if exportDir.joinpath(f"storytimeline_{storyId}.json").exists():
                return None
        elif current_args.type == "home":
            if exportDir.joinpath(
                f"hometimeline_{storyId.set}_{storyId.group}_{storyId.id}{storyId.idx}.json"
            ).exists():
                return None
        else:
            if any(exportDir.glob(f"{storyId.getFilenameIdx()}*.json")):
                return None

    # -------------------------
    # Resolve bundle path EARLY
    # -------------------------
    bundle_path = Path(GameBundle.createPath(GAME_ASSET_ROOT, bundle_hash))

    if not bundle_path.exists():
        return None

    if GameBundle.is_patched(bundle_path):
        return None

    # -------------------------
    # Create bundle (NO LOAD)
    # -------------------------
    asset = GameBundle(bundle_path, load=False, bundle_key=bundle_key)

    # -------------------------
    # Extract
    # -------------------------
    try:
        result = extractAsset(asset, storyId, current_args)
        if not result:
            return None
        outData, filename = result
    except Exception as e:
        print(f"ERROR: Failed extracting {bundle_hash}: {e}", flush=True)
        return None

    # -------------------------
    # Write output
    # -------------------------
    exportDir.mkdir(parents=True, exist_ok=True)
    writeJson(exportDir.joinpath(filename), outData)
    return filename


def main():
    args = Args("Extract Game Assets to Hachimi-style JSON").parse_args()

    db_file = args.meta if args.meta else GAME_META_FILE
    
    if not db_file or not db_file.exists():
        print(f"Error: Meta file not found at {db_file}")
        print("Please check your UMA_DATA_DIR env var or use -meta <path>")
        return

    db = None
    try:
        db = apsw.Connection(f"file:{str(db_file)}?hexkey={DB_KEY}", DB_OPEN_MODE)
        db.cursor().execute("SELECT 1 FROM a LIMIT 1") 
        print(f"Connected to DB: {db_file}")
        
    except (apsw.NotADBError, apsw.CantOpenError, apsw.SQLError):
        print("Database appears encrypted/unreadable. Attempting decryption...")
        script_dir = Path(__file__).parent.resolve()
        decrypted_db_path = script_dir.joinpath("meta_decrypted.sqlite")
        
        if decrypt_meta_file(db_file, decrypted_db_path):
             try:
                db = apsw.Connection(f"file:{str(decrypted_db_path)}", DB_OPEN_MODE)
                print(f"Connected to Decrypted DB: {decrypted_db_path}")
             except Exception as e:
                 print(f"Failed to open decrypted DB: {e}")
                 return
        else:
             print("Could not decrypt database. Ensure sqlite3mc_x64.dll is present.")
             return

    try:
        print(f"Querying database for type: {args.type}...")
        q = queryDB(db, StoryId(args.type, args.set, args.group, args.id, args.idx))
        total = len(q)
        print(f"Found {total} assets. Starting extraction...")

        if q:
            sample_hash = q[0][0]
            sample_path = GameBundle.createPath(GAME_ASSET_ROOT, sample_hash)
            print(f"DEBUG: Performing Pre-flight check on sample asset.")
            print(f"DEBUG: Looking for: {sample_path}")
            
            if not Path(sample_path).exists():
                 print("\n" + "="*60)
                 print("!!! CRITICAL ERROR: ASSET NOT FOUND !!!")
                 print(f"Failed to find: {sample_path}")
                 print("="*60 + "\n")
                 return
            else:
                print("DEBUG: Pre-flight check passed. Assets found.")

        workers = max(1, args.workers)
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for bundle, path, key in q:
                futures.append(executor.submit(exportAsset, bundle, path, key, args))
            
            success = 0
            skipped = 0
            processed = 0
            for future in as_completed(futures):
                processed += 1
                if processed % 100 == 0:
                    print(f"Progress: {processed}/{total}", flush=True)

                res = future.result()
                if res:
                    success += 1
                    print(f"Extracted: {res}", flush=True)
                else:
                    skipped += 1

        
        print(f"Done. Extracted: {success}, Skipped/Failed: {skipped}")

    finally:
        if db:
            db.close()

if __name__ == "__main__":
    main()