from fastapi import FastAPI, Request, File, UploadFile, HTTPException, BackgroundTasks, Form
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from contextlib import asynccontextmanager
from jinja2 import TemplateNotFound
import uvicorn
import os
import shutil
import asyncio
import base64
import struct
import json
import subprocess
import re
import threading
import time
import uuid
import traceback
from mutagen import File as MutagenFile
from mutagen.id3 import APIC
import numpy as np
try:
    import librosa
except Exception:
    librosa = None
try:
    import soundfile as sf
except Exception:
    sf = None
import tempfile
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote

try:
    from audio_analyzer import analyze_directory, sort_playlist
    AUDIO_ANALYZER_IMPORT_ERROR = ""
except Exception as e:
    analyze_directory = None
    sort_playlist = None
    AUDIO_ANALYZER_IMPORT_ERROR = str(e)
try:
    from Crypto.Cipher import AES
except Exception:
    AES = None
try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except Exception:
    Cipher = None
    algorithms = None
    modes = None

IS_VERCEL = os.environ.get("VERCEL") == "1"
BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = Path("/tmp") if IS_VERCEL else BASE_DIR
MUSIC_DIR = RUNTIME_DIR / "music_files"
CACHE_FILE = RUNTIME_DIR / "analysis_cache.json"
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "templates"

@asynccontextmanager
async def lifespan(app: FastAPI):
    _reset_runtime_playlist_state(clear_files=True, clear_cache=True)
    yield

app = FastAPI(lifespan=lifespan)

os.makedirs(MUSIC_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR), check_dir=False), name="static")
app.mount("/music_files", StaticFiles(directory=str(MUSIC_DIR), check_dir=False), name="music_files")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

def _template_response(request: Request, template_name: str, context: dict):
    try:
        return templates.TemplateResponse(request=request, name=template_name, context=context)
    except TypeError:
        return templates.TemplateResponse(template_name, context)

# Globals to hold current state
current_songs = []
sorted_songs = []
actual_curve = []
ideal_curve = []
playlist_scope_filenames: set[str] = set()
NCM_CORE_KEY = bytes.fromhex("687A4852416D736F356B496E62617857")
NCM_META_KEY = bytes.fromhex("2331346C6A6B5F215C5D2630553C2728")
AUDIO_EXTENSIONS = {".mp3", ".wav", ".flac", ".m4a", ".aac", ".ogg"}
SUFFIXED_STEM_PATTERN = re.compile(r"^(?P<base>.+)_(?P<index>\d+)$")

class AudioExportRequest(BaseModel):
    filenames: list[str]
    output_format: str = "wav"
    quality: str = "high"
    automix: bool = True

export_jobs: dict[str, dict] = {}
export_jobs_lock = threading.Lock()

def _ensure_analysis_backend() -> None:
    if analyze_directory is None or sort_playlist is None:
        detail = AUDIO_ANALYZER_IMPORT_ERROR or "audio_analyzer import failed"
        raise HTTPException(status_code=503, detail=f"Audio analysis backend unavailable: {detail}")

def _pkcs7_unpad(data: bytes) -> bytes:
    if not data:
        return data
    pad = data[-1]
    if pad <= 0 or pad > 16:
        return data
    return data[:-pad]

def _aes_ecb_decrypt(key: bytes, data: bytes) -> bytes:
    if AES is not None:
        cipher = AES.new(key, AES.MODE_ECB)
        return _pkcs7_unpad(cipher.decrypt(data))
    if Cipher is not None and algorithms is not None and modes is not None:
        decryptor = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
        decrypted = decryptor.update(data) + decryptor.finalize()
        return _pkcs7_unpad(decrypted)
    if os.name == "nt":
        key_b64 = base64.b64encode(key).decode("ascii")
        data_b64 = base64.b64encode(data).decode("ascii")
        ps_script = (
            f"$k=[Convert]::FromBase64String('{key_b64}');"
            f"$d=[Convert]::FromBase64String('{data_b64}');"
            "$aes=[System.Security.Cryptography.Aes]::Create();"
            "$aes.Mode='ECB';"
            "$aes.Padding='None';"
            "$aes.Key=$k;"
            "$aes.IV=(New-Object byte[] 16);"
            "$dec=$aes.CreateDecryptor().TransformFinalBlock($d,0,$d.Length);"
            "[Console]::Out.Write([Convert]::ToBase64String($dec));"
        )
        try:
            completed = subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_script],
                capture_output=True,
                text=True,
                check=True,
            )
            decrypted = base64.b64decode(completed.stdout.strip())
            return _pkcs7_unpad(decrypted)
        except Exception:
            pass
    raise RuntimeError("AES backend not available for NCM conversion")

def _build_ncm_key_box(key_data: bytes) -> list[int]:
    key_box = list(range(256))
    last_byte = 0
    key_offset = 0
    key_len = len(key_data)
    for i in range(256):
        swap = key_box[i]
        c = (swap + last_byte + key_data[key_offset]) & 0xFF
        key_offset = (key_offset + 1) % key_len
        key_box[i] = key_box[c]
        key_box[c] = swap
        last_byte = c
    return key_box

def _guess_ncm_output_ext(meta_payload: bytes) -> str:
    default_ext = ".mp3"
    if not meta_payload:
        return default_ext
    try:
        raw = bytes(b ^ 0x63 for b in meta_payload)
        meta_encoded = raw[22:]
        meta_decrypted = _aes_ecb_decrypt(NCM_META_KEY, base64.b64decode(meta_encoded))
        meta_json = json.loads(meta_decrypted[6:].decode("utf-8", errors="ignore"))
        fmt = meta_json.get("format")
        if isinstance(fmt, str) and fmt.strip():
            return f".{fmt.strip().lower()}"
    except Exception:
        return default_ext
    return default_ext

def _guess_image_extension(image_data: bytes) -> str:
    if len(image_data) >= 3 and image_data[:3] == b"\xFF\xD8\xFF":
        return ".jpg"
    if len(image_data) >= 8 and image_data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if len(image_data) >= 6 and (image_data[:6] == b"GIF87a" or image_data[:6] == b"GIF89a"):
        return ".gif"
    if len(image_data) >= 2 and image_data[:2] == b"BM":
        return ".bmp"
    if len(image_data) >= 12 and image_data[:4] == b"RIFF" and image_data[8:12] == b"WEBP":
        return ".webp"
    return ".jpg"

def _cover_candidate_paths(music_dir: Path, stem: str) -> list[Path]:
    return [music_dir / f"{stem}.cover{ext}" for ext in (".jpg", ".png", ".webp", ".gif", ".bmp")]

def _find_existing_cover_path(music_dir: Path, stem: str) -> Path | None:
    for path in _cover_candidate_paths(music_dir, stem):
        if path.exists():
            return path
    return None

def _decrypt_ncm_file(input_path: str, output_dir: str) -> str:
    with open(input_path, "rb") as src:
        header = src.read(8)
        if header != b"CTENFDAM":
            raise ValueError("invalid ncm header")
        src.read(2)
        key_len_bytes = src.read(4)
        if len(key_len_bytes) != 4:
            raise ValueError("invalid ncm key section")
        key_len = struct.unpack("<I", key_len_bytes)[0]
        key_data = bytearray(src.read(key_len))
        for i in range(len(key_data)):
            key_data[i] ^= 0x64
        key_data = _aes_ecb_decrypt(NCM_CORE_KEY, bytes(key_data))[17:]
        key_box = _build_ncm_key_box(key_data)

        meta_len = struct.unpack("<I", src.read(4))[0]
        meta_payload = src.read(meta_len) if meta_len > 0 else b""
        output_ext = _guess_ncm_output_ext(meta_payload)

        src.read(4)
        src.read(5)
        image_size = struct.unpack("<I", src.read(4))[0]
        image_data = b""
        if image_size > 0:
            image_data = src.read(image_size)

        stem = Path(input_path).stem
        output_name = f"{stem}{output_ext}"
        output_path = os.path.join(output_dir, output_name)

        # Save image data if present
        if image_data:
            cover_ext = _guess_image_extension(image_data)
            cover_path = os.path.join(output_dir, f"{Path(output_name).stem}.cover{cover_ext}")
            try:
                with open(cover_path, "wb") as img_file:
                    img_file.write(image_data)
            except Exception:
                pass

        with open(output_path, "wb") as dst:
            stream256 = np.array(
                [key_box[(key_box[j] + key_box[(key_box[j] + j) & 0xFF]) & 0xFF] for j in range(256)],
                dtype=np.uint8
            )
            global_index = 0
            while True:
                chunk = src.read(131072)
                if not chunk:
                    break
                chunk_arr = np.frombuffer(chunk, dtype=np.uint8)
                positions = (np.arange(chunk_arr.shape[0], dtype=np.uint32) + global_index + 1) & 0xFF
                xor_mask = stream256[positions]
                decrypted = np.bitwise_xor(chunk_arr, xor_mask)
                dst.write(decrypted.tobytes())
                global_index += chunk_arr.shape[0]

    return output_name

def _get_audio_duration_seconds(file_path: str) -> float:
    try:
        audio = MutagenFile(file_path)
        if audio and getattr(audio, "info", None) and getattr(audio.info, "length", None):
            return float(audio.info.length)
    except Exception:
        return 0.0
    return 0.0

def _is_same_audio_file(left_path: str, right_path: str) -> bool:
    if not (os.path.exists(left_path) and os.path.exists(right_path)):
        return False
    left_size = os.path.getsize(left_path)
    right_size = os.path.getsize(right_path)
    if left_size <= 0 or right_size <= 0:
        return False
    if left_size != right_size:
        return False
    left_duration = _get_audio_duration_seconds(left_path)
    right_duration = _get_audio_duration_seconds(right_path)
    if left_duration > 0 and right_duration > 0 and abs(left_duration - right_duration) > 0.02:
        return False
    return True

def _cleanup_suffixed_duplicates(music_dir: str) -> list[str]:
    existing = [name for name in os.listdir(music_dir) if os.path.isfile(os.path.join(music_dir, name))]
    normalized_map = {name.casefold(): name for name in existing}
    to_remove: list[str] = []
    for name in existing:
        ext = Path(name).suffix.lower()
        if ext not in AUDIO_EXTENSIONS:
            continue
        stem = Path(name).stem
        match = SUFFIXED_STEM_PATTERN.match(stem)
        if not match:
            continue
        base_name = f"{match.group('base')}{ext}"
        base_actual = normalized_map.get(base_name.casefold())
        if not base_actual:
            continue
        duplicate_path = os.path.join(music_dir, name)
        base_path = os.path.join(music_dir, base_actual)
        if _is_same_audio_file(base_path, duplicate_path):
            to_remove.append(name)
    if not to_remove:
        return []
    for name in to_remove:
        file_path = os.path.join(music_dir, name)
        if os.path.exists(file_path):
            os.remove(file_path)
        cover_path = os.path.join(music_dir, f"{Path(name).stem}.cover.jpg")
        if os.path.exists(cover_path):
            os.remove(cover_path)
    cache_file = CACHE_FILE
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
            changed = False
            for removed_name in to_remove:
                if removed_name in cache:
                    del cache[removed_name]
                    changed = True
            if changed:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(cache, f, indent=4)
        except Exception:
            pass
    return to_remove

def _list_audio_filenames(music_dir: str) -> list[str]:
    if not os.path.exists(music_dir):
        return []
    names: list[str] = []
    for name in os.listdir(music_dir):
        file_path = os.path.join(music_dir, name)
        if not os.path.isfile(file_path):
            continue
        if Path(name).suffix.lower() in AUDIO_EXTENSIONS:
            names.append(name)
    names.sort(key=lambda x: x.lower())
    return names

def _normalize_filename_set(filenames: list[str] | set[str] | tuple[str, ...] | None) -> set[str]:
    normalized: set[str] = set()
    if not filenames:
        return normalized
    for name in filenames:
        safe_name = os.path.basename(str(name or "")).strip()
        if safe_name:
            normalized.add(safe_name)
    return normalized

def _set_playlist_scope(filenames: list[str] | set[str] | tuple[str, ...] | None) -> set[str]:
    global playlist_scope_filenames
    playlist_scope_filenames = _normalize_filename_set(filenames)
    return set(playlist_scope_filenames)

def _get_effective_playlist_scope() -> set[str]:
    live_files = _list_audio_filenames(MUSIC_DIR)
    if not playlist_scope_filenames:
        return set(live_files)
    live_map = {name.casefold(): name for name in live_files}
    return {live_map[key] for key in {name.casefold() for name in playlist_scope_filenames} if key in live_map}

def _filter_song_items_to_scope(items: list[dict], allowed_filenames: set[str]) -> list[dict]:
    allowed_casefold = {name.casefold() for name in allowed_filenames}
    filtered: list[dict] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        filename = str(item.get("filename") or "").strip()
        if filename and filename.casefold() in allowed_casefold:
            filtered.append(item)
    return filtered

def _sync_playlist_state_with_scope() -> set[str]:
    global current_songs, sorted_songs, actual_curve, ideal_curve
    effective_scope = _get_effective_playlist_scope()
    _set_playlist_scope(effective_scope)
    current_songs = _filter_song_items_to_scope(current_songs, effective_scope)
    sorted_songs = _filter_song_items_to_scope(sorted_songs, effective_scope)
    current_names = {str(item.get("filename") or "").casefold() for item in current_songs if isinstance(item, dict)}
    sorted_names = {str(item.get("filename") or "").casefold() for item in sorted_songs if isinstance(item, dict)}
    if not current_songs:
        sorted_songs = []
        actual_curve = []
        ideal_curve = []
    elif current_names != sorted_names or len(actual_curve) != len(sorted_songs) or len(ideal_curve) != len(sorted_songs):
        if sort_playlist is not None:
            try:
                sorted_songs, actual_curve, ideal_curve = sort_playlist(current_songs)
            except Exception as e:
                print(f"sort_playlist fallback due to error: {e}")
                sorted_songs = list(current_songs)
                actual_curve = []
                ideal_curve = []
        else:
            sorted_songs = list(current_songs)
            actual_curve = []
            ideal_curve = []
    return effective_scope

def _reset_runtime_playlist_state(clear_files: bool = False, clear_cache: bool = False) -> None:
    global current_songs, sorted_songs, actual_curve, ideal_curve
    music_dir = MUSIC_DIR
    if clear_files and os.path.exists(music_dir):
        shutil.rmtree(music_dir, ignore_errors=True)
    os.makedirs(music_dir, exist_ok=True)
    if clear_cache:
        cache_file = CACHE_FILE
        if os.path.exists(cache_file):
            try:
                os.remove(cache_file)
            except Exception:
                pass
    current_songs = []
    sorted_songs = []
    actual_curve = []
    ideal_curve = []
    _set_playlist_scope(set())

def _rebuild_playlist_state_from_scope(scope_filenames: list[str] | None = None):
    global current_songs, sorted_songs, actual_curve, ideal_curve
    _ensure_analysis_backend()
    include_scope = scope_filenames if scope_filenames is not None else None
    _set_playlist_scope(include_scope if include_scope is not None else _list_audio_filenames(MUSIC_DIR))
    analyzed = analyze_directory(str(MUSIC_DIR), cache_file=str(CACHE_FILE), force_reanalyze=False, include_filenames=include_scope)
    current_songs = analyzed
    if current_songs:
        sorted_songs, actual_curve, ideal_curve = sort_playlist(current_songs)
    else:
        sorted_songs = []
        actual_curve = []
        ideal_curve = []
    _sync_playlist_state_with_scope()

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the main UI page."""
    try:
        _sync_playlist_state_with_scope()
        return _template_response(request, "index.html", {
            "request": request,
            "songs": current_songs,
            "sorted": sorted_songs,
            "actual_curve": actual_curve,
            "ideal_curve": ideal_curve,
            "sorted_json": _json_literal(sorted_songs, "[]"),
            "actual_curve_json": _json_literal(actual_curve, "[]"),
            "ideal_curve_json": _json_literal(ideal_curve, "[]"),
            "is_vercel": IS_VERCEL
        })
    except TemplateNotFound:
        detail = f"Missing template: {TEMPLATES_DIR / 'index.html'}"
        return HTMLResponse(detail, status_code=500)
    except Exception as e:
        detail = f"Homepage render failed: {e}\n{traceback.format_exc()}"
        return HTMLResponse(f"<pre>{detail}</pre>", status_code=500)

@app.get("/algorithm", response_class=HTMLResponse)
async def read_algorithm(request: Request, embedded: int = 0):
    """Serve the algorithm explanation page."""
    try:
        return _template_response(request, "algorithm.html", {
            "request": request,
            "embedded": bool(embedded)
        })
    except TemplateNotFound:
        detail = f"Missing template: {TEMPLATES_DIR / 'algorithm.html'}"
        return HTMLResponse(detail, status_code=500)
    except Exception as e:
        detail = f"Algorithm page render failed: {e}\n{traceback.format_exc()}"
        return HTMLResponse(f"<pre>{detail}</pre>", status_code=500)

@app.post("/analyze")
async def analyze_music(request: Request):
    """Trigger the analysis of the 'music_files' directory."""
    global current_songs, sorted_songs, actual_curve, ideal_curve
    _ensure_analysis_backend()
    
    print(f"Starting analysis of '{MUSIC_DIR}' directory...")
    requested_filenames: list[str] = []
    has_filename_scope = False
    force_reanalyze = False
    try:
        payload = await request.json()
        if isinstance(payload, dict):
            raw_filenames = payload.get("filenames")
            if isinstance(raw_filenames, list):
                has_filename_scope = True
                requested_filenames = [str(name).strip() for name in raw_filenames if str(name).strip()]
            force_reanalyze = bool(payload.get("force_reanalyze", False))
    except Exception:
        requested_filenames = []
        force_reanalyze = False
    _cleanup_suffixed_duplicates(MUSIC_DIR)
    include_scope = requested_filenames if has_filename_scope else None
    _set_playlist_scope(include_scope if include_scope is not None else _list_audio_filenames(MUSIC_DIR))
    current_songs = analyze_directory(str(MUSIC_DIR), cache_file=str(CACHE_FILE), force_reanalyze=force_reanalyze, include_filenames=include_scope)
    
    # 排序
    if current_songs:
        sorted_songs, actual_curve, ideal_curve = sort_playlist(current_songs)
    else:
        sorted_songs = []
        actual_curve = []
        ideal_curve = []
    _sync_playlist_state_with_scope()
    
    return {
        "message": "Analysis complete", 
        "count": len(current_songs),
        "sorted": sorted_songs,
        "actual_curve": actual_curve,
        "ideal_curve": ideal_curve
    }

@app.post("/upload")
async def upload_files(
    files: list[UploadFile] = File(...),
    append: bool = Form(True)
):
    """Upload audio files to the server. Can append or replace existing playlist scope."""
    global current_songs, sorted_songs, actual_curve, ideal_curve
    _ensure_analysis_backend()
    music_dir = MUSIC_DIR
    existing_scope = _get_effective_playlist_scope()
    if not append:
        try:
            _reset_runtime_playlist_state(clear_files=True, clear_cache=True)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to reset playlist state: {str(e)}")

    # 4. Save new files
    saved_files = []
    saved_file_infos = []
    converted_ncm_files = []
    failed_ncm_files = []
    for file in files:
        if file.filename:
            file_path = os.path.join(music_dir, file.filename)
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            ext = os.path.splitext(file.filename)[1].lower()
            if ext == ".ncm":
                try:
                    converted_filename = _decrypt_ncm_file(file_path, music_dir)
                    os.remove(file_path)
                    saved_files.append(converted_filename)
                    converted_path = os.path.join(music_dir, converted_filename)
                    saved_file_infos.append({
                        "filename": converted_filename,
                        "file_size_bytes": os.path.getsize(converted_path) if os.path.exists(converted_path) else 0,
                        "duration_sec": 0,
                        "source_bitrate_kbps_est": 0
                    })
                    converted_ncm_files.append({
                        "source": file.filename,
                        "target": converted_filename
                    })
                except Exception as e:
                    failed_ncm_files.append({
                        "source": file.filename,
                        "error": str(e)
                    })
                    if os.path.exists(file_path):
                        os.remove(file_path)
            else:
                saved_files.append(file.filename)
                saved_file_infos.append({
                    "filename": file.filename,
                    "file_size_bytes": os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                    "duration_sec": 0,
                    "source_bitrate_kbps_est": 0
                })
    if not saved_files:
        first_error = failed_ncm_files[0]["error"] if failed_ncm_files else "no supported files"
        raise HTTPException(status_code=400, detail=f"All uploaded files failed conversion: {first_error}")
    removed_duplicates = _cleanup_suffixed_duplicates(music_dir)
    if removed_duplicates:
        removed_set = {name.casefold() for name in removed_duplicates}
        saved_files = [name for name in saved_files if name.casefold() not in removed_set]
        saved_file_infos = [item for item in saved_file_infos if str(item.get("filename", "")).casefold() not in removed_set]
    analysis_error = ""
    analyze_scope = None if append else [str(name) for name in saved_files if str(name).strip()]
    if append:
        _set_playlist_scope(existing_scope.union(_normalize_filename_set(saved_files)))
    else:
        _set_playlist_scope(saved_files)
    try:
        _rebuild_playlist_state_from_scope(analyze_scope)
    except Exception as e:
        analysis_error = str(e)
        print(f"Post-upload rebuild failed: {e}")
        uploaded_scope = [str(name) for name in saved_files if str(name).strip()]
        try:
            current_songs = analyze_directory(str(MUSIC_DIR), cache_file=str(CACHE_FILE), force_reanalyze=False, include_filenames=uploaded_scope)
            if current_songs:
                sorted_songs, actual_curve, ideal_curve = sort_playlist(current_songs)
            else:
                sorted_songs = []
                actual_curve = []
                ideal_curve = []
            _sync_playlist_state_with_scope()
        except Exception as scoped_error:
            print(f"Scoped fallback analysis failed: {scoped_error}")
            current_songs = []
            sorted_songs = []
            actual_curve = []
            ideal_curve = []
            _sync_playlist_state_with_scope()

    return {
        "message": f"Successfully uploaded {len(saved_files)} files",
        "files": saved_files,
        "file_infos": saved_file_infos,
        "converted_ncm": converted_ncm_files,
        "failed_ncm": failed_ncm_files,
        "removed_duplicates": removed_duplicates,
        "count": len(current_songs),
        "sorted": sorted_songs,
        "actual_curve": actual_curve,
        "ideal_curve": ideal_curve,
        "analysis_error": analysis_error
    }

@app.delete("/delete/{filename:path}")
async def delete_song(filename: str):
    """Delete a song from the server."""
    music_dir = MUSIC_DIR
    normalized_filename = unquote(filename)
    file_path = os.path.join(music_dir, normalized_filename)
    
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            # also remove cover if exists
            cover_path = os.path.join(music_dir, f"{Path(normalized_filename).stem}.cover.jpg")
            if os.path.exists(cover_path):
                os.remove(cover_path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")
            
    # Update global state and cache
    global current_songs, sorted_songs, actual_curve, ideal_curve
    current_songs = [s for s in current_songs if s["filename"] != normalized_filename]
    
    # Update cache file
    cache_file = CACHE_FILE
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            if normalized_filename in cache:
                del cache[normalized_filename]
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(cache, f, indent=4)
        except Exception:
            pass

    if current_songs:
        sorted_songs, actual_curve, ideal_curve = sort_playlist(current_songs)
    else:
        sorted_songs = []
        actual_curve = []
        ideal_curve = []
    _set_playlist_scope({name for name in _get_effective_playlist_scope() if name.casefold() != normalized_filename.casefold()})
    _sync_playlist_state_with_scope()
        
    return {
        "message": f"Successfully deleted {normalized_filename}",
        "count": len(current_songs),
        "sorted": sorted_songs,
        "actual_curve": actual_curve,
        "ideal_curve": ideal_curve
    }

@app.get("/cover/{filename}")
async def get_cover(filename: str):
    """Get the cover art for a song."""
    music_dir = MUSIC_DIR
    stem = Path(filename).stem
    existing_cover_path = _find_existing_cover_path(music_dir, stem)
    
    if existing_cover_path:
        return FileResponse(existing_cover_path)
        
    file_path = os.path.join(music_dir, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
        
    try:
        audio = MutagenFile(file_path)
        cover_data = None
        
        if hasattr(audio, 'tags') and audio.tags:
            for key, tag in audio.tags.items():
                if isinstance(tag, APIC):
                    cover_data = tag.data
                    break
                elif key.startswith('APIC:'):
                    cover_data = tag.data
                    break
                elif key == 'covr':
                    cover_data = tag[0]
                    break
            if not cover_data and hasattr(audio.tags, 'images') and audio.tags.images:
                cover_data = audio.tags.images[0].data

        if cover_data:
            cover_path = music_dir / f"{stem}.cover{_guess_image_extension(cover_data)}"
            with open(cover_path, "wb") as f:
                f.write(cover_data)
            return FileResponse(cover_path)
    except Exception as e:
        print(f"Failed to extract cover for {filename}: {e}")
        
    raise HTTPException(status_code=404, detail="No cover found")

def _normalize_to_stereo(y: np.ndarray) -> np.ndarray:
    if y.ndim == 1:
        return np.stack([y, y], axis=0)
    if y.shape[0] == 2:
        return y
    if y.shape[0] > 2:
        return y[:2, :]
    return np.repeat(y, 2, axis=0)

def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        casted = float(value)
    except Exception:
        return float(default)
    if not np.isfinite(casted):
        return float(default)
    return float(casted)

def _json_safe_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        safe_obj = {}
        for k, v in value.items():
            safe_obj[str(k)] = _json_safe_value(v)
        return safe_obj
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]
    return str(value)

def _json_literal(value, fallback: str = "[]") -> str:
    try:
        return json.dumps(_json_safe_value(value), ensure_ascii=False)
    except Exception:
        return fallback

def _load_audio_for_export(audio_path: str, sample_rate: int) -> np.ndarray:
    if sf is not None:
        try:
            raw, source_sr = sf.read(audio_path, dtype="float32", always_2d=True)
            y = np.asarray(raw.T, dtype=np.float32)
            y = _normalize_to_stereo(y)
            if int(source_sr) != int(sample_rate):
                if librosa is None:
                    raise RuntimeError("librosa unavailable for resample")
                resampled_channels = []
                for ch in range(y.shape[0]):
                    resampled = librosa.resample(y[ch], orig_sr=source_sr, target_sr=sample_rate, res_type="soxr_mq")
                    resampled_channels.append(np.asarray(resampled, dtype=np.float32))
                y = np.vstack([channel.reshape(1, -1) for channel in resampled_channels])
            return y.astype(np.float32, copy=False)
        except Exception:
            pass
    if librosa is None:
        raise RuntimeError("No audio decoder backend available")
    try:
        y, _ = librosa.load(audio_path, sr=sample_rate, mono=False, res_type="soxr_mq")
        return _normalize_to_stereo(np.asarray(y, dtype=np.float32))
    except Exception:
        raise RuntimeError("Failed to decode audio")

def _load_cached_song_items() -> list[dict]:
    cache_file = CACHE_FILE
    if not os.path.exists(cache_file):
        return []
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            cache_data = json.load(f)
    except Exception:
        return []
    if isinstance(cache_data, dict):
        raw_items = list(cache_data.values())
    elif isinstance(cache_data, list):
        raw_items = cache_data
    else:
        raw_items = []
    return [item for item in raw_items if isinstance(item, dict)]

def _resolve_song_metadata_map() -> dict[str, dict]:
    merged: dict[str, dict] = {}
    sources = []
    effective_scope = {name.casefold() for name in _get_effective_playlist_scope()}
    if isinstance(current_songs, list):
        sources.extend(current_songs)
    if isinstance(sorted_songs, list):
        sources.extend(sorted_songs)
    sources.extend(_load_cached_song_items())
    for item in sources:
        if not isinstance(item, dict):
            continue
        filename = str(item.get("filename") or "").strip()
        if not filename:
            continue
        if effective_scope and filename.casefold() not in effective_scope:
            continue
        merged[filename] = item
    return merged

def _resolve_mix_entry_seconds_map() -> dict[str, float]:
    mix_map: dict[str, float] = {}
    metadata_map = _resolve_song_metadata_map()
    for filename, item in metadata_map.items():
        mix_value = _safe_float(item.get("mix_entry_sec"), 0.0)
        if mix_value > 0:
            mix_map[filename] = mix_value
    return mix_map

def _build_transition_overlaps(file_paths: list[str], automix: bool, metadata_map: dict[str, dict]) -> list[float]:
    transition_count = max(0, len(file_paths) - 1)
    if not automix or transition_count == 0:
        return [0.0] * transition_count
    mix_entry_map = _resolve_mix_entry_seconds_map()
    overlaps: list[float] = []
    default_overlap = 8.0
    for i in range(transition_count):
        filename = os.path.basename(file_paths[i])
        raw_overlap = _safe_float((metadata_map.get(filename) or {}).get("mix_entry_sec"), mix_entry_map.get(filename, default_overlap))
        try:
            overlap_sec = float(raw_overlap)
        except Exception:
            overlap_sec = default_overlap
        if not np.isfinite(overlap_sec) or overlap_sec <= 0:
            overlap_sec = default_overlap
        overlaps.append(float(np.clip(overlap_sec, 2.0, 14.0)))
    return overlaps

def _smooth_step(value: float) -> float:
    safe = min(1.0, max(0.0, float(value)))
    return safe * safe * (3.0 - 2.0 * safe)

def _ease_in_out_cubic(value: float) -> float:
    safe = min(1.0, max(0.0, float(value)))
    if safe < 0.5:
        return 4.0 * (safe ** 3)
    return 1.0 - ((-2.0 * safe + 2.0) ** 3) / 2.0

def _get_fx_intensity_from_loudness_diff(loudness_diff_db: float) -> float:
    normalized = min(1.0, abs(float(loudness_diff_db)) / 8.5)
    return 0.78 + normalized * 0.54

def _get_transition_dynamic_profile(outgoing_song: dict | None, incoming_song: dict | None) -> dict:
    outgoing_base = outgoing_song or {}
    incoming_base = incoming_song or {}
    outgoing_energy = max(
        0.008,
        _safe_float(
            outgoing_base.get("end_dynamic_energy"),
            _safe_float(
                outgoing_base.get("end_10s_energy"),
                _safe_float(
                    outgoing_base.get("end_energy"),
                    _safe_float(outgoing_base.get("energy"), 0.06),
                ),
            ),
        ),
    )
    incoming_energy = max(
        0.008,
        _safe_float(
            incoming_base.get("start_dynamic_energy"),
            _safe_float(
                incoming_base.get("start_10s_energy"),
                _safe_float(
                    incoming_base.get("start_energy"),
                    _safe_float(incoming_base.get("energy"), 0.06),
                ),
            ),
        ),
    )
    loudness_diff_db = 20.0 * np.log10(incoming_energy / outgoing_energy)
    base_fx_intensity = _get_fx_intensity_from_loudness_diff(loudness_diff_db)
    if loudness_diff_db >= 1.5:
        attenuation_db = min(10.0, (loudness_diff_db - 1.5) * 0.92)
        incoming_start_gain = max(0.52, min(0.9, 10.0 ** (-attenuation_db / 20.0)))
        incoming_end_gain = max(incoming_start_gain, 0.93)
        return {
            "incomingStartGain": incoming_start_gain,
            "incomingEndGain": incoming_end_gain,
            "outgoingShape": 1.07,
            "incomingMaxGain": 0.92,
            "totalMixCap": 0.98,
            "fxIntensity": min(1.4, base_fx_intensity * 1.06),
        }
    if loudness_diff_db <= -1.5:
        boost = min(0.08, (-1.5 - loudness_diff_db) * 0.012)
        return {
            "incomingStartGain": 0.88 + boost * 0.5,
            "incomingEndGain": 0.96 + boost,
            "outgoingShape": 1.03,
            "incomingMaxGain": 0.98,
            "totalMixCap": 1.04,
            "fxIntensity": min(1.4, base_fx_intensity * 1.05),
        }
    neutral_trim = min(0.05, abs(loudness_diff_db) * 0.012)
    return {
        "incomingStartGain": 0.84 - neutral_trim * 0.35,
        "incomingEndGain": 0.93 - neutral_trim,
        "outgoingShape": 1.05,
        "incomingMaxGain": 0.95,
        "totalMixCap": 1.0,
        "fxIntensity": max(0.74, base_fx_intensity * 0.92),
    }

def _get_crossfade_effect_multiplier(incoming_time_sec: float) -> float:
    effect_release_start_incoming_sec = 6.0
    effect_release_end_incoming_sec = 8.4
    effect_min_multiplier = 0.0
    safe_incoming_time = max(0.0, float(incoming_time_sec))
    if safe_incoming_time < effect_release_start_incoming_sec:
        return 1.0
    if safe_incoming_time >= effect_release_end_incoming_sec:
        return effect_min_multiplier
    release_ratio = (safe_incoming_time - effect_release_start_incoming_sec) / (effect_release_end_incoming_sec - effect_release_start_incoming_sec)
    smooth_release = _ease_in_out_cubic(release_ratio)
    soft_tail_release = 1.0 - ((1.0 - release_ratio) ** 2.6)
    release_curve = smooth_release * 0.45 + soft_tail_release * 0.55
    return 1.0 - (1.0 - effect_min_multiplier) * release_curve

def _crossfade_state(ratio: float, base_vol: float, breath_ratio: float, dynamic_profile: dict, effect_multiplier: float) -> dict:
    effect_entry_smooth_ratio = 0.3
    safe_ratio = min(1.0, max(0.0, float(ratio)))
    safe_breath = min(0.46, max(0.2, float(breath_ratio)))
    in_ratio = 0.0 if safe_ratio <= safe_breath else (safe_ratio - safe_breath) / (1.0 - safe_breath)
    safe_effect = min(1.0, max(0.0, float(effect_multiplier)))
    entry_enhance = _ease_in_out_cubic(min(1.0, safe_ratio / effect_entry_smooth_ratio))
    fade_out_core = np.cos(safe_ratio * 0.5 * np.pi)
    fade_out_duck = 1.0 - (0.14 * safe_ratio) - (0.12 * (safe_ratio ** 2.2))
    outgoing_depth_raw = min(1.0, 0.28 + (safe_ratio ** 1.16))
    incoming_depth_linear = max(0.12, 0.84 - 0.68 * in_ratio)
    incoming_depth_curve = max(0.12, 0.84 - 0.72 * (in_ratio ** 0.68))
    incoming_depth_raw = incoming_depth_linear * 0.42 + incoming_depth_curve * 0.58
    outgoing_reverb_raw = 0.1 + (safe_ratio ** 1.2) * 0.26
    incoming_reverb_raw = max(0.1, 0.3 - in_ratio * 0.17)
    profile = dynamic_profile or {"incomingStartGain": 0.84, "incomingEndGain": 0.94, "outgoingShape": 1.05, "incomingMaxGain": 0.94, "totalMixCap": 1.0, "fxIntensity": 0.9}
    fx_intensity = min(1.4, max(0.72, _safe_float(profile.get("fxIntensity"), 0.9)))
    fx_depth_scale = 0.72 + fx_intensity * 0.42
    fx_reverb_scale = 0.7 + fx_intensity * 0.5
    depth_entry_floor_curve = entry_enhance ** 1.28
    outgoing_depth_shaped = outgoing_depth_raw * 0.82 + (0.74 * depth_entry_floor_curve) * 0.18
    incoming_depth_shaped = incoming_depth_raw * 0.84 + (0.76 * depth_entry_floor_curve) * 0.16
    outgoing_depth_target = min(1.0, outgoing_depth_shaped * safe_effect * fx_depth_scale)
    incoming_depth_target = min(1.0, incoming_depth_shaped * safe_effect * fx_depth_scale)
    outgoing_depth = min(1.0, outgoing_depth_target * entry_enhance)
    incoming_depth = min(1.0, incoming_depth_target * entry_enhance)
    outgoing_reverb_target = min(0.4, max(outgoing_reverb_raw, 0.3 * entry_enhance) * safe_effect * fx_reverb_scale)
    incoming_reverb_target = min(0.4, max(incoming_reverb_raw, 0.3 * entry_enhance) * safe_effect * fx_reverb_scale)
    outgoing_reverb = min(0.36, outgoing_reverb_target * entry_enhance)
    incoming_reverb = min(0.36, incoming_reverb_target * entry_enhance)
    incoming_start_gain = _safe_float(profile.get("incomingStartGain"), 0.84)
    incoming_end_gain = _safe_float(profile.get("incomingEndGain"), 0.94)
    outgoing_shape = max(0.8, _safe_float(profile.get("outgoingShape"), 1.05))
    incoming_gain = incoming_start_gain + (incoming_end_gain - incoming_start_gain) * (in_ratio ** 0.9)
    shaped_outgoing = (max(0.0, fade_out_core * max(0.0, fade_out_duck))) ** outgoing_shape
    outgoing_volume_raw = min(1.0, base_vol * shaped_outgoing)
    incoming_volume_raw = min(1.0, base_vol * incoming_gain)
    incoming_max_gain = _safe_float(profile.get("incomingMaxGain"), 0.94)
    total_mix_cap = _safe_float(profile.get("totalMixCap"), 1.0)
    incoming_max_volume = min(1.0, base_vol * min(1.05, max(0.72, incoming_max_gain)))
    total_mix_cap_volume = min(1.0, base_vol * min(1.25, max(0.84, total_mix_cap)))
    outgoing_volume = outgoing_volume_raw
    incoming_volume_capped = min(incoming_volume_raw, incoming_max_volume)
    incoming_volume = max(0.0, min(incoming_volume_capped, total_mix_cap_volume - outgoing_volume))
    return {
        "outgoingVolume": outgoing_volume,
        "incomingVolume": incoming_volume,
        "outgoingDepth": outgoing_depth,
        "incomingDepth": incoming_depth,
        "outgoingReverb": outgoing_reverb,
        "incomingReverb": incoming_reverb,
    }

def _next_power_of_two(n: int) -> int:
    value = 1
    while value < n:
        value <<= 1
    return value

def _fft_convolve_1d(signal: np.ndarray, kernel: np.ndarray) -> np.ndarray:
    total = signal.shape[0] + kernel.shape[0] - 1
    fft_len = _next_power_of_two(total)
    sig_fft = np.fft.rfft(signal, n=fft_len)
    ker_fft = np.fft.rfft(kernel, n=fft_len)
    merged_fft = sig_fft * ker_fft
    full = np.fft.irfft(merged_fft, n=fft_len)
    return full[:total]

def _smooth_envelope(envelope: np.ndarray, window_size: int) -> np.ndarray:
    safe = np.asarray(envelope, dtype=np.float32)
    if safe.size == 0:
        return safe
    safe_window = max(1, int(window_size))
    if safe_window <= 1:
        return safe
    if safe_window % 2 == 0:
        safe_window += 1
    padded = np.pad(safe, (safe_window // 2, safe_window // 2), mode="edge")
    kernel = np.ones((safe_window,), dtype=np.float32) / float(safe_window)
    smoothed = np.convolve(padded, kernel, mode="valid")
    return smoothed[:safe.shape[0]].astype(np.float32, copy=False)

def _apply_one_pole_lowpass(signal: np.ndarray, cutoff_hz: float, sample_rate: int) -> np.ndarray:
    safe_signal = np.asarray(signal, dtype=np.float32)
    if safe_signal.size == 0:
        return safe_signal
    safe_cutoff = float(np.clip(cutoff_hz, 120.0, sample_rate * 0.45))
    dt = 1.0 / max(1, sample_rate)
    rc = 1.0 / (2.0 * np.pi * safe_cutoff)
    alpha = dt / (rc + dt)
    output = np.empty_like(safe_signal, dtype=np.float32)
    output[0] = safe_signal[0]
    for i in range(1, safe_signal.shape[0]):
        output[i] = output[i - 1] + alpha * (safe_signal[i] - output[i - 1])
    return output

@lru_cache(maxsize=8)
def _build_impulse_response(sample_rate: int, seconds: float = 2.2, decay: float = 2.0) -> np.ndarray:
    length = max(1, int(sample_rate * seconds))
    rng = np.random.default_rng(20260327)
    progress = np.linspace(0.0, 1.0, length, endpoint=False, dtype=np.float32)
    envelope = (1.0 - progress) ** decay
    noise = rng.uniform(-1.0, 1.0, size=(2, length)).astype(np.float32)
    for ch in range(noise.shape[0]):
        noise[ch] = _apply_one_pole_lowpass(noise[ch], min(7200.0, sample_rate * 0.22), sample_rate)
    impulse = noise * envelope
    impulse_peak = float(np.max(np.abs(impulse))) if impulse.size else 0.0
    if impulse_peak > 0:
        impulse = impulse / impulse_peak * 0.8
    return impulse.astype(np.float32, copy=False)

def _apply_tone_block(block: np.ndarray, depth: float, sample_rate: int) -> np.ndarray:
    safe_depth = min(1.0, max(0.0, float(depth)))
    if safe_depth <= 1e-5:
        return block
    block_len = block.shape[1]
    freqs = np.fft.rfftfreq(block_len, d=1.0 / sample_rate)
    low_gain = 10.0 ** ((safe_depth * 11.5) / 20.0)
    high_gain = 10.0 ** ((-safe_depth * 24.0) / 20.0)
    cutoff = 18000.0 - safe_depth * 17200.0
    cutoff = float(np.clip(cutoff, 800.0, 18000.0))
    q_value = 0.65 + safe_depth * 1.35
    low_curve = 1.0 + (low_gain - 1.0) / (1.0 + (freqs / 220.0) ** 2.0)
    high_curve = 1.0 + (high_gain - 1.0) / (1.0 + (1800.0 / np.maximum(freqs, 1.0)) ** 2.0)
    lp_slope = 2.0 + q_value * 2.0
    lowpass_curve = 1.0 / (1.0 + (freqs / cutoff) ** lp_slope)
    dry_gain = 1.0 - safe_depth * 0.08
    curve = (low_curve * high_curve * lowpass_curve * dry_gain).astype(np.float32, copy=False)
    output = np.zeros_like(block, dtype=np.float32)
    for ch in range(block.shape[0]):
        spectrum = np.fft.rfft(block[ch], n=block_len)
        shaped = spectrum * curve
        restored = np.fft.irfft(shaped, n=block_len)
        output[ch] = restored.astype(np.float32, copy=False)
    return output

def _apply_dynamic_fx(segment: np.ndarray, depth_env: np.ndarray, reverb_env: np.ndarray, sample_rate: int) -> np.ndarray:
    total_samples = segment.shape[1]
    if total_samples == 0:
        return segment
    if float(np.max(depth_env)) <= 1e-5 and float(np.max(reverb_env)) <= 1e-5:
        return segment.astype(np.float32, copy=False)
    depth_track = _smooth_envelope(depth_env, max(9, int(sample_rate * 0.01)))
    reverb_track = _smooth_envelope(reverb_env, max(9, int(sample_rate * 0.012)))
    block_size = min(4096, max(1536, int(sample_rate * 0.09)))
    hop_size = max(256, block_size // 2)
    window = np.sin(np.pi * (np.arange(block_size, dtype=np.float32) + 0.5) / block_size).astype(np.float32, copy=False)
    toned_accum = np.zeros((segment.shape[0], total_samples + block_size), dtype=np.float32)
    weight_accum = np.zeros((total_samples + block_size,), dtype=np.float32)
    for start in range(0, total_samples, hop_size):
        end = min(total_samples, start + block_size)
        block = np.zeros((segment.shape[0], block_size), dtype=np.float32)
        block[:, :end - start] = segment[:, start:end]
        depth = float(np.mean(depth_track[start:end])) if end > start else 0.0
        processed = _apply_tone_block(block, depth, sample_rate) * window.reshape(1, -1)
        toned_accum[:, start:start + block_size] += processed
        weight_accum[start:start + block_size] += window
    safe_weight = np.maximum(weight_accum[:total_samples], 1e-4).reshape(1, -1)
    toned = toned_accum[:, :total_samples] / safe_weight
    impulse = _build_impulse_response(sample_rate)
    wet_signal = np.zeros_like(segment, dtype=np.float32)
    for ch in range(segment.shape[0]):
        convolved = _fft_convolve_1d(toned[ch], impulse[min(ch, impulse.shape[0] - 1)])
        wet_signal[ch] = _apply_one_pole_lowpass(convolved[:total_samples], min(10400.0, sample_rate * 0.24), sample_rate)
        dry_rms = float(np.sqrt(np.mean(np.square(toned[ch], dtype=np.float64)) + 1e-12))
        wet_rms = float(np.sqrt(np.mean(np.square(wet_signal[ch], dtype=np.float64)) + 1e-12))
        if wet_rms > 1e-8:
            wet_scale = min(1.0, max(0.0, (dry_rms / wet_rms) * 0.42))
            wet_signal[ch] *= wet_scale
    wet = np.clip(reverb_track.astype(np.float32, copy=False), 0.0, 0.5).reshape(1, -1)
    return toned * (1.0 - wet) + wet_signal * wet

def _build_crossfade_envelopes(overlap_samples: int, overlap_sec: float, mix_duration_sec: float, breath_ratio: float, dynamic_profile: dict, base_vol: float = 0.8) -> dict[str, np.ndarray]:
    if overlap_samples <= 0:
        zeros = np.zeros((0,), dtype=np.float32)
        return {
            "outgoingVolume": zeros,
            "incomingVolume": zeros,
            "outgoingDepth": zeros,
            "incomingDepth": zeros,
            "outgoingReverb": zeros,
            "incomingReverb": zeros,
        }
    sample_pos = np.arange(overlap_samples, dtype=np.float32)
    ratio_denom = max(mix_duration_sec, 1e-6)
    sec_pos = sample_pos / max(overlap_samples - 1, 1) * max(overlap_sec, 1e-6)
    ratio_pos = np.clip(sec_pos / ratio_denom, 0.0, 1.0)
    outgoing_volume = np.zeros((overlap_samples,), dtype=np.float32)
    incoming_volume = np.zeros((overlap_samples,), dtype=np.float32)
    outgoing_depth = np.zeros((overlap_samples,), dtype=np.float32)
    incoming_depth = np.zeros((overlap_samples,), dtype=np.float32)
    outgoing_reverb = np.zeros((overlap_samples,), dtype=np.float32)
    incoming_reverb = np.zeros((overlap_samples,), dtype=np.float32)
    for i in range(overlap_samples):
        effect_multiplier = _get_crossfade_effect_multiplier(float(sec_pos[i]))
        state = _crossfade_state(float(ratio_pos[i]), base_vol, breath_ratio, dynamic_profile, effect_multiplier)
        outgoing_volume[i] = state["outgoingVolume"]
        incoming_volume[i] = state["incomingVolume"]
        outgoing_depth[i] = state["outgoingDepth"]
        incoming_depth[i] = state["incomingDepth"]
        outgoing_reverb[i] = state["outgoingReverb"]
        incoming_reverb[i] = state["incomingReverb"]
    return {
        "outgoingVolume": outgoing_volume,
        "incomingVolume": incoming_volume,
        "outgoingDepth": outgoing_depth,
        "incomingDepth": incoming_depth,
        "outgoingReverb": outgoing_reverb,
        "incomingReverb": incoming_reverb,
    }

def _mix_tracks(
    file_paths: list[str],
    automix: bool,
    sample_rate: int = 44100,
    progress_callback=None,
) -> np.ndarray:
    merged: np.ndarray | None = None
    metadata_map = _resolve_song_metadata_map()
    transition_overlaps = _build_transition_overlaps(file_paths, automix, metadata_map)
    total_duration = 0.0
    for audio_path in file_paths:
        metadata = metadata_map.get(os.path.basename(audio_path)) or {}
        duration = _safe_float(metadata.get("duration"), 0.0)
        if duration <= 0:
            try:
                info = sf.info(audio_path)
                duration = float(getattr(info, "duration", 0.0) or 0.0)
            except Exception:
                duration = 0.0
        total_duration += max(duration, 20.0)
    processed_duration = 0.0
    for index, audio_path in enumerate(file_paths):
        try:
            song_name = os.path.basename(audio_path)
            song_meta = metadata_map.get(song_name) or {}
            song_duration = max(_safe_float(song_meta.get("duration"), 0.0), 20.0)
            if progress_callback is not None:
                progress_callback(
                    0.03 + 0.67 * min(processed_duration / max(total_duration, 1e-6), 1.0),
                    "mixing",
                    f"正在混音 {index + 1}/{len(file_paths)}",
                )
            y = _load_audio_for_export(audio_path, sample_rate)
            if merged is None:
                merged = y
                processed_duration += song_duration
                continue
            overlap_sec = transition_overlaps[index - 1] if index - 1 < len(transition_overlaps) else 0.0
            if overlap_sec <= 0:
                merged = np.concatenate([merged, y], axis=1)
                processed_duration += song_duration
                continue
            overlap_samples = int(overlap_sec * sample_rate)
            overlap_samples = min(overlap_samples, merged.shape[1], y.shape[1])
            if overlap_samples <= 0:
                merged = np.concatenate([merged, y], axis=1)
                processed_duration += song_duration
                continue
            if not automix:
                phase = np.linspace(0.0, 1.0, overlap_samples, dtype=np.float32)
                fade_out = np.cos(phase * np.pi * 0.5).astype(np.float32, copy=False)
                fade_in = np.sin(phase * np.pi * 0.5).astype(np.float32, copy=False)
                mixed_overlap = merged[:, -overlap_samples:] * fade_out + y[:, :overlap_samples] * fade_in
                merged = np.concatenate([merged[:, :-overlap_samples], mixed_overlap, y[:, overlap_samples:]], axis=1)
                processed_duration += song_duration
                continue
            outgoing_name = os.path.basename(file_paths[index - 1]) if index - 1 >= 0 else ""
            incoming_name = os.path.basename(audio_path)
            outgoing_song = metadata_map.get(outgoing_name)
            incoming_song = metadata_map.get(incoming_name)
            mix_lead = _safe_float((outgoing_song or {}).get("mix_lead_sec"), 10.0)
            mix_breath = _safe_float((outgoing_song or {}).get("mix_breath_sec"), 2.0)
            mix_duration_sec = float(np.clip(mix_lead + 0.05, 9.0, 10.2))
            breath_ratio = float(np.clip(mix_breath / max(mix_duration_sec, 1e-6), 0.2, 0.46))
            dynamic_profile = _get_transition_dynamic_profile(outgoing_song, incoming_song)
            envelopes = _build_crossfade_envelopes(
                overlap_samples=overlap_samples,
                overlap_sec=overlap_sec,
                mix_duration_sec=mix_duration_sec,
                breath_ratio=breath_ratio,
                dynamic_profile=dynamic_profile,
                base_vol=1.0,
            )
            outgoing_overlap = merged[:, -overlap_samples:]
            incoming_overlap = y[:, :overlap_samples]
            processed_outgoing = _apply_dynamic_fx(
                outgoing_overlap,
                envelopes["outgoingDepth"],
                envelopes["outgoingReverb"],
                sample_rate,
            )
            processed_incoming = _apply_dynamic_fx(
                incoming_overlap,
                envelopes["incomingDepth"],
                envelopes["incomingReverb"],
                sample_rate,
            )
            out_vol = envelopes["outgoingVolume"].reshape(1, -1)
            in_vol = envelopes["incomingVolume"].reshape(1, -1)
            mixed_overlap = processed_outgoing * out_vol + processed_incoming * in_vol
            incoming_tail = y[:, overlap_samples:]
            release_samples = min(incoming_tail.shape[1], int(sample_rate * 0.35))
            if release_samples > 0:
                release_source = incoming_tail[:, :release_samples]
                release_depth_start = float(envelopes["incomingDepth"][-1]) if envelopes["incomingDepth"].size else 0.0
                release_reverb_start = float(envelopes["incomingReverb"][-1]) if envelopes["incomingReverb"].size else 0.0
                release_gain_start = float(envelopes["incomingVolume"][-1]) if envelopes["incomingVolume"].size else 1.0
                release_depth_env = np.linspace(release_depth_start, 0.0, release_samples, dtype=np.float32)
                release_reverb_env = np.linspace(release_reverb_start, 0.0, release_samples, dtype=np.float32)
                release_gain_env = np.linspace(release_gain_start, 1.0, release_samples, dtype=np.float32).reshape(1, -1)
                release_processed = _apply_dynamic_fx(release_source, release_depth_env, release_reverb_env, sample_rate) * release_gain_env
                incoming_tail = np.concatenate([release_processed, incoming_tail[:, release_samples:]], axis=1)
            merged = np.concatenate([merged[:, :-overlap_samples], mixed_overlap, incoming_tail], axis=1)
            processed_duration += song_duration
        except Exception as e:
            print(f"Error loading {audio_path}: {e}")
            continue
    if merged is None:
        raise ValueError("no audio data to export")
    peak = float(np.max(np.abs(merged))) if merged.size else 0.0
    if peak > 0.999:
        merged = merged / peak * 0.98
    if progress_callback is not None:
        progress_callback(0.84, "finalizing_mix", "正在整理导出音频")
    return merged.T.astype(np.float32, copy=False)

def _cleanup_temp(path: str) -> None:
    try:
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass

def _prune_export_jobs(max_age_sec: float = 1800.0) -> None:
    cutoff = time.time() - max_age_sec
    with export_jobs_lock:
        stale_ids = [
            job_id
            for job_id, job in export_jobs.items()
            if float(job.get("updated_at", 0.0)) < cutoff and job.get("status") in {"completed", "failed"}
        ]
        for job_id in stale_ids:
            export_jobs.pop(job_id, None)

def _validate_export_payload(payload: AudioExportRequest) -> tuple[list[str], str, str]:
    if not payload.filenames:
        raise HTTPException(status_code=400, detail="No songs selected")
    export_format = (payload.output_format or "wav").lower()
    quality = (payload.quality or "high").lower()
    if export_format not in {"wav", "mp3"}:
        raise HTTPException(status_code=400, detail="Unsupported format")
    if quality not in {"low", "medium", "high"}:
        raise HTTPException(status_code=400, detail="Unsupported quality")
    file_paths: list[str] = []
    for filename in payload.filenames:
        safe_name = os.path.basename(filename)
        candidate = os.path.join(MUSIC_DIR, safe_name)
        if not os.path.isfile(candidate):
            raise HTTPException(status_code=404, detail=f"File not found: {safe_name}")
        file_paths.append(candidate)
    return file_paths, export_format, quality

def _validate_export_options(output_format: str, quality: str) -> tuple[str, str]:
    export_format = (output_format or "wav").lower()
    normalized_quality = (quality or "high").lower()
    if export_format not in {"wav", "mp3"}:
        raise HTTPException(status_code=400, detail="Unsupported format")
    if normalized_quality not in {"low", "medium", "high"}:
        raise HTTPException(status_code=400, detail="Unsupported quality")
    return export_format, normalized_quality

def _upsert_export_job(job_id: str, **updates) -> dict:
    with export_jobs_lock:
        current = export_jobs.get(job_id, {}).copy()
        current.update(updates)
        current["updated_at"] = time.time()
        export_jobs[job_id] = current
        return current.copy()

def _create_export_job(payload: AudioExportRequest) -> dict:
    _prune_export_jobs()
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "status": "queued",
        "progress": 0.0,
        "phase": "queued",
        "message": "准备导出音频",
        "download_ready": False,
        "download_url": None,
        "file_name": None,
        "media_type": "application/octet-stream",
        "temp_dir": None,
        "created_at": time.time(),
        "updated_at": time.time(),
        "payload": payload.model_dump(),
        "error": None,
    }
    with export_jobs_lock:
        export_jobs[job_id] = job
    return job

def _create_uploaded_export_job(payload_dict: dict, file_paths: list[str], temp_dir: str) -> dict:
    _prune_export_jobs()
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "status": "queued",
        "progress": 0.0,
        "phase": "queued",
        "message": "准备导出音频",
        "download_ready": False,
        "download_url": None,
        "file_name": None,
        "media_type": "application/octet-stream",
        "temp_dir": temp_dir,
        "created_at": time.time(),
        "updated_at": time.time(),
        "payload": payload_dict,
        "error": None,
        "file_paths": list(file_paths),
    }
    with export_jobs_lock:
        export_jobs[job_id] = job
    return job

def _get_export_job(job_id: str) -> dict | None:
    with export_jobs_lock:
        job = export_jobs.get(job_id)
        return job.copy() if job else None

def _write_wav_with_progress(
    out_path: str,
    merged: np.ndarray,
    sample_rate: int,
    start_progress: float,
    end_progress: float,
    progress_callback=None,
) -> None:
    if progress_callback is not None:
        progress_callback(start_progress, "writing", "正在写入导出文件")
    sf.write(out_path, merged, sample_rate, subtype="PCM_16")
    if progress_callback is not None:
        progress_callback(end_progress, "writing", "正在完成导出文件")

@lru_cache(maxsize=1)
def _can_write_mp3_with_soundfile() -> bool:
    if sf is None:
        return False
    try:
        formats = sf.available_formats()
        if "MP3" not in formats:
            return False
        subtypes = sf.available_subtypes("MP3")
        return "MPEG_LAYER_III" in subtypes
    except Exception:
        return False

def _write_mp3_with_soundfile(
    out_path: str,
    merged: np.ndarray,
    sample_rate: int,
    start_progress: float,
    end_progress: float,
    progress_callback=None,
) -> None:
    if not _can_write_mp3_with_soundfile():
        raise RuntimeError("soundfile mp3 backend unavailable")
    if progress_callback is not None:
        progress_callback(start_progress, "encoding", "正在编码 MP3")
    sf.write(out_path, merged, sample_rate, format="MP3", subtype="MPEG_LAYER_III")
    if progress_callback is not None:
        progress_callback(end_progress, "encoding", "正在完成 MP3 导出")

@lru_cache(maxsize=1)
def _get_ffmpeg_audio_encoders() -> set[str]:
    try:
        proc = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True,
            text=True,
            check=False,
        )
        output = f"{proc.stdout or ''}\n{proc.stderr or ''}"
    except Exception:
        return set()
    encoders: set[str] = set()
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--") or line.startswith("Encoders:"):
            continue
        match = re.match(r"^[A-Z\.]{6}\s+([A-Za-z0-9_]+)\s+", line)
        if match:
            encoders.add(match.group(1))
    return encoders

def _get_mp3_encode_candidates(quality: str) -> list[list[str]]:
    bitrate_map = {"low": "128k", "medium": "192k", "high": "320k"}
    quality_map = {"low": "4", "medium": "2", "high": "0"}
    bitrate = bitrate_map[quality]
    q_value = quality_map[quality]
    available = _get_ffmpeg_audio_encoders()
    candidates: list[list[str]] = []
    if "libmp3lame" in available:
        candidates.append(["-codec:a", "libmp3lame", "-q:a", q_value, "-b:a", bitrate])
    if "mp3" in available:
        candidates.append(["-codec:a", "mp3", "-b:a", bitrate])
    if "mp3_mf" in available:
        candidates.append(["-codec:a", "mp3_mf", "-b:a", bitrate])
    if "libshine" in available:
        candidates.append(["-codec:a", "libshine", "-b:a", bitrate])
    if not candidates:
        candidates.append(["-codec:a", "libmp3lame", "-q:a", q_value, "-b:a", bitrate])
    return candidates

def _encode_mp3_with_progress(
    wav_path: str,
    out_path: str,
    quality: str,
    duration_sec: float,
    start_progress: float,
    end_progress: float,
    progress_callback=None,
) -> None:
    total_duration = max(float(duration_sec), 0.1)
    error_messages: list[str] = []
    for codec_args in _get_mp3_encode_candidates(quality):
        if os.path.isfile(out_path):
            try:
                os.remove(out_path)
            except OSError:
                pass
        proc = subprocess.Popen(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-progress",
                "pipe:1",
                "-nostats",
                "-i",
                wav_path,
                "-threads",
                "0",
                *codec_args,
                out_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        if progress_callback is not None:
            progress_callback(start_progress, "encoding", "正在编码 MP3")
        if proc.stdout is not None:
            for raw_line in proc.stdout:
                line = raw_line.strip()
                if line.startswith("out_time_ms="):
                    try:
                        out_time_ms = float(line.split("=", 1)[1])
                        ratio = max(0.0, min(out_time_ms / (total_duration * 1000000.0), 1.0))
                        current_progress = start_progress + (end_progress - start_progress) * ratio
                        if progress_callback is not None:
                            progress_callback(current_progress, "encoding", "正在编码 MP3")
                    except Exception:
                        continue
        stderr_output = proc.stderr.read() if proc.stderr is not None else ""
        return_code = proc.wait()
        if return_code == 0:
            if progress_callback is not None:
                progress_callback(end_progress, "encoding", "正在完成 MP3 导出")
            return
        error_messages.append((stderr_output or "ffmpeg failed").strip())
    raise RuntimeError(" / ".join(msg for msg in error_messages if msg) or "ffmpeg failed")

def _run_export_render(
    file_paths: list[str],
    export_format: str,
    quality: str,
    automix: bool,
    temp_dir: str,
    progress_callback=None,
) -> tuple[str, str]:
    sample_rate = 44100
    if progress_callback is not None:
        progress_callback(0.02, "preparing", "正在准备导出资源")
    merged = _mix_tracks(file_paths, automix, sample_rate=sample_rate, progress_callback=progress_callback)
    duration_sec = merged.shape[0] / float(sample_rate) if merged.size else 0.0
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if export_format == "wav":
        out_name = f"LiveSort_mix_{timestamp}.wav"
        out_path = os.path.join(temp_dir, out_name)
        _write_wav_with_progress(out_path, merged, sample_rate, 0.88, 1.0, progress_callback)
        return out_path, out_name
    out_name = f"LiveSort_mix_{timestamp}.mp3"
    out_path = os.path.join(temp_dir, out_name)
    if _can_write_mp3_with_soundfile():
        _write_mp3_with_soundfile(out_path, merged, sample_rate, 0.92, 1.0, progress_callback)
        return out_path, out_name
    wav_tmp_path = os.path.join(temp_dir, f"LiveSort_mix_{timestamp}_tmp.wav")
    _write_wav_with_progress(wav_tmp_path, merged, sample_rate, 0.86, 0.92, progress_callback)
    _encode_mp3_with_progress(wav_tmp_path, out_path, quality, duration_sec, 0.92, 1.0, progress_callback)
    return out_path, out_name

def _process_export_job(job_id: str) -> None:
    job = _get_export_job(job_id)
    if not job:
        return
    payload = AudioExportRequest(**job["payload"])
    file_paths = list(job.get("file_paths") or [])
    if file_paths:
        export_format, quality = _validate_export_options(payload.output_format, payload.quality)
    else:
        file_paths, export_format, quality = _validate_export_payload(payload)
    temp_dir = job.get("temp_dir") or tempfile.mkdtemp(prefix="livesort_export_")
    _upsert_export_job(job_id, status="running", temp_dir=temp_dir, progress=1.0, phase="preparing", message="正在准备导出")
    last_progress = 1.0

    def report(progress_ratio: float, phase: str, message: str) -> None:
        nonlocal last_progress
        safe_progress = max(last_progress, min(float(progress_ratio) * 100.0, 100.0))
        last_progress = safe_progress
        _upsert_export_job(job_id, status="running", progress=safe_progress, phase=phase, message=message)

    try:
        out_path, out_name = _run_export_render(
            file_paths=file_paths,
            export_format=export_format,
            quality=quality,
            automix=bool(payload.automix),
            temp_dir=temp_dir,
            progress_callback=report,
        )
        _upsert_export_job(
            job_id,
            status="completed",
            progress=100.0,
            phase="completed",
            message="导出完成",
            download_ready=True,
            download_url=f"/export/audio/download/{job_id}",
            file_name=out_name,
            media_type="application/octet-stream",
            output_path=out_path,
        )
    except HTTPException as exc:
        _cleanup_temp(temp_dir)
        _upsert_export_job(
            job_id,
            status="failed",
            phase="failed",
            message="导出失败",
            error=str(exc.detail),
            progress=min(max(last_progress, 1.0), 100.0),
        )
    except Exception as exc:
        _cleanup_temp(temp_dir)
        _upsert_export_job(
            job_id,
            status="failed",
            phase="failed",
            message="导出失败",
            error=str(exc),
            progress=min(max(last_progress, 1.0), 100.0),
        )

@app.post("/convert/ncm")
async def convert_ncm_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing file name")
    safe_name = os.path.basename(file.filename)
    if Path(safe_name).suffix.lower() != ".ncm":
        raise HTTPException(status_code=400, detail="Only .ncm files are supported")
    temp_dir = tempfile.mkdtemp(prefix="livesort_ncm_")
    input_path = os.path.join(temp_dir, safe_name)
    try:
        with open(input_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        converted_name = _decrypt_ncm_file(input_path, temp_dir)
        converted_path = os.path.join(temp_dir, converted_name)
        if not os.path.isfile(converted_path):
            raise HTTPException(status_code=500, detail="Converted file missing")
        background_tasks.add_task(_cleanup_temp, temp_dir)
        return FileResponse(
            path=converted_path,
            filename=converted_name,
            media_type="application/octet-stream",
            headers={"X-Converted-Filename": converted_name}
        )
    except HTTPException:
        _cleanup_temp(temp_dir)
        raise
    except ValueError as exc:
        _cleanup_temp(temp_dir)
        raise HTTPException(status_code=400, detail=f"NCM conversion failed: {exc}")
    except Exception as exc:
        _cleanup_temp(temp_dir)
        raise HTTPException(status_code=500, detail=f"NCM conversion failed: {exc}")

@app.post("/export/audio/start")
async def export_audio_start(payload: AudioExportRequest):
    file_paths, export_format, quality = _validate_export_payload(payload)
    if not file_paths:
        raise HTTPException(status_code=400, detail="No songs selected")
    job = _create_export_job(payload)
    thread = threading.Thread(target=_process_export_job, args=(job["job_id"],), daemon=True)
    thread.start()
    return {
        "job_id": job["job_id"],
        "status": "queued",
        "progress": 0,
        "phase": "queued",
        "message": "准备导出音频",
        "format": export_format,
        "quality": quality,
    }

@app.post("/export/audio/start-uploaded")
async def export_audio_start_uploaded(
    files: list[UploadFile] = File(...),
    manifest: str = Form(...)
):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")
    try:
        payload_data = json.loads(manifest)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid export manifest: {exc}")
    payload = AudioExportRequest(**payload_data)
    export_format, quality = _validate_export_options(payload.output_format, payload.quality)
    temp_dir = tempfile.mkdtemp(prefix="livesort_export_")
    source_dir = os.path.join(temp_dir, "sources")
    os.makedirs(source_dir, exist_ok=True)
    saved_map: dict[str, str] = {}
    source_items = payload_data.get("source_items")
    try:
        for index, upload in enumerate(files):
            if not upload.filename:
                continue
            safe_name = os.path.basename(upload.filename).strip() or f"track_{index + 1}.bin"
            suffix = Path(safe_name).suffix or ".bin"
            target_path = os.path.join(source_dir, f"{index + 1:04d}{suffix}")
            with open(target_path, "wb") as buffer:
                shutil.copyfileobj(upload.file, buffer)
            saved_map[safe_name.casefold()] = target_path
        ordered_paths: list[str] = []
        if isinstance(source_items, list) and source_items:
            for index, item in enumerate(source_items):
                if not isinstance(item, dict):
                    raise HTTPException(status_code=400, detail=f"Invalid uploaded source descriptor at index {index}")
                upload_name = os.path.basename(str(item.get("upload_name") or "")).strip()
                if not upload_name:
                    raise HTTPException(status_code=400, detail=f"Missing uploaded source name at index {index}")
                candidate = saved_map.get(upload_name.casefold())
                if not candidate or not os.path.isfile(candidate):
                    raise HTTPException(status_code=404, detail=f"Uploaded source missing: {upload_name}")
                ordered_paths.append(candidate)
        else:
            for filename in payload.filenames:
                safe_name = os.path.basename(filename)
                candidate = saved_map.get(safe_name.casefold())
                if not candidate or not os.path.isfile(candidate):
                    raise HTTPException(status_code=404, detail=f"Uploaded source missing: {safe_name}")
                ordered_paths.append(candidate)
        if not ordered_paths:
            raise HTTPException(status_code=400, detail="No songs selected")
        job = _create_uploaded_export_job(payload.model_dump(), ordered_paths, temp_dir)
        thread = threading.Thread(target=_process_export_job, args=(job["job_id"],), daemon=True)
        thread.start()
        return {
            "job_id": job["job_id"],
            "status": "queued",
            "progress": 0,
            "phase": "queued",
            "message": "准备导出音频",
            "format": export_format,
            "quality": quality,
        }
    except Exception:
        _cleanup_temp(temp_dir)
        raise

@app.get("/export/audio/status/{job_id}")
async def export_audio_status(job_id: str):
    job = _get_export_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")
    return {
        "job_id": job["job_id"],
        "status": job.get("status"),
        "progress": job.get("progress", 0),
        "phase": job.get("phase"),
        "message": job.get("message"),
        "download_ready": job.get("download_ready", False),
        "download_url": job.get("download_url"),
        "file_name": job.get("file_name"),
        "error": job.get("error"),
    }

@app.get("/export/audio/download/{job_id}")
async def export_audio_download(job_id: str, background_tasks: BackgroundTasks):
    job = _get_export_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")
    if job.get("status") != "completed" or not job.get("download_ready"):
        raise HTTPException(status_code=409, detail="Export is not ready")
    out_path = job.get("output_path")
    temp_dir = job.get("temp_dir")
    file_name = job.get("file_name") or os.path.basename(out_path or "")
    if not out_path or not os.path.isfile(out_path):
        raise HTTPException(status_code=404, detail="Exported file not found")
    background_tasks.add_task(_cleanup_temp, temp_dir)
    with export_jobs_lock:
        export_jobs.pop(job_id, None)
    return FileResponse(path=out_path, filename=file_name, media_type=job.get("media_type", "application/octet-stream"))

@app.post("/export/audio")
async def export_audio(payload: AudioExportRequest, background_tasks: BackgroundTasks):
    file_paths, export_format, quality = _validate_export_payload(payload)
    temp_dir = tempfile.mkdtemp(prefix="livesort_export_")
    try:
        out_path, out_name = _run_export_render(
            file_paths=file_paths,
            export_format=export_format,
            quality=quality,
            automix=bool(payload.automix),
            temp_dir=temp_dir,
        )
        background_tasks.add_task(_cleanup_temp, temp_dir)
        return FileResponse(path=out_path, filename=out_name, media_type="application/octet-stream")
    except HTTPException:
        _cleanup_temp(temp_dir)
        raise
    except Exception as e:
        _cleanup_temp(temp_dir)
        raise HTTPException(status_code=500, detail=f"Export failed: {e}")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
