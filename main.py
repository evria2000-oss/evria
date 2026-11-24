import os
import json
import uuid
from pathlib import Path
from typing import Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

APP_DIR = Path(__file__).parent
STATIC_DIR = APP_DIR / "static"
DATA_DIR = Path(os.getenv("DATA_DIR", str(APP_DIR / "data")))
DATA_DIR.mkdir(parents=True, exist_ok=True)
VIDEO_PATH = DATA_DIR / "shared_video"

ROOM_CODE = os.getenv("ROOM_CODE", "246810")
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "500"))
ALLOWED_TYPES = {"video/mp4", "video/webm", "video/ogg", "application/octet-stream"}

app = FastAPI(title="VStream")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# This starter version keeps chat and playback state in memory.
rooms: Dict[str, Set[WebSocket]] = {}
names: Dict[WebSocket, str] = {}
playback_state: Dict[str, dict] = {}


@app.get("/", response_class=HTMLResponse)
async def home():
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/video")
async def get_video():
    if not VIDEO_PATH.exists():
        raise HTTPException(status_code=404, detail="هنوز ویدئویی آپلود نشده است.")
    return FileResponse(VIDEO_PATH, media_type="video/mp4", filename="shared-video.mp4")


@app.get("/api/status")
async def status():
    return {
        "video_exists": VIDEO_PATH.exists(),
        "room_code_required": True,
        "max_upload_mb": MAX_UPLOAD_MB
    }


@app.post("/upload")
async def upload_video(file: UploadFile = File(...), room_code: str = ""):
    if room_code != ROOM_CODE:
        raise HTTPException(status_code=403, detail="کد اتاق اشتباه است.")
    if VIDEO_PATH.exists():
        raise HTTPException(status_code=409, detail="یک ویدئو قبلاً آپلود شده است. برای تعویض، ابتدا فایل قبلی را از پوشه data حذف کنید.")
    if file.content_type not in ALLOWED_TYPES and not (file.filename or "").lower().endswith((".mp4", ".webm", ".ogg")):
        raise HTTPException(status_code=415, detail="فرمت مجاز: MP4، WebM یا OGG")
    temp_path = DATA_DIR / f"upload-{uuid.uuid4().hex}.tmp"
    total = 0
    try:
        with temp_path.open("wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_UPLOAD_MB * 1024 * 1024:
                    raise HTTPException(status_code=413, detail=f"حجم ویدئو نباید بیشتر از {MAX_UPLOAD_MB} مگابایت باشد.")
                out.write(chunk)
        temp_path.replace(VIDEO_PATH)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    finally:
        await file.close()
    return {"ok": True, "message": "ویدئو با موفقیت آپلود شد."}


async def broadcast(room: str, message: dict, exclude: WebSocket = None):
    dead = []
    for ws in list(rooms.get(room, set())):
        if ws is exclude:
            continue
        try:
            await ws.send_text(json.dumps(message, ensure_ascii=False))
        except Exception:
            dead.append(ws)
    for ws in dead:
        rooms.get(room, set()).discard(ws)
        names.pop(ws, None)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    room = ws.query_params.get("room", "")
    name = (ws.query_params.get("name", "مهمان") or "مهمان")[:24]
    code = ws.query_params.get("code", "")
    if room != "main" or code != ROOM_CODE:
        await ws.send_text(json.dumps({"type": "error", "text": "کد اتاق معتبر نیست."}, ensure_ascii=False))
        await ws.close(code=1008)
        return

    members = rooms.setdefault(room, set())
    if len(members) >= 2:
        await ws.send_text(json.dumps({"type": "error", "text": "ظرفیت اتاق تکمیل است؛ فقط دو نفر می‌توانند وارد شوند."}, ensure_ascii=False))
        await ws.close(code=1008)
        return

    members.add(ws)
    names[ws] = name
    await ws.send_text(json.dumps({
        "type": "welcome",
        "members": len(members),
        "video_exists": VIDEO_PATH.exists(),
        "state": playback_state.get(room, {"action": "pause", "time": 0})
    }, ensure_ascii=False))
    await broadcast(room, {"type": "system", "text": f"{name} وارد اتاق شد.", "members": len(members)}, exclude=ws)

    try:
        while True:
            raw = await ws.receive_text()
            if len(raw) > 8000:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            kind = data.get("type")
            if kind == "chat":
                text = str(data.get("text", "")).strip()[:1000]
                if text:
                    await broadcast(room, {"type": "chat", "name": name, "text": text})
            elif kind in ("play", "pause", "seek"):
                try:
                    current_time = max(0, float(data.get("time", 0)))
                except (TypeError, ValueError):
                    current_time = 0
                state = {"action": kind, "time": current_time, "by": name}
                playback_state[room] = state
                await broadcast(room, {"type": "sync", **state}, exclude=ws)
    except WebSocketDisconnect:
        pass
    finally:
        members.discard(ws)
        names.pop(ws, None)
        await broadcast(room, {"type": "system", "text": f"{name} از اتاق خارج شد.", "members": len(members)})


@app.get("/health")
async def health():
    return {"status": "ok"}
