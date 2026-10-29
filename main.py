import asyncio
import json
import time
import uuid

import socketio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

app = FastAPI()
sio = socketio.AsyncServer(async_mode="asgi")

# ---------- Event store ----------
events: list[dict] = []
next_id = 1
new_event = asyncio.Event()  # "something happened" signal for waiters

# Ids live in memory, so they restart at 1 whenever the server restarts.
# The epoch tells clients "this is a new id sequence, reset your cursor".
# (A real system would persist ids instead, e.g. a Postgres sequence.)
EPOCH = uuid.uuid4().hex[:8]

# Bumped by /api/chaos/drop. Every held connection remembers the value it
# started with; if it changes, the server is telling it to hang up.
drop_gen = 0

# What the server is holding open right now (shown in the stats bar)
open_sse = 0
waiting_long_polls = 0
connected_sids: set[str] = set()


def wake_all():
    """Wake every waiter (long polls and SSE streams), then hand out a fresh Event."""
    global new_event
    new_event.set()
    new_event = asyncio.Event()


async def publish(text: str) -> dict:
    global next_id
    evt = {
        "id": next_id,
        "epoch": EPOCH,
        "text": text,
        "at": int(time.time() * 1000),  # milliseconds, to match JS Date.now()
    }
    next_id += 1
    events.append(evt)
    if len(events) > 1000:
        events.pop(0)  # keep memory bounded

    wake_all()
    await sio.emit("update", evt)  # no `to=` means broadcast to every socket

    print("published", evt["id"], text)
    return evt


def events_since(last_id: int) -> list[dict]:
    return [e for e in events if e["id"] > last_id]


def parse_since(value) -> int:
    """A client asking for ids we never issued must be from before a restart: start it from 0."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return 0
    return 0 if n > next_id - 1 else n


def ms_since(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


# ---------- Send messages ----------
class MessageIn(BaseModel):
    text: str = Field(min_length=1, max_length=200)


@app.post("/api/messages", status_code=201)
async def send_message(msg: MessageIn):
    evt = await publish(msg.text)
    return {"id": evt["id"]}


@app.post("/api/burst", status_code=201)
async def burst():
    ids = [(await publish(f"burst message {i}"))["id"] for i in range(1, 6)]
    return {"ids": ids}


# ---------- 1. Short polling ----------
@app.get("/api/poll")
async def poll(since: int = 0):
    return {"events": events_since(parse_since(since))}


# ---------- 2. Long polling ----------
LONG_POLL_TIMEOUT = 25  # seconds; keep below proxy/load-balancer idle timeouts (often 30-60s)


@app.get("/api/long-poll")
async def long_poll(since: int = 0):
    global waiting_long_polls
    since = parse_since(since)

    ready = events_since(since)
    if ready:
        return {"events": ready, "waitedMs": 0}

    started = time.monotonic()
    my_gen = drop_gen
    waiter = new_event  # grab the CURRENT Event; publish() will swap in a new one
    waiting_long_polls += 1
    try:
        await asyncio.wait_for(waiter.wait(), timeout=LONG_POLL_TIMEOUT)
    except asyncio.TimeoutError:
        return {"events": [], "waitedMs": ms_since(started), "timedOut": True}
    finally:
        waiting_long_polls -= 1

    if drop_gen != my_gen:
        return JSONResponse(status_code=503, content={"error": "connection dropped by server"})

    return {"events": events_since(since), "waitedMs": ms_since(started)}


# ---------- 3. Server-Sent Events ----------
SSE_HEARTBEAT = 15  # seconds; keeps idle proxies from closing the stream


def sse_frame(evt: dict) -> str:
    return f"id: {evt['id']}\nevent: update\ndata: {json.dumps(evt)}\n\n"


@app.get("/api/sse")
async def sse(request: Request, since: int = 0):
    # On auto-reconnect the browser sends Last-Event-ID. A brand-new
    # EventSource can't set headers, so the first connect uses ?since=.
    header = request.headers.get("last-event-id")
    last_id = parse_since(header if header is not None else since)
    via = "Last-Event-ID header" if header is not None else "?since query"

    async def stream():
        global open_sse
        nonlocal last_id
        my_gen = drop_gen
        open_sse += 1
        print(f"SSE client connected, resuming after #{last_id}")
        try:
            yield "retry: 3000\n\n"  # tell the browser: reconnect after 3s
            yield f"event: hello\ndata: {json.dumps({'resumedFrom': last_id, 'via': via})}\n\n"

            while True:
                if await request.is_disconnected():
                    break

                # ORDER MATTERS: grab the signal BEFORE sending the backlog.
                waiter = new_event

                for evt in events_since(last_id):
                    yield sse_frame(evt)
                    last_id = evt["id"]

                try:
                    await asyncio.wait_for(waiter.wait(), timeout=SSE_HEARTBEAT)
                except asyncio.TimeoutError:
                    yield ": ping\n\n"

                if drop_gen != my_gen:
                    print("SSE stream dropped by server")
                    return  # ends the response; the browser reconnects by itself
        finally:
            open_sse -= 1
            print("SSE client disconnected")

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------- 4. Socket.io ----------
@sio.event
async def connect(sid, environ, auth):
    last_id = parse_since((auth or {}).get("lastId", 0))
    connected_sids.add(sid)
    print(f"socket {sid} connected, resuming after #{last_id}")
    await sio.emit("hello", {"resumedFrom": last_id}, to=sid)
    for evt in events_since(last_id):
        await sio.emit("update", evt, to=sid)


@sio.event
async def disconnect(sid, *args):  # newer versions also pass a reason
    connected_sids.discard(sid)
    print(f"socket {sid} disconnected", *args)


@sio.on("send") # pyright: ignore[reportOptionalCall]
async def on_send(sid, data):
    text = str((data or {}).get("text", "")).strip()[:200]
    if not text:
        return {"ok": False, "error": "empty message"}
    evt = await publish(text)
    return {"ok": True, "id": evt["id"]}  # the return value is sent back as the ack


@sio.on("rtt") # pyright: ignore[reportOptionalCall]
async def on_rtt(sid, sent_at):
    return sent_at


# ---------- 5. Chaos + stats ----------
@app.post("/api/chaos/drop")
async def drop_all():
    global drop_gen
    dropped = {"sse": open_sse, "longPolls": waiting_long_polls, "sockets": len(connected_sids)}

    drop_gen += 1
    wake_all()  # held long polls and SSE streams wake up, see the new drop_gen, and hang up
    for sid in list(connected_sids):
        await sio.disconnect(sid)  # client sees reason "io server disconnect"

    return {"dropped": dropped}


@app.get("/api/stats")
async def stats():
    return {
        "epoch": EPOCH,
        "totalEvents": next_id - 1,
        "openSse": open_sse,
        "waitingLongPolls": waiting_long_polls,
        "sockets": len(connected_sids),
    }


# Keep this LAST among the FastAPI routes: it catches every other path.
app.mount("/", StaticFiles(directory="public", html=True), name="static")

# Socket.io handles /socket.io/*, everything else is passed to FastAPI
asgi_app = socketio.ASGIApp(sio, other_asgi_app=app)