"""How long does each streaming TTS plugin take to notice a dead websocket?

A local server accepts the websocket upgrade, then never sends anything and never
answers pings -- a provider, or a NAT in between, that vanished without FIN/RST.
Each plugin is asked to synthesize and we time how long it takes to raise.

If a TTS socket goes half-open the agent goes mute: the reply is generated, the
text is pushed, and no audio ever arrives. Nothing fails, so no failover happens
and `tts.FallbackAdapter` is never told.

Companion to https://github.com/Rehansanjay/livekit-stt-halfopen, which asks the
same question of the STT plugins. No API keys and no network: everything talks to
127.0.0.1.

    uv run python probe/halfopen_probe.py            # all plugins
    uv run python probe/halfopen_probe.py cartesia   # just some
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import traceback
from contextlib import suppress

import aiohttp
from aiohttp import web

from livekit.agents import APIConnectOptions

WATCH = 75.0  # give up after this; a plugin with heartbeat=30.0 answers in ~46s
PORT = 8766
HTTP = f"http://127.0.0.1:{PORT}"
WS = f"ws://127.0.0.1:{PORT}"
TEXT = "The quick brown fox jumps over the lazy dog."


async def silent_ws(request: web.Request) -> web.WebSocketResponse:
    """Accept the upgrade, then answer nothing at all -- pings included."""
    ws = web.WebSocketResponse(autoping=False)
    await ws.prepare(request)
    await asyncio.sleep(3600)
    return ws


async def session_post(request: web.Request) -> web.Response:
    """Some plugins POST for a session or a voice list before connecting."""
    return web.json_response(
        {
            "id": "probe",
            "url": f"{WS}/ws",
            "websocket_url": f"{WS}/ws",
            "session_id": "probe",
            "token": "probe",
            "voices": [],
            "data": [],
        }
    )


async def session_get(request: web.Request) -> web.StreamResponse:
    """A plain GET that is not an upgrade: hand back the same shape."""
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return await silent_ws(request)
    return web.json_response({"voices": [], "data": [], "url": f"{WS}/ws"})


# Each entry builds a TTS pointed at the local server. A plugin with no way to
# redirect its endpoint is reported as unmeasured rather than guessed.
def build(name: str, http: aiohttp.ClientSession):
    if name == "cartesia":
        from livekit.plugins import cartesia

        return cartesia.TTS(api_key="x", base_url=HTTP, http_session=http)
    if name == "elevenlabs":
        from livekit.plugins import elevenlabs

        return elevenlabs.TTS(api_key="x", base_url=f"{HTTP}/v1", http_session=http)
    if name == "deepgram":
        from livekit.plugins import deepgram

        return deepgram.TTS(api_key="x", base_url=f"{HTTP}/v1/speak", http_session=http)
    if name == "asyncai":
        from livekit.plugins import asyncai

        return asyncai.TTS(api_key="x", base_url=HTTP, http_session=http)
    if name == "bland":
        from livekit.plugins import bland

        return bland.TTS(api_key="x", base_url=HTTP, http_session=http)
    if name == "fishaudio":
        from livekit.plugins import fishaudio

        return fishaudio.TTS(api_key="x", base_url=HTTP, http_session=http)
    if name == "inworld":
        from livekit.plugins import inworld

        return inworld.TTS(api_key="x", base_url=HTTP, ws_url=f"{WS}/ws", http_session=http)
    if name == "minimax":
        from livekit.plugins import minimax

        return minimax.TTS(api_key="x", base_url=HTTP, http_session=http)
    if name == "murf":
        from livekit.plugins import murf

        return murf.TTS(api_key="x", base_url=HTTP, http_session=http)
    if name == "neuphonic":
        from livekit.plugins import neuphonic

        return neuphonic.TTS(api_key="x", base_url=HTTP, http_session=http)
    if name == "soniox":
        from livekit.plugins import soniox

        # the constructor takes the url directly, under `websocket_url`
        return soniox.TTS(api_key="x", websocket_url=f"{WS}/ws", http_session=http)
    if name == "resemble":
        from livekit.plugins.resemble import tts as resemble_tts

        # no endpoint override in the constructor; the url is a module constant
        resemble_tts.RESEMBLE_WEBSOCKET_URL = f"{WS}/ws"
        return resemble_tts.TTS(api_key="x", http_session=http)
    if name == "xai":
        from livekit.plugins.xai import tts as xai_tts

        # no endpoint override in the constructor; the url is a module constant
        xai_tts.XAI_WEBSOCKET_URL = f"{WS}/ws"
        return xai_tts.TTS(api_key="x", http_session=http)
    if name == "respeecher":
        # `_connect_ws` rejects any url that is not wss:// and raises before
        # connecting, so it cannot be pointed at a local plaintext server. It
        # would otherwise register as "raised at 0s", which reads like fast
        # failover and is only input validation.
        raise LookupError("rejects a plaintext ws:// url; wss:// is required by the plugin")
    if name == "sarvam":
        from livekit.plugins import sarvam

        return sarvam.TTS(api_key="x", base_url=HTTP, ws_url=f"{WS}/ws", http_session=http)
    if name == "smallestai":
        from livekit.plugins import smallestai

        return smallestai.TTS(api_key="x", base_url=HTTP, ws_url=f"{WS}/ws", http_session=http)
    if name == "telnyx":
        from livekit.plugins import telnyx

        return telnyx.TTS(api_key="x", base_url=HTTP, http_session=http)
    if name == "baseten":
        from livekit.plugins import baseten

        return baseten.TTS(api_key="x", model_endpoint=f"{WS}/ws", http_session=http)
    if name == "gradium":
        from livekit.plugins import gradium

        return gradium.TTS(api_key="x", model_endpoint=f"{WS}/ws", http_session=http)
    if name == "slng":
        from livekit.plugins import slng

        from livekit.plugins.slng.connection import TTSConnectionConfig

        return slng.TTS(
            api_key="x",
            voice="v",
            connections=[
                TTSConnectionConfig(
                    endpoint=f"{WS}/v1/bridges/unmute/tts/cartesia/sonic:2",
                    model="cartesia/sonic:2",
                    voice="v",
                )
            ],
            http_session=http,
        )
    raise LookupError(f"no local-endpoint override known for {name}")


ALL = [
    "asyncai",
    "baseten",
    "bland",
    "cartesia",
    "deepgram",
    "elevenlabs",
    "fishaudio",
    "gradium",
    "inworld",
    "minimax",
    "murf",
    "neuphonic",
    "resemble",
    "sarvam",
    "slng",
    "smallestai",
    "soniox",
    "telnyx",
    "xai",
]
# respeecher raises for any url that is not wss:// before it opens a socket.
NO_OVERRIDE = ["respeecher"]


async def probe(name: str, http: aiohttp.ClientSession) -> dict:
    started = time.monotonic()
    try:
        tts = build(name, http)
    except LookupError as exc:
        return {"plugin": name, "result": "unmeasured", "detail": str(exc)}
    except Exception as exc:
        return {"plugin": name, "result": "unmeasured", "detail": f"construct: {exc!r}"}

    try:
        stream = tts.stream(conn_options=APIConnectOptions(max_retry=0, timeout=10))
    except Exception as exc:
        return {"plugin": name, "result": "unmeasured", "detail": f"stream(): {exc!r}"}

    async def drain() -> str:
        async for _ in stream:
            return "audio"
        return "ended"

    try:
        stream.push_text(TEXT)
        stream.flush()
        stream.end_input()
    except Exception as exc:
        with suppress(BaseException):
            await stream.aclose()
        return {"plugin": name, "result": "unmeasured", "detail": f"push: {exc!r}"}

    try:
        how = await asyncio.wait_for(drain(), timeout=WATCH)
        return {
            "plugin": name,
            "result": "audio_received" if how == "audio" else "ended_without_error",
            "seconds": round(time.monotonic() - started, 1),
        }
    except asyncio.TimeoutError:
        return {"plugin": name, "result": "never_noticed", "seconds": WATCH}
    except Exception as exc:
        return {
            "plugin": name,
            "result": "raised",
            "seconds": round(time.monotonic() - started, 1),
            "detail": f"{type(exc).__name__}: {str(exc)[:90]}",
        }
    finally:
        with suppress(BaseException):
            await stream.aclose()


async def main() -> None:
    wanted = sys.argv[1:] or ALL

    app = web.Application()
    app.router.add_get("/ws", silent_ws)
    app.router.add_post("/{tail:.*}", session_post)
    app.router.add_get("/{tail:.*}", session_get)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", PORT)
    await site.start()

    results = []
    async with aiohttp.ClientSession() as http:
        # one at a time: a shared event loop makes concurrent timings unreliable
        for name in wanted:
            r = await probe(name, http)
            results.append(r)
            print(
                f"  {r['plugin']:14} {r['result']:20} "
                f"{str(r.get('seconds', '')):>6}  {r.get('detail', '')[:70]}",
                flush=True,
            )

    if not sys.argv[1:]:
        for name in NO_OVERRIDE:
            results.append(
                {
                    "plugin": name,
                    "result": "unmeasured",
                    "detail": "no endpoint override in the public constructor",
                }
            )

    await runner.cleanup()
    print("\n" + json.dumps(results, indent=2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        traceback.print_exc()
