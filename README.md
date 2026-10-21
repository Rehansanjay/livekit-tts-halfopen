# How long does a LiveKit TTS plugin take to notice a dead websocket?

If a TTS websocket goes half-open — the provider, or a NAT in between, disappears without a FIN or an RST — nothing arrives and nothing fails. The reply is generated, the text is pushed, and no audio ever comes back. On a call the agent simply goes mute, which the caller reads as a dropped line.

This measures all twenty streaming TTS plugins in the `livekit/agents` tree. It is the companion to [livekit-stt-halfopen](https://github.com/Rehansanjay/livekit-stt-halfopen), which asks the same question of the STT plugins, and **the answer turns out to be the opposite one.**

A local server accepts the websocket upgrade and then sends nothing and answers no pings. Each plugin is asked to synthesize and timed until it raises. No API keys and no network: everything talks to `127.0.0.1`.

Measured against `livekit/agents` at **`3c6e65124`**, 21 October 2026, with `APIConnectOptions(max_retry=0, timeout=10)` and a 75-second watch.

## Results

**Fails over** — raises inside the configured 10-second timeout:

| plugin | time to raise | |
| --- | --- | --- |
| bland | 10.0s | `APITimeoutError` |
| cartesia | 10.0s | `APITimeoutError` |
| gradium | 10.0s | `APITimeoutError` |
| slng | 10.0s | `APITimeoutError` |
| asyncai | 10.1s | `APITimeoutError` |
| deepgram | 10.1s | `APITimeoutError` |
| elevenlabs | 10.1s | `APIStatusError: Could not synthesize` |
| sarvam | 10.1s | `APITimeoutError: WebSocket receive timeout` |
| smallestai | 10.4s | `APITimeoutError` |

**Fails over eventually** — raises, but long after the timeout it was given:

| plugin | time to raise | why it takes that long |
| --- | --- | --- |
| minimax | 20.0s | twice the configured timeout |
| fishaudio | 46.8s | no read timeout; the `heartbeat=30.0` ping is what eventually closes it |
| inworld | 70.0s | its `receive(timeout=60.0)` is a poll, not a bound — see below |

**Never notices** — still waiting when the watch expires at 75s:

| plugin | |
| --- | --- |
| baseten | `conn_options.timeout` is spent on `sock_connect` only |
| murf | same |
| neuphonic | same |
| resemble | same |
| xai | same |
| telnyx | `conn_options.timeout` is spent on the `ws_connect` wait only |
| soniox | same, and `_Connection.connect()` cannot see the timeout at all |

Nine fail over properly, three eventually, seven not at all. One could not be measured — see below.

All seven are fixed in [#8035](https://github.com/livekit/agents/pull/8035), which brings them to 10.0–12.9s:

| plugin | before | after |
| --- | --- | --- |
| baseten | never (75s) | 12.9s |
| murf | never (75s) | 10.1s |
| neuphonic | never (75s) | 10.1s |
| telnyx | never (75s) | 10.0s |
| resemble | never (75s) | 10.1s |
| soniox | never (75s) | 10.0s |
| xai | never (75s) | 11.2s |

### The instrument that matters here is not the one that mattered for STT

For STT, `heartbeat=` was the whole story: every plugin that passed one noticed the dead socket, and every plugin that did not, did not. I expected to repeat that here, and started by scanning all twenty TTS plugins for it. **18 of the 20 pass no heartbeat at all** — only `fishaudio` and `sarvam` do.

That scan predicts almost nothing. What actually separates the groups is whether a read timeout reaches `ws.receive()`:

```python
# cartesia, deepgram, smallestai — bounded, because asyncio.TimeoutError propagates
msg = await ws.receive(timeout=self._conn_options.timeout)
```

```python
# baseten, murf, neuphonic — conn_options.timeout never reaches the read
timeout=aiohttp.ClientTimeout(total=30, sock_connect=self._conn_options.timeout)
```

```python
# telnyx — the timeout bounds connecting, and nothing bounds receiving
ws = await asyncio.wait_for(session.ws_connect(...), self._conn_options.timeout)
```

A heartbeat still works, but it is the weaker instrument: `fishaudio` has one and no read timeout, so it takes **46.8s against cartesia's 10.0s** — 4.7x slower, for the same outcome. Eight of the nine plugins in that first group pass no heartbeat at all and still beat it, because the timeout the caller actually asked for reaches the read. The ninth, `sarvam`, carries both and answers on the read timeout at 10.1s, never reaching its heartbeat.

This is the opposite recommendation to the STT repo, and the reason is the workload. A quiet caller is the normal state for STT, so a read timeout is the wrong instrument there ([#5717](https://github.com/livekit/agents/issues/5717)) and a heartbeat is right. A TTS stream that has been handed text and returns nothing is never normal, so the read timeout is both available and strictly better.

### The sixty-second timeout that isn't one

`inworld` looks bounded and is not:

```python
msg = await self._ws.receive(timeout=60.0)
except asyncio.TimeoutError:
    continue      # round again
```

That is the same shape I got wrong in the STT repo, where `baseten`, `fireworksai` and `gradium` each wrapped a read in `asyncio.wait_for(..., timeout=5)` whose handler also `continue`d. A timeout whose handler loops is a poll, not a liveness bound. `inworld`'s 70.0s comes from something further out, not from that line, and the hardcoded `60.0` ignores `conn_options.timeout` entirely.

I checked for this one specifically because of that earlier mistake. It is worth checking for by hand: the pattern reads like a bound in review and measures like nothing at all.

## Not measured, and why

Reported rather than guessed:

| plugin | reason |
| --- | --- |
| respeecher | `_connect_ws` raises `APIConnectionError` for any url that is not `wss://`, before it opens a socket. It would otherwise register as "raised at 0.0s", which reads like the fastest failover in the table and is only input validation. Reading the source it does pass a timeout to `ws.receive()`, so it looks bounded — but that is a source read, not a measurement. |

Three plugins were on this list in the first pass and should not have been. I had scanned constructors for `base_url`, `ws_url`, `url` and `endpoint`, and concluded that `resemble`, `soniox` and `xai` could not be redirected. That was the scan's limitation, not the plugins':

- **soniox** takes the url directly, as `websocket_url`. My signature scan looked for a parameter named `url`, which does not match `websocket_url`.
- **resemble** and **xai** keep theirs in a module constant (`RESEMBLE_WEBSOCKET_URL`, `XAI_WEBSOCKET_URL`) that the probe can assign before constructing.

All three then measured at the full 75-second watch. Reporting something as unmeasurable is cheap and safe, and it hid three of the seven broken plugins for a few hours — worth more scepticism than I gave it.

## Running it

```bash
# from a livekit/agents checkout, with the workspace installed
uv run python probe/halfopen_probe.py              # every plugin
uv run python probe/halfopen_probe.py cartesia     # just some
```

The probe needs a checkout of `livekit/agents` with the plugins importable, because it constructs each `TTS` directly and points it at `127.0.0.1`. `probe/halfopen_probe.py` carries a per-plugin construction table: most take a `base_url`, some a `model_endpoint` or a `ws_url`, `slng` needs a `TTSConnectionConfig` whose endpoint carries the model in its path, and four cannot be redirected at all.

## Related

- [livekit-stt-halfopen](https://github.com/Rehansanjay/livekit-stt-halfopen) — the same question for the nineteen streaming STT plugins, where the answer is `heartbeat=` rather than a read timeout
- [livekit/agents#7900](https://github.com/livekit/agents/issues/7900) — the original four-plugin STT measurement by @DEVJHAWAR11, and the probe both of these extend
- [#8031](https://github.com/livekit/agents/issues/8031) — detecting a dead socket is not the same as reporting it: two STT plugins notice and reconnect into it forever

Raw numbers: [`results/2026-10-21-origin-main.json`](results/2026-10-21-origin-main.json).
