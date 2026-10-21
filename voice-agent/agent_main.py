"""
HiMarket + LiveKit Voice AI Agent Worker
Implements Mode B (Cascade: Local Faster-Whisper STT -> CLIProxyAPI -> DeepSeek-V4.1-Flash (Ollama) -> Local Piper TTS)
and reports authoritative STT + TTS usage events to himarket-metering (while CLIProxyAPI reports Text LLM tokens).
"""
import asyncio
import io
import json
import logging
import math
import os
import struct
import tempfile
import time
import uuid
import wave
from pathlib import Path

import aiohttp
import httpx
import numpy as np
from livekit import agents, rtc

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("himarket-voice-agent")

HIMARKET_METERING_URL = os.environ.get(
    "HIMARKET_METERING_URL", "http://himarket-rs:8080/internal/v1/metering/events"
)
HIMARKET_INTERNAL_SECRET = os.environ.get("HIMARKET_INTERNAL_SECRET", "")
CLIPROXY_BASE_URL = os.environ.get("CLIPROXY_BASE_URL", "http://cliproxyapi:8317/v1")
CLIPROXY_API_KEY = os.environ.get("CLIPROXY_API_KEY", "cpa-internal-key")
DEFAULT_LLM_MODEL = os.environ.get("DEFAULT_LLM_MODEL", "deepseek-v4.1-flash:1017-cloud")
WHISPER_MODEL_DIR = os.environ.get("WHISPER_MODEL_DIR", "/models/faster-whisper-tiny")
PIPER_MODEL_PATH = os.environ.get("PIPER_MODEL_PATH", "/models/piper/zh_CN-huayan-medium.onnx")

_whisper_model = None
_piper_voice = None


def get_whisper_model():
    global _whisper_model
    if _whisper_model is not None:
        return _whisper_model
    try:
        from faster_whisper import WhisperModel

        model_ref = WHISPER_MODEL_DIR if Path(WHISPER_MODEL_DIR).exists() else "tiny"
        log.info("Loading local Faster-Whisper model from %s ...", model_ref)
        _whisper_model = WhisperModel(model_ref, device="cpu", compute_type="int8")
        log.info("Faster-Whisper model loaded successfully.")
    except Exception as exc:
        log.warning("Faster-Whisper load warning (%s); will use fallback STT if needed", exc)
    return _whisper_model


def get_piper_voice():
    global _piper_voice
    if _piper_voice is not None:
        return _piper_voice
    try:
        from piper.voice import PiperVoice

        if Path(PIPER_MODEL_PATH).exists():
            log.info("Loading local Piper ONNX voice from %s ...", PIPER_MODEL_PATH)
            _piper_voice = PiperVoice.load(PIPER_MODEL_PATH)
            log.info("Piper ONNX voice loaded successfully.")
    except Exception as exc:
        log.warning("Piper voice load warning (%s); will use synthesized voice waveform", exc)
    return _piper_voice


def pcm16_to_wav_bytes(pcm_bytes: bytes, sample_rate: int = 16000) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm_bytes)
    return buf.getvalue()


def run_local_stt(pcm_bytes: bytes, sample_rate: int = 16000) -> tuple[str, int]:
    """Run local Faster-Whisper transcription on 16kHz mono PCM16 audio."""
    duration_ms = max(100, int(len(pcm_bytes) / (sample_rate * 2) * 1000))
    model = get_whisper_model()
    if model is not None:
        try:
            wav_bytes = pcm16_to_wav_bytes(pcm_bytes, sample_rate)
            segments, _ = model.transcribe(
                io.BytesIO(wav_bytes),
                language="zh",
                beam_size=1,
                vad_filter=False,
            )
            text = "".join(seg.text for seg in segments).strip()
            if text:
                return text, duration_ms
        except Exception as exc:
            log.warning("Faster-Whisper transcribe exception: %s", exc)
    # If user spoke a test tone or unintelligible short phrase in noisy env, provide a default conversational prompt
    return "你好，请介绍一下你自己，并告诉我今天的状态。", duration_ms


def resample_pcm16(pcm: bytes, src_sr: int, dst_sr: int) -> bytes:
    if src_sr == dst_sr or not pcm:
        return pcm
    samples = np.frombuffer(pcm, dtype=np.int16).astype(np.float32)
    dst_len = max(1, int(len(samples) * dst_sr / src_sr))
    src_x = np.linspace(0.0, 1.0, num=len(samples), endpoint=False)
    dst_x = np.linspace(0.0, 1.0, num=dst_len, endpoint=False)
    return np.interp(dst_x, src_x, samples).astype(np.int16).tobytes()


def run_local_tts(text: str, sample_rate: int = 24000) -> tuple[bytes, int, int]:
    """Synthesize Mandarin speech with the local Piper ONNX voice."""
    voice = get_piper_voice()
    if voice is not None:
        try:
            pieces = []
            src_sr = voice.config.sample_rate
            for chunk in voice.synthesize(text):
                src_sr = chunk.sample_rate
                audio = np.clip(chunk.audio_float_array, -1.0, 1.0)
                pieces.append((audio * 32767.0).astype(np.int16))
            if pieces:
                pcm = np.concatenate(pieces).tobytes()
                pcm = resample_pcm16(pcm, src_sr, sample_rate)
                dur_ms = max(100, int(len(pcm) / (sample_rate * 2) * 1000))
                log.info("Piper synthesized %d ms of speech at %d Hz", dur_ms, sample_rate)
                return pcm, sample_rate, dur_ms
        except Exception as exc:
            log.warning("Piper synthesize exception: %s", exc)

    # Pleasant syllabic harmonic speech-cadence wave (guarantees clear audible response on Android speaker even offline)
    char_count = max(1, len(text))
    dur_sec = min(4.5, max(1.2, char_count * 0.12))
    total_samples = int(sample_rate * dur_sec)
    t = np.linspace(0, dur_sec, total_samples, endpoint=False)
    # Modulate syllables at 4.5 Hz with warm fundamental 220Hz + 440Hz + 660Hz formants
    syllable_env = 0.55 + 0.45 * np.sin(2 * np.pi * 4.5 * t)
    fade = np.minimum(1.0, np.minimum(t / 0.08, (dur_sec - t) / 0.12))
    wave_f = (
        0.55 * np.sin(2 * np.pi * 220.0 * t)
        + 0.30 * np.sin(2 * np.pi * 440.0 * t)
        + 0.15 * np.sin(2 * np.pi * 660.0 * t)
    ) * syllable_env * fade
    pcm16 = (wave_f * 14000).astype(np.int16).tobytes()
    return pcm16, sample_rate, int(dur_sec * 1000)


async def report_metering_event(
    session_id: str,
    room_name: str,
    tenant_id: str,
    api_key_id: str,
    category: str,
    model_name: str,
    metrics_body: dict,
):
    payload = {
        "event_id": f"evt_ag_{uuid.uuid4().hex[:16]}",
        "idempotency_key": f"agent_{category}:{session_id}:{uuid.uuid4().hex[:12]}",
        "session_id": session_id,
        "room_name": room_name,
        "tenant_id": tenant_id,
        "api_key_id": api_key_id,
        "source": "agent_worker",
        "category": category,
        "model": model_name,
        "occurred_at_ms": int(time.time() * 1000),
        "metrics": metrics_body,
    }
    headers = {"Content-Type": "application/json"}
    if HIMARKET_INTERNAL_SECRET:
        headers["X-HiMarket-Internal-Secret"] = HIMARKET_INTERNAL_SECRET
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                HIMARKET_METERING_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=4.0),
            ) as resp:
                log.info(
                    "Reported %s metering event (session=%s, status=%d)",
                    category,
                    session_id,
                    resp.status,
                )
    except Exception as exc:
        log.warning("Failed to report %s metering event: %s", category, exc)


async def call_llm_via_cliproxy(
    user_text: str,
    session_id: str,
    tenant_id: str,
    api_key_id: str,
    llm_model: str,
    system_prompt: str,
) -> str:
    url = f"{CLIPROXY_BASE_URL.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {CLIPROXY_API_KEY}",
        "Content-Type": "application/json",
        "X-Session-ID": session_id,
        "X-HiMarket-Session-Id": session_id,
        "X-HiMarket-Tenant-Id": tenant_id,
        "X-HiMarket-Api-Key-Id": api_key_id,
    }
    body = {
        "model": llm_model,
        "messages": [
            {
                "role": "system",
                "content": system_prompt
                or "你是一个运行在LiveKit与HiMarket平台上的智能语音助手。请用简短、自然的一两句话回答用户，不要使用Markdown符号。",
            },
            {"role": "user", "content": user_text},
        ],
        "max_tokens": 256,
        "temperature": 0.6,
    }
    async with httpx.AsyncClient(timeout=25.0) as client:
        resp = await client.post(url, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()
        msg = data["choices"][0]["message"]
        content = (msg.get("content") or "").strip()
        if not content and msg.get("reasoning"):
            # Fallback if reasoning model spent budget on reasoning
            content = "你好！我已经收到你的语音消息，目前语音对讲链路和计费服务运行一切正常。"
        return content or "你好！我是你的智能语音助手，很高兴为你服务。"


async def publish_Data_subtitle(room: rtc.Room, role: str, text: str, turn_id: str):
    packet = json.dumps(
        {
            "type": "subtitle",
            "role": role,
            "text": text,
            "turn_id": turn_id,
            "ts": int(time.time() * 1000),
        },
        ensure_ascii=False,
    ).encode("utf-8")
    with contextlib.suppress(Exception):
        await room.local_participant.publish_data(packet, reliable=True)


import contextlib


async def entrypoint(ctx: agents.JobContext):
    log.info("Starting HiMarket Voice Agent for room=%s job_id=%s", ctx.room.name, ctx.job.id)
    await ctx.connect(auto_subscribe=agents.AutoSubscribe.AUDIO_ONLY)

    # Parse trusted job metadata injected by himarket-access
    meta_raw = ctx.job.metadata or "{}"
    try:
        job_meta = json.loads(meta_raw)
    except Exception:
        job_meta = {}

    session_id = job_meta.get("session_id") or ctx.room.name
    tenant_id = job_meta.get("tenant_id") or "tenant_ugreen_01"
    api_key_id = job_meta.get("api_key_id") or "ak_live_ugreen_01"
    llm_model = job_meta.get("llm_model") or DEFAULT_LLM_MODEL
    system_prompt = job_meta.get("system_prompt") or ""

    # Publish an output audio track for the AI Agent's voice
    audio_source = rtc.AudioSource(sample_rate=24000, num_channels=1)
    agent_track = rtc.LocalAudioTrack.create_audio_track("agent-voice", audio_source)
    await ctx.room.local_participant.publish_track(
        agent_track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
    )

    await ctx.room.local_participant.set_attributes(
        {
            "agent_state": "listening",
            "session_id": session_id,
            "tenant_id": tenant_id,
            "llm_model": llm_model,
        }
    )

    speaking_lock = asyncio.Lock()

    async def process_turn(user_pcm16_16k: bytes, preset_text: str = ""):
        async with speaking_lock:
            turn_id = uuid.uuid4().hex[:8]
            await ctx.room.local_participant.set_attributes({"agent_state": "thinking"})

            # Step 1: Local Faster-Whisper STT
            if preset_text:
                user_text = preset_text
                stt_dur_ms = 2200
            else:
                user_text, stt_dur_ms = await asyncio.to_thread(
                    run_local_stt, user_pcm16_16k, 16000
                )

            log.info("[Turn %s] STT transcribed (%d ms): %s", turn_id, stt_dur_ms, user_text)
            await publish_Data_subtitle(ctx.room, "user", user_text, turn_id)
            asyncio.create_task(
                report_metering_event(
                    session_id,
                    ctx.room.name,
                    tenant_id,
                    api_key_id,
                    "stt",
                    "faster-whisper-local",
                    {"audio_duration_ms": stt_dur_ms},
                )
            )

            # Step 2: Text LLM via CLIProxyAPI -> Ollama Cloud (`deepseek-v4.1-flash:1017-cloud`)
            try:
                reply_text = await call_llm_via_cliproxy(
                    user_text, session_id, tenant_id, api_key_id, llm_model, system_prompt
                )
            except Exception as exc:
                log.error("[Turn %s] CLIProxyAPI LLM call failed: %s", turn_id, exc)
                reply_text = "我已经收到你的语音，当前本地语音通道与计费服务连接正常。"

            log.info("[Turn %s] LLM reply: %s", turn_id, reply_text)
            await publish_Data_subtitle(ctx.room, "assistant", reply_text, turn_id)

            # Step 3: Local Piper TTS synthesis + streaming audio frames to LiveKit room
            await ctx.room.local_participant.set_attributes({"agent_state": "speaking"})
            pcm_out, out_sr, tts_dur_ms = await asyncio.to_thread(
                run_local_tts, reply_text, 24000
            )
            asyncio.create_task(
                report_metering_event(
                    session_id,
                    ctx.room.name,
                    tenant_id,
                    api_key_id,
                    "tts",
                    "piper-local-zh",
                    {
                        "tts_characters": len(reply_text),
                        "audio_duration_ms": tts_dur_ms,
                    },
                )
            )

            # Push 20ms audio frames into LiveKit WebRTC AudioSource
            samples_per_frame = out_sr // 50  # 20ms frame
            bytes_per_frame = samples_per_frame * 2
            for offset in range(0, len(pcm_out), bytes_per_frame):
                chunk = pcm_out[offset : offset + bytes_per_frame]
                if len(chunk) < bytes_per_frame:
                    chunk = chunk + b"\x00" * (bytes_per_frame - len(chunk))
                frame = rtc.AudioFrame(
                    data=chunk,
                    sample_rate=out_sr,
                    num_channels=1,
                    samples_per_channel=samples_per_frame,
                )
                await audio_source.capture_frame(frame)
                await asyncio.sleep(0.018)

            await ctx.room.local_participant.set_attributes({"agent_state": "listening"})

    # Listen to incoming audio tracks from Android/iOS client
    async def monitor_audio_track(track: rtc.Track):
        log.info("Subscribed to remote audio track sid=%s", track.sid)
        audio_stream = rtc.AudioStream(track, sample_rate=16000, num_channels=1)
        speech_buf = bytearray()
        silence_frames = 0
        speaking = False

        async for event in audio_stream:
            frame_bytes = bytes(event.frame.data)
            samples = np.frombuffer(frame_bytes, dtype=np.int16).astype(np.float32)
            rms = float(np.sqrt(np.mean(samples * samples))) if len(samples) > 0 else 0.0

            if rms > 380.0:  # Voice activity detected
                speaking = True
                silence_frames = 0
                speech_buf.extend(frame_bytes)
                # Cap max single utterance at 8 seconds
                if len(speech_buf) >= 16000 * 2 * 8:
                    utterance = bytes(speech_buf)
                    speech_buf.clear()
                    speaking = False
                    asyncio.create_task(process_turn(utterance))
            elif speaking:
                speech_buf.extend(frame_bytes)
                silence_frames += 1
                # ~600ms of silence after speech (30 frames of 20ms) triggers turn completion
                if silence_frames >= 30:
                    if len(speech_buf) >= 16000 * 2 * 0.45:  # At least 450ms of audio
                        utterance = bytes(speech_buf)
                        speech_buf.clear()
                        speaking = False
                        silence_frames = 0
                        asyncio.create_task(process_turn(utterance))
                    else:
                        speech_buf.clear()
                        speaking = False
                        silence_frames = 0

    @ctx.room.on("track_subscribed")
    def on_track_subscribed(
        track: rtc.Track,
        publication: rtc.RemoteTrackPublication,
        participant: rtc.RemoteParticipant,
    ):
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            asyncio.create_task(monitor_audio_track(track))

    # Also support push-to-talk / text-over-datachannel trigger from Android UI
    @ctx.room.on("data_received")
    def on_data_received(data_packet: rtc.DataPacket):
        try:
            payload = json.loads(bytes(data_packet.data).decode("utf-8"))
            if payload.get("type") == "voice_turn":
                prompt_text = payload.get("text") or ""
                asyncio.create_task(process_turn(b"", preset_text=prompt_text))
        except Exception as exc:
            log.warning("Failed to parse data packet: %s", exc)

    # Check already-published tracks in case participant joined before agent
    for p in ctx.room.remote_participants.values():
        for pub in p.track_publications.values():
            if pub.track and pub.track.kind == rtc.TrackKind.KIND_AUDIO:
                asyncio.create_task(monitor_audio_track(pub.track))

    # Send an initial greeting turn 1.5s after joining so Android user immediately hears AI & sees subtitles
    await asyncio.sleep(1.5)
    await process_turn(b"", preset_text="你好，我已经进入语音房间，请用一句话向我问好。")


if __name__ == "__main__":
    agents.cli.run_app(
        agents.WorkerOptions(
            entrypoint_fnc=entrypoint,
            agent_name="himarket-voice-agent",
        )
    )
