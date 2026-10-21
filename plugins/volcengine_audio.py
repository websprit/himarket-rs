"""Volcengine SAUC, Seed-TTS, and Seeduplex frame builders. No network calls."""

import json
import struct

SAUC_FULL_CLIENT_REQUEST = 0x01
SAUC_AUDIO_ONLY = 0x02
SERIAL_JSON = 0x01


def sauc_frame(message_type: int, payload: bytes) -> bytes:
    header = bytes([
        0x11,  # version 1, header size 1
        message_type,
        (SERIAL_JSON << 4),
        0x00,
    ])
    return header + struct.pack(">I", len(payload)) + payload


def parse_sauc_frame(frame: bytes) -> tuple[int, bytes]:
    if len(frame) < 8:
        raise ValueError("sauc frame too short")
    message_type = frame[1]
    size = struct.unpack(">I", frame[4:8])[0]
    payload = frame[8:8 + size]
    if len(payload) != size:
        raise ValueError("sauc payload length mismatch")
    return message_type, payload


def sauc_start(app_id: str, access_token: str, resource_id: str, *, language: str = "zh-CN") -> bytes:
    payload = {
        "app": {"appid": app_id, "token": access_token},
        "request": {"resource_id": resource_id, "language": language},
        "audio": {"format": "pcm", "rate": 16000, "bits": 16, "channel": 1},
    }
    return sauc_frame(SAUC_FULL_CLIENT_REQUEST, json.dumps(payload, separators=(",", ":")).encode())


def sauc_audio(pcm: bytes) -> bytes:
    return sauc_frame(SAUC_AUDIO_ONLY, pcm)


def parse_sauc_result(payload: bytes) -> str:
    data = json.loads(payload.decode())
    result = data.get("result") or {}
    return (result.get("text") or "").strip()


def tts_request(app_id: str, access_token: str, resource_id: str, text: str, *, voice: str = "") -> dict:
    return {
        "app": {"appid": app_id, "token": access_token},
        "audio": {"voice_type": voice, "encoding": "pcm", "rate": 24000},
        "request": {"resource_id": resource_id, "text": text},
    }


def realtime_start(resource_id: str) -> dict:
    return {
        "event": "StartSession",
        "resource_id": resource_id,
        "audio": {"format": "pcm", "sample_rate": 16000},
    }


def parse_realtime_event(message: dict) -> tuple[str, bytes]:
    text = (message.get("text") or "").strip()
    audio_hex = message.get("audio") or ""
    return text, bytes.fromhex(audio_hex) if audio_hex else b""
