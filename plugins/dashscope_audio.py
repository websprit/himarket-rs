"""DashScope ASR, TTS, and Omni Realtime message builders. No network calls."""

import json
import uuid


def asr_run_task(model: str, *, language: str = "zh", task_id: str | None = None) -> dict:
    return {
        "header": {
            "action": "run-task",
            "task_id": task_id or uuid.uuid4().hex,
            "streaming": "duplex",
        },
        "payload": {
            "task_group": "audio",
            "task": "asr",
            "function": "recognition",
            "model": model,
            "parameters": {"format": "pcm", "sample_rate": 16000, "language": language},
            "input": {},
        },
    }


def asr_audio_frame(task_id: str, pcm: bytes) -> dict:
    return {
        "header": {"action": "continue-task", "task_id": task_id},
        "payload": {"input": {"audio": pcm.hex()}},
    }


def parse_asr_event(message: dict) -> tuple[str, bool]:
    header = message.get("header") or {}
    payload = message.get("payload") or {}
    output = payload.get("output") or {}
    text = (output.get("sentence") or output.get("text") or "").strip()
    finished = header.get("event") in {"task-finished", "result-generated"} and bool(output.get("sentence_end", header.get("event") == "task-finished"))
    return text, finished


def tts_request(model: str, text: str, *, voice: str = "") -> dict:
    body = {
        "model": model,
        "input": {"text": text},
        "parameters": {"format": "pcm", "sample_rate": 24000},
    }
    if voice:
        body["parameters"]["voice"] = voice
    return body


def parse_tts_response(payload: dict) -> bytes:
    audio = (payload.get("output") or {}).get("audio") or ""
    if not audio:
        raise ValueError("dashscope tts response has no audio")
    return bytes.fromhex(audio)


def realtime_session(model: str) -> dict:
    return {
        "type": "session.update",
        "session": {
            "model": model,
            "modalities": ["text", "audio"],
            "input_audio_format": "pcm16",
            "output_audio_format": "pcm16",
        },
    }


def authorization_header(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}"}


def dumps(message: dict) -> str:
    return json.dumps(message, ensure_ascii=False, separators=(",", ":"))
