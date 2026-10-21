"""Request builders for the livekit-plugins-google Chirp 3 and Gemini TTS changes."""

import base64
import json


def chirp3_recognize_url(project_id: str, location: str = "global") -> str:
    location = location or "global"
    host = "speech.googleapis.com" if location == "global" else f"{location}-speech.googleapis.com"
    return (
        f"https://{host}/v2/projects/{project_id}/locations/{location}"
        "/recognizers/_:recognize"
    )


def chirp3_request(pcm: bytes, *, language: str = "cmn-Hans-CN", sample_rate: int = 16000) -> dict:
    return {
        "config": {
            "autoDecodingConfig": {},
            "model": "chirp_3",
            "languageCodes": [language],
            "features": {"enableAutomaticPunctuation": True},
            "explicitDecodingConfig": {
                "encoding": "LINEAR16",
                "sampleRateHertz": sample_rate,
                "audioChannelCount": 1,
            },
        },
        "content": base64.b64encode(pcm).decode("ascii"),
    }


def parse_chirp3_response(payload: dict) -> str:
    results = payload.get("results") or []
    if not results:
        return ""
    alternatives = results[0].get("alternatives") or []
    if not alternatives:
        return ""
    return (alternatives[0].get("transcript") or "").strip()


def gemini_tts_url(model: str, base_url: str = "https://generativelanguage.googleapis.com/v1beta") -> str:
    return f"{base_url.rstrip('/')}/models/{model}:generateContent"


def gemini_tts_request(text: str, *, voice: str = "") -> dict:
    body = {
        "contents": [{"role": "user", "parts": [{"text": text}]}],
        "generationConfig": {"responseModalities": ["AUDIO"]},
    }
    if voice:
        body["generationConfig"]["speechConfig"] = {
            "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice}}
        }
    return body


def parse_gemini_tts_response(payload: dict) -> tuple[bytes, int]:
    parts = payload["candidates"][0]["content"]["parts"]
    inline = parts[0]["inlineData"]
    pcm = base64.b64decode(inline["data"])
    rate = int(inline.get("sampleRate") or 24000)
    return pcm, rate


def is_gemini_tts(model: str) -> bool:
    return model.startswith("gemini-") and model.endswith("-tts")


async def post_json(url: str, headers: dict, body: dict) -> dict:
    import httpx

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(url, headers=headers, json=body)
        response.raise_for_status()
        return response.json()
