import base64
import json
import unittest

import dashscope_audio
import google_audio
import vendor_dispatch
import volcengine_audio


class GoogleAudioTest(unittest.TestCase):
    def test_chirp3_request_uses_v2_model(self):
        url = google_audio.chirp3_recognize_url("demo-project", "us-central1")
        self.assertIn("/v2/projects/demo-project/locations/us-central1/", url)
        body = google_audio.chirp3_request(b"\x01\x02")
        self.assertEqual(body["config"]["model"], "chirp_3")
        self.assertEqual(google_audio.parse_chirp3_response({"results": [{"alternatives": [{"transcript": "你好"}]}]}), "你好")

    def test_gemini_tts_round_trip_parser(self):
        self.assertTrue(google_audio.is_gemini_tts("gemini-3.8-flash-tts"))
        pcm = b"\x00\x01\x02\x03"
        payload = {"candidates": [{"content": {"parts": [{"inlineData": {"data": base64.b64encode(pcm).decode(), "sampleRate": 24000}}]}}]}
        parsed, rate = google_audio.parse_gemini_tts_response(payload)
        self.assertEqual(parsed, pcm)
        self.assertEqual(rate, 24000)
        self.assertEqual(google_audio.gemini_tts_request("你好")["generationConfig"]["responseModalities"], ["AUDIO"])


class DashScopeTest(unittest.TestCase):
    def test_asr_and_tts_messages(self):
        task = dashscope_audio.asr_run_task("qwen-audio-3.1-asr-flash-streaming", task_id="task-1")
        self.assertEqual(task["payload"]["model"], "qwen-audio-3.1-asr-flash-streaming")
        frame = dashscope_audio.asr_audio_frame("task-1", b"\x10\x20")
        self.assertEqual(frame["header"]["task_id"], "task-1")
        text, done = dashscope_audio.parse_asr_event({"header": {"event": "task-finished"}, "payload": {"output": {"sentence": "测试", "sentence_end": True}}})
        self.assertEqual(text, "测试")
        self.assertTrue(done)
        request = dashscope_audio.tts_request("qwen-audio-3.1-tts-flash", "你好", voice="Cherry")
        self.assertEqual(request["parameters"]["voice"], "Cherry")
        self.assertEqual(dashscope_audio.parse_tts_response({"output": {"audio": b"abc".hex()}}), b"abc")

    def test_realtime_session_names_model(self):
        session = dashscope_audio.realtime_session("qwen3.8-omni-flash-realtime")
        self.assertEqual(session["session"]["model"], "qwen3.8-omni-flash-realtime")


class VolcengineTest(unittest.TestCase):
    def test_sauc_frame_round_trip(self):
        start = volcengine_audio.sauc_start("app", "token", "volc.seedasr.sauc.duration")
        message_type, payload = volcengine_audio.parse_sauc_frame(start)
        self.assertEqual(message_type, volcengine_audio.SAUC_FULL_CLIENT_REQUEST)
        body = json.loads(payload)
        self.assertEqual(body["request"]["resource_id"], "volc.seedasr.sauc.duration")
        audio = volcengine_audio.sauc_audio(b"pcm")
        self.assertEqual(volcengine_audio.parse_sauc_frame(audio)[1], b"pcm")
        self.assertEqual(volcengine_audio.parse_sauc_result('{"result":{"text":"北京"}}'.encode()), "北京")

    def test_tts_and_realtime_payloads(self):
        request = volcengine_audio.tts_request("app", "token", "seed-tts-2.0", "你好", voice="zh_female")
        self.assertEqual(request["audio"]["voice_type"], "zh_female")
        event = volcengine_audio.realtime_start("seeduplex")
        self.assertEqual(event["event"], "StartSession")
        text, audio = volcengine_audio.parse_realtime_event({"text": "嗯", "audio": b"xy".hex()})
        self.assertEqual((text, audio), ("嗯", b"xy"))


class DispatchTest(unittest.TestCase):
    def test_unknown_protocol_is_rejected(self):
        with self.assertRaises(vendor_dispatch.UnsupportedProtocol):
            vendor_dispatch.transcription_request({"protocol": "missing"}, b"")

    def test_vendor_bindings_build_requests(self):
        google = vendor_dispatch.transcription_request({"protocol": "google_stt", "api_secret": "proj", "api_key": "k", "voice": "global"}, b"\x00\x00")
        self.assertIn("chirp_3", json.dumps(google["json"]))
        dash = vendor_dispatch.synthesis_request({"protocol": "dashscope_tts", "model": "qwen-audio-3.1-tts-flash"}, "你好")
        self.assertEqual(dash["json"]["model"], "qwen-audio-3.1-tts-flash")
        volc = vendor_dispatch.realtime_session({"protocol": "volcengine_realtime", "model": "seeduplex"})
        self.assertEqual(volc["resource_id"], "seeduplex")
        azure = vendor_dispatch.synthesis_request({"protocol": "azure_tts", "voice": "MAI-Voice-2.1", "base_url": "https://eastus.tts.speech.microsoft.com"}, "你好")
        self.assertEqual(azure["voice"], "MAI-Voice-2.1")


if __name__ == "__main__":
    unittest.main()
