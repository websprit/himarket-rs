package com.ugreen.himarket.voice

import android.Manifest
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.media.AudioManager
import android.os.Bundle
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.core.content.ContextCompat
import androidx.lifecycle.lifecycleScope
import io.livekit.android.ConnectOptions
import io.livekit.android.LiveKit
import io.livekit.android.events.RoomEvent
import io.livekit.android.events.collect
import io.livekit.android.room.Room
import io.livekit.android.room.track.DataPublishReliability
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.OutputStreamWriter
import java.net.HttpURLConnection
import java.net.URL

class MainActivity : ComponentActivity() {

    private var room: Room? = null

    private val statusText = mutableStateOf("待连接 (点击下方按钮启动语音会话)")
    private val sessionIdState = mutableStateOf("-")
    private val roomNameState = mutableStateOf("-")
    private val tenantIdState = mutableStateOf("tenant_ugreen_01")
    private val apiKeyIdState = mutableStateOf("ak_live_ugreen_01")
    private val usageSummaryState = mutableStateOf("等待计量数据...")
    private val chatLogs = mutableStateListOf<String>()

    private val permLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { granted ->
        Log.i("HiMarketVoice", "RECORD_AUDIO permission granted=$granted")
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        if (ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO)
            != PackageManager.PERMISSION_GRANTED
        ) {
            permLauncher.launch(Manifest.permission.RECORD_AUDIO)
        }

        setContent {
            MaterialTheme(colorScheme = darkColorScheme()) {
                VoiceIntercomScreen()
            }
        }

        handleIntentCommand(intent)
        // Auto-start session on launch so USB E2E test immediately connects and converses
        lifecycleScope.launch {
            delay(800)
            if (room == null) {
                startVoiceSession("你好！我是通过三星安卓手机连接进来的，请用一句话回答我。")
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        handleIntentCommand(intent)
    }

    private fun handleIntentCommand(intent: Intent?) {
        val prompt = intent?.getStringExtra("voice_prompt")
        if (!prompt.isNullOrBlank()) {
            lifecycleScope.launch {
                if (room == null) {
                    startVoiceSession(prompt)
                } else {
                    sendVoicePromptTurn(prompt)
                }
            }
        }
    }

    private fun appendLog(msg: String) {
        Log.i("HiMarketVoice", msg)
        lifecycleScope.launch(Dispatchers.Main) {
            chatLogs.add(0, msg)
        }
    }

    private fun startVoiceSession(initialPrompt: String? = null) {
        lifecycleScope.launch {
            try {
                statusText.value = "1. 正在向 himarket-rs (:8080) 申请会话与短期 JWT..."
                appendLog("→ POST http://127.0.0.1:8080/api/v1/voice/sessions")

                val sessionResp = withContext(Dispatchers.IO) {
                    requestSessionFromHiMarket()
                }

                val sessionId = sessionResp.getString("session_id")
                val roomName = sessionResp.getString("room_name")
                val wsUrl = sessionResp.getString("livekit_ws_url")
                val jwtToken = sessionResp.getString("participant_token")
                val tenantId = sessionResp.getString("tenant_id")
                val apiKeyId = sessionResp.getString("api_key_id")

                sessionIdState.value = sessionId
                roomNameState.value = roomName
                tenantIdState.value = tenantId
                apiKeyIdState.value = apiKeyId

                appendLog("✓ 获批 Session=$sessionId, Room=$roomName")
                statusText.value = "2. 正在使用短期 JWT 连接 LiveKit SFU ($wsUrl)..."

                room?.disconnect()
                val lkRoom = LiveKit.create(applicationContext)
                room = lkRoom

                // Collect Room events (subtitles, remote audio tracks, participant attributes)
                lifecycleScope.launch {
                    lkRoom.events.collect { event ->
                        when (event) {
                            is RoomEvent.Connected -> {
                                appendLog("✓ 已连接 LiveKit 房间: ${lkRoom.name}")
                                statusText.value = "🟢 实时语音对讲中 (WebRTC 已建立)"
                            }
                            is RoomEvent.TrackSubscribed -> {
                                appendLog("🔊 已订阅 AI 语音音轨: ${event.track.name} (${event.participant.identity})")
                            }
                            is RoomEvent.DataReceived -> {
                                val raw = String(event.data, Charsets.UTF_8)
                                try {
                                    val obj = JSONObject(raw)
                                    val role = obj.optString("role", "ai")
                                    val text = obj.optString("text", raw)
                                    val prefix = if (role == "user") "🧑 我(STT)" else "🤖 AI(DeepSeek)"
                                    appendLog("$prefix: $text")
                                    refreshUsageSummary()
                                } catch (_: Exception) {
                                    appendLog("📩 数据消息: $raw")
                                }
                            }
                            is RoomEvent.Disconnected -> {
                                statusText.value = "⚪ 会话已断开"
                            }
                            else -> {}
                        }
                    }
                }

                val audioManager = getSystemService(Context.AUDIO_SERVICE) as AudioManager
                audioManager.mode = AudioManager.MODE_IN_COMMUNICATION
                audioManager.isSpeakerphoneOn = true
                audioManager.setStreamVolume(
                    AudioManager.STREAM_VOICE_CALL,
                    audioManager.getStreamMaxVolume(AudioManager.STREAM_VOICE_CALL),
                    0,
                )
                audioManager.setStreamVolume(
                    AudioManager.STREAM_MUSIC,
                    (audioManager.getStreamMaxVolume(AudioManager.STREAM_MUSIC) * 4) / 5,
                    0,
                )

                lkRoom.connect(
                    url = wsUrl,
                    token = jwtToken,
                    options = ConnectOptions(autoSubscribe = true)
                )

                if (ContextCompat.checkSelfPermission(
                        this@MainActivity,
                        Manifest.permission.RECORD_AUDIO
                    ) == PackageManager.PERMISSION_GRANTED
                ) {
                    lkRoom.localParticipant.setMicrophoneEnabled(true)
                    appendLog("🎙️ 手机麦克风已开启并发布 Opus 音轨")
                } else {
                    appendLog("⚠️ 麦克风权限未授予，使用 DataChannel 触发语音合成对讲")
                }

                if (!initialPrompt.isNullOrBlank()) {
                    delay(2500)
                    sendVoicePromptTurn(initialPrompt)
                }

                // Periodically refresh usage billing summary
                repeat(6) {
                    delay(3000)
                    refreshUsageSummary()
                }
            } catch (e: Exception) {
                Log.e("HiMarketVoice", "Session error", e)
                statusText.value = "❌ 错误: ${e.message}"
                appendLog("❌ 异常: ${e.message}")
            }
        }
    }

    private fun sendVoicePromptTurn(text: String) {
        lifecycleScope.launch {
            val lkRoom = room ?: return@launch
            appendLog("🗣️ 发起语音对讲提问: $text")
            val payload = JSONObject().apply {
                put("type", "voice_turn")
                put("text", text)
            }.toString().toByteArray(Charsets.UTF_8)
            lkRoom.localParticipant.publishData(payload, DataPublishReliability.RELIABLE)
        }
    }

    private fun refreshUsageSummary() {
        lifecycleScope.launch {
            try {
                val summary = withContext(Dispatchers.IO) {
                    val url = URL(
                        "http://127.0.0.1:8080/api/v1/usage/summary?tenant_id=${tenantIdState.value}&api_key_id=${apiKeyIdState.value}"
                    )
                    val conn = url.openConnection() as HttpURLConnection
                    conn.connectTimeout = 3000
                    conn.readTimeout = 3000
                    val text = conn.inputStream.bufferedReader().readText()
                    JSONObject(text).getJSONObject("data")
                }
                val totalEvents = summary.optInt("total_events", 0)
                val totalUsd = summary.optString("total_cost_usd", "0.000000")
                val voiceObj = summary.optJSONObject("voice_models")
                val llmObj = summary.optJSONObject("text_llm")
                val rtcObj = summary.optJSONObject("rtc")

                val sttMs = voiceObj?.optLong("stt_audio_duration_ms", 0) ?: 0
                val ttsChars = voiceObj?.optLong("tts_characters", 0) ?: 0
                val pTok = llmObj?.optLong("prompt_tokens", 0) ?: 0
                val cTok = llmObj?.optLong("completion_tokens", 0) ?: 0
                val rtcBytes = rtcObj?.optLong("rtc_bytes", 0) ?: 0

                val line =
                    "事件:$totalEvents | 总额:$$totalUsd | STT:${sttMs}ms | LLM:${pTok}+${cTok}tok | TTS:${ttsChars}字 | RTC:${rtcBytes}B"
                usageSummaryState.value = line
                Log.i("HiMarketVoice", "USAGE_SUMMARY: $line")
            } catch (e: Exception) {
                Log.w("HiMarketVoice", "Failed to query usage summary: ${e.message}")
            }
        }
    }

    private fun requestSessionFromHiMarket(): JSONObject {
        val url = URL("http://127.0.0.1:8080/api/v1/voice/sessions")
        val conn = url.openConnection() as HttpURLConnection
        conn.requestMethod = "POST"
        conn.connectTimeout = 5000
        conn.readTimeout = 5000
        conn.doOutput = true
        conn.setRequestProperty("Authorization", "Bearer hm_ak_live_ugreen_2026_voice_key")
        conn.setRequestProperty("Content-Type", "application/json")

        val reqBody = JSONObject().apply {
            put("mode", "cascade")
            put("llm_model", "deepseek-v4.1-flash:1017-cloud")
            put("stt_model", "faster-whisper-local")
            put("tts_voice", "piper-local-zh")
        }.toString()

        OutputStreamWriter(conn.outputStream, Charsets.UTF_8).use { it.write(reqBody) }
        val respText = conn.inputStream.bufferedReader().readText()
        return JSONObject(respText).getJSONObject("data")
    }

    @Composable
    private fun VoiceIntercomScreen() {
        Column(
            modifier = Modifier
                .fillMaxSize()
                .background(Color(0xFF0B0F19))
                .padding(16.dp)
        ) {
            Text(
                text = "🎙️ HiMarket-RS + LiveKit 语音 AI 对讲终端",
                color = Color(0xFF38BDF8),
                fontSize = 19.sp,
                fontWeight = FontWeight.Bold
            )
            Spacer(modifier = Modifier.height(6.dp))
            Text(
                text = statusText.value,
                color = Color(0xFF34D399),
                fontSize = 13.5.sp,
                fontWeight = FontWeight.SemiBold
            )
            Spacer(modifier = Modifier.height(10.dp))

            Surface(
                color = Color(0xFF131B2E),
                shape = RoundedCornerShape(10.dp),
                modifier = Modifier.fillMaxWidth()
            ) {
                Column(modifier = Modifier.padding(12.dp)) {
                    Text(
                        text = "租户: ${tenantIdState.value}  |  Key: ${apiKeyIdState.value}",
                        color = Color(0xFFCBD5E1),
                        fontSize = 12.sp,
                        fontFamily = FontFamily.Monospace
                    )
                    Text(
                        text = "房间: ${roomNameState.value}",
                        color = Color(0xFF94A3B8),
                        fontSize = 11.5.sp,
                        fontFamily = FontFamily.Monospace
                    )
                    Spacer(modifier = Modifier.height(4.dp))
                    Text(
                        text = "📊 实时计量: ${usageSummaryState.value}",
                        color = Color(0xFFFBBF24),
                        fontSize = 12.sp,
                        fontFamily = FontFamily.Monospace
                    )
                }
            }

            Spacer(modifier = Modifier.height(10.dp))

            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Button(
                    onClick = { startVoiceSession("你好，请用一句话介绍HiMarket语音平台。") },
                    modifier = Modifier.weight(1f)
                ) {
                    Text("重连/新建会话", fontSize = 12.5.sp)
                }
                Button(
                    onClick = { sendVoicePromptTurn("请问今天深圳的天气适合出门吗？请简短回答。") },
                    modifier = Modifier.weight(1f)
                ) {
                    Text("发送语音追问", fontSize = 12.5.sp)
                }
                Button(
                    onClick = {
                        room?.disconnect()
                        room = null
                        lifecycleScope.launch {
                            delay(1200)
                            refreshUsageSummary()
                        }
                    },
                    colors = ButtonDefaults.buttonColors(containerColor = Color(0xFFE11D48))
                ) {
                    Text("挂断结算", fontSize = 12.5.sp)
                }
            }

            Spacer(modifier = Modifier.height(10.dp))
            Text(
                text = "实时对话字幕与信令日志 (STT → DeepSeek-V4.1 → TTS):",
                color = Color(0xFF94A3B8),
                fontSize = 12.sp
            )
            Spacer(modifier = Modifier.height(6.dp))

            LazyColumn(
                modifier = Modifier
                    .fillMaxWidth()
                    .weight(1f)
                    .background(Color(0xFF0F172A), RoundedCornerShape(10.dp))
                    .padding(10.dp),
                verticalArrangement = Arrangement.spacedBy(6.dp)
            ) {
                items(chatLogs) { line ->
                    Text(
                        text = line,
                        color = Color(0xFFE2E8F0),
                        fontSize = 12.5.sp,
                        fontFamily = FontFamily.Monospace
                    )
                }
            }
        }
    }
}
