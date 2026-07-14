import asyncio
import json
import os
import queue
import threading
import time
from datetime import datetime

import requests
import websocket
from flask import Flask, jsonify, render_template_string, request, send_file
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.after_request
def add_headers(response):
    response.headers["ngrok-skip-browser-warning"] = "true"
    return response

# ── تنظیمات (از environment variables یا مستقیم) ──────────────────────────
CONFIG = {
    "elevenlabs_api_key": os.environ.get("ELEVENLABS_API_KEY", ""),
    "voice_id": os.environ.get("VOICE_ID", "pNInz6obpgDQGcFmaJgB"),
    "chatroom_id": os.environ.get("CHATROOM_ID", "53220552"),
    "name_format": os.environ.get("NAME_FORMAT", "{name} گفت: {text}"),
    "max_len": int(os.environ.get("MAX_LEN", "120")),
    "speed": float(os.environ.get("SPEED", "1.0")),
    "filter_commands": True,
    "filter_links": True,
    "filter_emoji_only": True,
    "blocked_words": [],
}

# ── وضعیت سرور ───────────────────────────────────────────────────────────
STATE = {
    "connected": False,
    "playing": False,
    "current_text": "",
    "queue": [],          # لیست متن‌های آماده پخش
    "audio_ready": None,  # مسیر فایل صوتی آماده
    "stats": {"total": 0, "played": 0, "skipped": 0, "queued": 0},
    "log": [],            # آخرین چت‌ها
}

audio_queue = queue.Queue()
ws_instance = None


# ── فیلترها ──────────────────────────────────────────────────────────────
def should_skip(text):
    t = text.strip()
    if CONFIG["filter_commands"] and t.startswith("!"):
        return True
    if CONFIG["filter_links"] and ("http://" in t or "https://" in t):
        return True
    if CONFIG["filter_emoji_only"]:
        import re
        if re.match(r'^[\U0001F300-\U0001FFFF\U00002600-\U000027BF\s]+$', t):
            return True
    for w in CONFIG["blocked_words"]:
        if w and w.lower() in t.lower():
            return True
    return False


def truncate(text):
    if len(text) > CONFIG["max_len"]:
        return text[:CONFIG["max_len"]] + "..."
    return text


def build_tts_text(user, text):
    fmt = CONFIG["name_format"]
    return fmt.replace("{name}", user).replace("{text}", text)


# ── ElevenLabs TTS ────────────────────────────────────────────────────────
def fetch_audio(tts_text):
    api_key = CONFIG["elevenlabs_api_key"]
    voice_id = CONFIG["voice_id"]
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    headers = {"xi-api-key": api_key, "Content-Type": "application/json"}
    body = {
        "text": tts_text,
        "model_id": "eleven_multilingual_v2",
        "voice_settings": {
            "stability": 0.5,
            "similarity_boost": 0.75,
            "speed": CONFIG["speed"],
        },
    }
    resp = requests.post(url, headers=headers, json=body, timeout=15)
    if resp.status_code != 200:
        raise Exception(f"ElevenLabs error {resp.status_code}: {resp.text[:200]}")
    return resp.content  # bytes


# ── پردازش صف صدا ─────────────────────────────────────────────────────────
AUDIO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tts_current.mp3")


def audio_worker():
    """در یه thread جداگانه چرخه می‌زنه و صف رو پردازش می‌کنه"""
    while True:
        try:
            item = audio_queue.get(timeout=1)
        except queue.Empty:
            continue

        tts_text = item["tts_text"]
        display = item["display"]

        STATE["playing"] = True
        STATE["current_text"] = display
        STATE["audio_ready"] = None

        try:
            audio_bytes = fetch_audio(tts_text)
            with open(AUDIO_PATH, "wb") as f:
                f.write(audio_bytes)
            STATE["audio_ready"] = AUDIO_PATH
            STATE["stats"]["played"] += 1
            log_entry("played", item["user"], item["text"])
        except Exception as e:
            print(f"[TTS error] {e}")
            log_entry("error", item["user"], str(e))

        # صبر می‌کنیم موبایل صدا رو بگیره و پخش کنه
        # (موبایل بعد از گرفتن صدا، /audio/done رو صدا می‌زنه)
        timeout = time.time() + 30  # حداکثر 30 ثانیه صبر
        while STATE["audio_ready"] is not None and time.time() < timeout:
            time.sleep(0.2)

        STATE["playing"] = False
        STATE["current_text"] = ""
        STATE["stats"]["queued"] = audio_queue.qsize()
        audio_queue.task_done()


# ── لاگ چت ────────────────────────────────────────────────────────────────
def log_entry(state, user, text):
    STATE["log"].append({
        "state": state,
        "user": user,
        "text": text,
        "time": datetime.now().strftime("%H:%M:%S"),
    })
    if len(STATE["log"]) > 50:
        STATE["log"] = STATE["log"][-50:]


# ── WebSocket کیک ─────────────────────────────────────────────────────────
PUSHER_KEY = "32cbd69e4b950bf97679"


def on_ws_message(ws, message):
    try:
        msg = json.loads(message)
        event = msg.get("event", "")

        if event == "pusher:ping":
            ws.send(json.dumps({"event": "pusher:pong", "data": {}}))
            return

        if event == "App\\Events\\ChatMessageEvent":
            data = json.loads(msg.get("data", "{}"))
            user = (data.get("sender") or {}).get("username", "ناشناس")
            text = data.get("content", "").strip()
            if not text:
                return

            STATE["stats"]["total"] += 1

            if should_skip(text):
                STATE["stats"]["skipped"] += 1
                log_entry("skipped", user, text)
                return

            final_text = truncate(text)
            tts_text = build_tts_text(user, final_text)
            audio_queue.put({"tts_text": tts_text, "display": f"{user}: {final_text}", "user": user, "text": final_text})
            STATE["stats"]["queued"] = audio_queue.qsize()
            log_entry("queued", user, final_text)

    except Exception as e:
        print(f"[WS message error] {e}")


def on_ws_open(ws):
    chatroom_id = CONFIG["chatroom_id"]
    channel = f"chatrooms.{chatroom_id}.v2"
    ws.send(json.dumps({"event": "pusher:subscribe", "data": {"auth": "", "channel": channel}}))
    STATE["connected"] = True
    print(f"[WS] متصل به chatroom {chatroom_id}")


def on_ws_close(ws, code, msg):
    STATE["connected"] = False
    print(f"[WS] قطع شد ({code}). تلاش مجدد در ۵ ثانیه...")
    time.sleep(5)
    start_ws()


def on_ws_error(ws, error):
    print(f"[WS error] {error}")


def start_ws():
    global ws_instance
    chatroom_id = CONFIG["chatroom_id"]
    url = f"wss://ws-us2.pusher.com/app/{PUSHER_KEY}?protocol=7&client=js&version=7.6.0&flash=false"
    ws_instance = websocket.WebSocketApp(
        url,
        on_open=on_ws_open,
        on_message=on_ws_message,
        on_close=on_ws_close,
        on_error=on_ws_error,
    )
    thread = threading.Thread(target=ws_instance.run_forever, daemon=True)
    thread.start()


# ── Auth ─────────────────────────────────────────────────────────────────
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "kick123")

def check_auth():
    token = request.cookies.get("auth") or request.headers.get("X-Auth")
    return token == DASHBOARD_PASSWORD

def auth_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not check_auth():
            return jsonify({"error": "unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pwd = (request.json or {}).get("password", "")
        if pwd == DASHBOARD_PASSWORD:
            from flask import make_response
            resp = make_response(jsonify({"ok": True}))
            resp.set_cookie("auth", pwd, max_age=60*60*24*30)
            return resp
        return jsonify({"error": "wrong password"}), 403
    return render_template_string(LOGIN_HTML)

# ── API Routes ────────────────────────────────────────────────────────────
@app.route("/status")
def status():
    return jsonify({
        "connected": STATE["connected"],
        "playing": STATE["playing"],
        "current_text": STATE["current_text"],
        "audio_ready": STATE["audio_ready"] is not None,
        "stats": STATE["stats"],
        "queue_size": audio_queue.qsize(),
        "log": STATE["log"][-10:],
    })


@app.route("/audio/current")
def audio_current():
    """موبایل این endpoint رو poll می‌کنه — اگه صدا آماده بود برمیگردونه"""
    if STATE["audio_ready"] and os.path.exists(AUDIO_PATH):
        return send_file(AUDIO_PATH, mimetype="audio/mpeg")
    return jsonify({"ready": False}), 204


@app.route("/audio/done", methods=["POST"])
def audio_done():
    """موبایل بعد از پخش صدا این رو صدا می‌زنه"""
    STATE["audio_ready"] = None
    return jsonify({"ok": True})


@app.route("/config", methods=["GET", "POST"])
@auth_required
def config_route():
    if request.method == "POST":
        data = request.json or {}
        for k in ["elevenlabs_api_key", "voice_id", "chatroom_id", "name_format", "speed", "max_len"]:
            if k in data:
                CONFIG[k] = data[k]
        if "blocked_words" in data:
            CONFIG["blocked_words"] = [w.strip() for w in data["blocked_words"].split(",") if w.strip()]
        return jsonify({"ok": True})
    safe = {k: v for k, v in CONFIG.items() if k != "elevenlabs_api_key"}
    safe["has_api_key"] = bool(CONFIG["elevenlabs_api_key"])
    return jsonify(safe)


@app.route("/queue/clear", methods=["POST"])
@auth_required
def clear_queue():
    while not audio_queue.empty():
        try:
            audio_queue.get_nowait()
            audio_queue.task_done()
        except:
            break
    STATE["audio_ready"] = None
    return jsonify({"ok": True})


@app.route("/")
def index():
    if not check_auth():
        from flask import redirect
        return redirect("/login")
    return render_template_string(DASHBOARD_HTML)


@app.route("/player")
def player():
    return render_template_string(PLAYER_HTML)


# ── لاگین HTML ───────────────────────────────────────────────────────────
LOGIN_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ورود</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: Tahoma, Arial, sans-serif; background: #0f0f0f; color: #eee;
       display: flex; align-items: center; justify-content: center; min-height: 100vh; }
.box { background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 12px; padding: 24px; width: 280px; }
h2 { font-size: 16px; margin-bottom: 16px; text-align: center; }
input { width: 100%; padding: 10px; border-radius: 8px; border: 1px solid #333;
        background: #252525; color: #eee; font-size: 14px; margin-bottom: 10px; direction: ltr; }
button { width: 100%; padding: 10px; border-radius: 8px; border: 1px solid #53fc18;
         background: #1a4a1a; color: #53fc18; font-size: 14px; cursor: pointer; font-weight: bold; }
.err { color: #ff5555; font-size: 12px; margin-top: 8px; text-align: center; display: none; }
</style>
</head>
<body>
<div class="box">
  <h2>🎙 Kick TTS</h2>
  <input type="password" id="pwd" placeholder="پسورد" onkeydown="if(event.key==='Enter')login()">
  <button onclick="login()">ورود</button>
  <div class="err" id="err">پسورد اشتباه است</div>
</div>
<script>
async function login() {
  const pwd = document.getElementById('pwd').value;
  const r = await fetch('/login', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({password: pwd}) });
  if (r.ok) { location.href = '/'; }
  else { document.getElementById('err').style.display = 'block'; }
}
</script>
</body>
</html>"""

# ── داشبورد HTML ──────────────────────────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Kick TTS Dashboard</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: Tahoma, Arial, sans-serif; background: #0f0f0f; color: #eee; padding: 12px; font-size: 14px; }
h1 { font-size: 16px; color: #fff; margin-bottom: 12px; text-align: center; }
.card { background: #1a1a1a; border-radius: 10px; padding: 12px; margin-bottom: 10px; border: 1px solid #2a2a2a; }
.card-title { font-size: 12px; color: #888; margin-bottom: 8px; }
.row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; gap: 8px; }
label { font-size: 13px; color: #aaa; display: block; margin-top: 8px; }
input[type=text], input[type=password], select {
  width: 100%; padding: 8px 10px; border-radius: 8px;
  border: 1px solid #333; background: #252525; color: #eee;
  font-size: 13px; margin-top: 4px; direction: ltr; font-family: Tahoma, Arial, sans-serif;
}
.badge { padding: 2px 8px; border-radius: 6px; font-size: 11px; white-space: nowrap; }
.green { background: #1a3a1a; color: #53fc18; }
.red { background: #3a1a1a; color: #ff5555; }
.yellow { background: #3a3000; color: #ffcc00; }
.gray { background: #2a2a2a; color: #888; }
button { padding: 10px; border-radius: 8px; border: none; font-size: 14px; cursor: pointer; font-weight: bold; font-family: Tahoma, Arial, sans-serif; }
.btn-green { background: #1a4a1a; color: #53fc18; border: 1px solid #53fc18; width: 100%; margin-top: 6px; }
.btn-gray { background: #2a2a2a; color: #aaa; border: 1px solid #444; width: 100%; margin-top: 4px; }
.stat { text-align: center; }
.stat-val { font-size: 20px; font-weight: bold; color: #53fc18; }
.stat-label { font-size: 11px; color: #666; margin-top: 2px; }
.chat-log { height: 180px; overflow-y: auto; background: #111; border-radius: 8px; padding: 8px; border: 1px solid #2a2a2a; }
.msg { padding: 3px 6px; border-radius: 4px; font-size: 12px; margin-bottom: 3px; }
.msg.queued { background: #1e1e1e; }
.msg.played { background: #0d2a0d; color: #53fc18; }
.msg.skipped { background: #151515; color: #555; text-decoration: line-through; }
.msg.error { background: #2a0d0d; color: #ff5555; }
.player-link { background: #1a2a3a; border: 1px solid #3a6a9a; border-radius: 8px; padding: 10px; text-align: center; margin-top: 6px; }
.player-link a { color: #53a8fc; font-size: 13px; }
</style>
</head>
<body>
<h1>🎙 Kick TTS — داشبورد</h1>

<div class="card">
  <div class="row">
    <span class="card-title" style="margin:0">اتصال Kick:</span>
    <span class="badge gray" id="ws-badge">بررسی...</span>
  </div>
  <div class="row">
    <span class="card-title" style="margin:0">وضعیت TTS:</span>
    <span class="badge gray" id="tts-badge">—</span>
  </div>
  <div id="now-playing" style="display:none; margin-top:6px; font-size:12px; color:#53fc18; direction:rtl"></div>
</div>

<div class="card">
  <div class="row">
    <div class="stat"><div class="stat-val" id="s-total">0</div><div class="stat-label">دریافتی</div></div>
    <div class="stat"><div class="stat-val" id="s-queue">0</div><div class="stat-label">صف</div></div>
    <div class="stat"><div class="stat-val" id="s-played">0</div><div class="stat-label">پخش</div></div>
    <div class="stat"><div class="stat-val" id="s-skipped">0</div><div class="stat-label">رد شده</div></div>
  </div>
</div>

<div class="card">
  <div class="card-title">تنظیمات</div>
  <label>ElevenLabs API Key:</label>
  <input type="password" id="cfg-apikey" placeholder="sk_...">
  <label>Voice ID:</label>
  <input type="text" id="cfg-voice" value="pNInz6obpgDQGcFmaJgB">
  <label>Chatroom ID:</label>
  <input type="text" id="cfg-chatroom" value="53220552">
  <label>فرمت اسم:</label>
  <select id="cfg-format">
    <option value="{name} گفت: {text}">{name} گفت: {text}</option>
    <option value="{name}: {text}">{name}: {text}</option>
    <option value="{text}">بدون اسم</option>
  </select>
  <label>کلمات مسدود:</label>
  <input type="text" id="cfg-blocked" placeholder="spam, تبلیغ">
  <button class="btn-green" onclick="saveConfig()">💾 ذخیره تنظیمات</button>
  <button class="btn-gray" onclick="clearQueue()">🗑 پاک کردن صف</button>
</div>

<div class="card">
  <div class="card-title">لینک موبایل (پخش صدا)</div>
  <div class="player-link">
    <a id="player-link" href="/player" target="_blank">/player</a>
    <div style="font-size:11px; color:#666; margin-top:4px">این صفحه رو روی موبایل باز کن</div>
  </div>
</div>

<div class="card">
  <div class="card-title">لاگ چت‌ها</div>
  <div class="chat-log" id="chat-log"></div>
</div>

<script>
function $(id) { return document.getElementById(id); }

async function poll() {
  try {
    const r = await fetch('/status');
    const d = await r.json();
    $('ws-badge').textContent = d.connected ? 'متصل ✓' : 'قطع';
    $('ws-badge').className = 'badge ' + (d.connected ? 'green' : 'red');
    $('tts-badge').textContent = d.playing ? '▶ پخش' : 'آماده';
    $('tts-badge').className = 'badge ' + (d.playing ? 'green' : 'gray');
    const np = $('now-playing');
    if (d.current_text) { np.style.display='block'; np.textContent = '▶ ' + d.current_text; }
    else { np.style.display='none'; }
    $('s-total').textContent = d.stats.total;
    $('s-queue').textContent = d.queue_size;
    $('s-played').textContent = d.stats.played;
    $('s-skipped').textContent = d.stats.skipped;

    const log = $('chat-log');
    log.innerHTML = (d.log || []).reverse().map(e =>
      '<div class="msg ' + e.state + '">' +
      '<span style="color:#666;font-size:10px">' + e.time + '</span> ' +
      '<b style="color:#53fc18">' + e.user + '</b>: ' + e.text + '</div>'
    ).join('');
  } catch(e) {}
  setTimeout(poll, 1500);
}

async function saveConfig() {
  const body = {
    elevenlabs_api_key: $('cfg-apikey').value,
    voice_id: $('cfg-voice').value,
    chatroom_id: $('cfg-chatroom').value,
    name_format: $('cfg-format').value,
    blocked_words: $('cfg-blocked').value,
  };
  await fetch('/config', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
  alert('ذخیره شد ✓');
}

async function clearQueue() {
  await fetch('/queue/clear', { method: 'POST' });
}

// set player link to current host
$('player-link').href = location.origin + '/player';
$('player-link').textContent = location.origin + '/player';

poll();
</script>
</body>
</html>"""


# ── صفحه موبایل (Player) ──────────────────────────────────────────────────
PLAYER_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TTS Player</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: Tahoma, Arial, sans-serif; background: #0f0f0f; color: #eee;
       display: flex; flex-direction: column; align-items: center; justify-content: center;
       min-height: 100vh; padding: 20px; text-align: center; user-select: none; }
.icon { font-size: 56px; margin-bottom: 16px; transition: all 0.3s; }
.text { font-size: 15px; color: #53fc18; margin-bottom: 8px; min-height: 22px; direction: rtl; line-height: 1.5; max-width: 300px; }
.badge { padding: 4px 14px; border-radius: 8px; font-size: 13px; margin-top: 10px; display: inline-block; }
.green { background: #1a3a1a; color: #53fc18; }
.gray  { background: #2a2a2a; color: #888; }
.red   { background: #3a1a1a; color: #ff5555; }
.err { font-size: 11px; color: #ff5555; margin-top: 8px; max-width: 280px; }
</style>
</head>
<body>
<div class="icon" id="icon">🔇</div>
<div class="text" id="text">در انتظار چت...</div>
<div class="badge gray" id="badge">متصل در حال اجرا</div>
<div class="err" id="err"></div>

<script>
const SERVER = location.origin;
let busy = false;

async function checkAndPlay() {
  if (busy) return;
  busy = true;
  try {
    const r = await fetch(SERVER + '/audio/current', { cache: 'no-store' });

    if (r.status === 200) {
      // صدا آماده‌ست
      const blob = await r.blob();
      const url = URL.createObjectURL(blob);

      // دریافت متن
      try {
        const st = await fetch(SERVER + '/status', { cache: 'no-store' });
        const sd = await st.json();
        document.getElementById('text').textContent = sd.current_text || '';
      } catch(_) {}

      document.getElementById('icon').textContent = '🔊';
      document.getElementById('badge').className = 'badge green';
      document.getElementById('badge').textContent = '▶ در حال پخش';
      document.getElementById('err').textContent = '';

      const audio = new Audio(url);
      audio.volume = 1.0;

      await new Promise((res) => {
        audio.onended = res;
        audio.onerror = res; // حتی اگه خطا داد ادامه بده
        // autoplay بدون نیاز به کلیک (WebView)
        const p = audio.play();
        if (p && p.catch) p.catch(() => res());
      });

      URL.revokeObjectURL(url);

      // به سرور بگو پخش تموم شد
      await fetch(SERVER + '/audio/done', { method: 'POST' });

      document.getElementById('icon').textContent = '🔇';
      document.getElementById('badge').className = 'badge gray';
      document.getElementById('badge').textContent = 'آماده';
      document.getElementById('text').textContent = 'در انتظار چت...';
    }
    // 204 = صدایی نیست، فقط صبر می‌کنیم
  } catch(e) {
    document.getElementById('err').textContent = e.message;
    document.getElementById('badge').className = 'badge red';
    document.getElementById('badge').textContent = 'خطا - تلاش مجدد...';
    await new Promise(r => setTimeout(r, 2000));
    document.getElementById('badge').className = 'badge gray';
    document.getElementById('badge').textContent = 'آماده';
  }
  busy = false;
}

// شروع اتوماتیک بدون نیاز به کلیک
setInterval(checkAndPlay, 800);
</script>
</body>
</html>"""


# ── شروع سرور ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🎙 Kick TTS Server starting...")

    # شروع worker thread برای TTS
    worker = threading.Thread(target=audio_worker, daemon=True)
    worker.start()

    # شروع WebSocket به Kick
    start_ws()

    port = int(os.environ.get("PORT", 5000))
    print(f"✓ Dashboard: http://localhost:{port}")
    print(f"✓ Player:    http://localhost:{port}/player")
    app.run(host="0.0.0.0", port=port, debug=False)
