# Kick TTS

چت‌های کیک رو به صورت صوتی پخش می‌کنه.

## اجرای لوکال

```bash
pip install -r requirements.txt
python server.py
```

بعد:
- داشبورد: http://localhost:5000
- پلیر موبایل: http://localhost:5000/player

## تنظیمات

API Key رو توی داشبورد وارد کن یا به عنوان environment variable بذار:

```
ELEVENLABS_API_KEY=sk_...
VOICE_ID=pNInz6obpgDQGcFmaJgB
CHATROOM_ID=53220552
```

## Deploy روی Railway

1. این پوشه رو به GitHub push کن
2. توی Railway یه پروژه جدید بساز و ریپو رو وصل کن
3. Environment variables رو اضافه کن
4. Deploy!

لینک پلیر موبایل رو از داشبورد بگیر و روی موبایل باز کن.
