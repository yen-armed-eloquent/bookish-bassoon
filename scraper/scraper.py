import os, sys, json, asyncio, random, re, subprocess, shutil
from pathlib import Path
from datetime import datetime
import httpx
from loguru import logger

# ─── CONFIGURATION ───────────────────────────────────────────────
NODE_INDEX  = int(os.environ.get("NODE_INDEX", "0"))
TOTAL_NODES = int(os.environ.get("TOTAL_NODES", "1"))
LINKS_FILE  = os.environ.get("LINKS_FILE", "/data/links.txt")
OUTPUT_DIR  = os.environ.get("OUTPUT_DIR", "/data/output")
RCLONE_REMOTE = "vfx"

logger.remove()
logger.add(sys.stdout,
    format="{time:HH:mm:ss} | {level:<8} | [Node " + str(NODE_INDEX) + "/" + str(TOTAL_NODES) + "] | {message}")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://www.tiktok.com/"
}

# ─── HELPERS ─────────────────────────────────────────────────────

def clean_name(text):
    """Folder aur file names se fultu characters hatane ke liye"""
    if not text: return "no_desc"
    text = re.sub(r'[\/*?:"<>|#@]', "", str(text))
    text = text.replace("\n", " ").strip()
    return text[:50]

def upload_to_mega(local_path, folder_name):
    """Rclone upload with Read-only config fix"""
    original_config = "/root/.config/rclone/rclone.conf"
    writable_config = "/tmp/rclone.conf"
    
    try:
        # Config file ko writable /tmp folder mein copy karna zaroori hai
        if os.path.exists(original_config):
            shutil.copy2(original_config, writable_config)
        
        remote_path = f"{RCLONE_REMOTE}:/tiktok_data/{folder_name}"
        logger.info(f" 📦 Uploading to Mega: {folder_name}")
        
        cmd = [
            "rclone", "copy", 
            str(local_path), 
            remote_path,
            "--config", writable_config,
            "--transfers", "12", # Mega stability ke liye 12 rakha hai
            "--checkers", "24",
            "--no-check-dest",
            "-P"
        ]
        
        subprocess.run(cmd, check=True)
        logger.success(f" ✅ Uploaded & Synced: {folder_name}")
        
        # Upload success hone ke baad local delete karein
        if os.path.exists(local_path):
            shutil.rmtree(local_path)
            logger.debug(f" 🗑️ Local cleanup done: {folder_name}")
            
    except Exception as e:
        logger.error(f" ❌ Rclone Upload Failed: {e}")

async def download_file(client, url, path):
    try:
        r = await client.get(url, headers=HEADERS, timeout=60, follow_redirects=True)
        r.raise_for_status()
        Path(path).write_bytes(r.content)
        return True
    except Exception as e:
        logger.error(f"  Download failed: {e}")
        return False

async def get_meta(client, url):
    clean_url = url.replace("/photo/", "/video/")
    try:
        r = await client.get(clean_url, headers=HEADERS, follow_redirects=True, timeout=30)
        match = re.search(r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">([\s\S]*?)</script>', r.text)
        if not match: return None
        data = json.loads(match.group(1))
        scope = data.get("__DEFAULT_SCOPE__", {})
        item = scope.get("webapp.video-detail", {}).get("itemInfo", {}).get("itemStruct")
        if not item:
            item = scope.get("webapp.image-detail", {}).get("itemInfo", {}).get("itemStruct")
        return item
    except Exception as e:
        logger.error(f"  Meta fetch error: {e}")
        return None

async def fetch_comments(client, video_id, folder, file_prefix, limit=500):
    raw, clean, cursor, page = [], [], 0, 1
    while len(raw) < limit:
        try:
            r = await client.get("https://www.tiktok.com/api/comment/list/", 
                               params={"aweme_id": video_id, "cursor": cursor, "count": 50, "aid": "1988"}, 
                               headers=HEADERS, timeout=20)
            if r.status_code != 200: break
            data = r.json()
            batch = data.get("comments") or []
            if not batch: break
            raw.extend(batch)
            for c in batch:
                clean.append({
                    "cid": c.get("cid"), 
                    "text": c.get("text"), 
                    "likes": c.get("digg_count"), 
                    "user": {"username": c.get("user", {}).get("unique_id")}
                })
            if not data.get("has_more"): break
            cursor = data.get("cursor", cursor + len(batch))
            page += 1
            await asyncio.sleep(random.uniform(0.5, 1.0))
        except: break
    if raw:
        (folder / f"RAW__comments__{file_prefix}.json").write_text(json.dumps(raw, indent=2, ensure_ascii=False))
        (folder / f"comments__{file_prefix}.json").write_text(json.dumps(clean, indent=2, ensure_ascii=False))

async def scrape_one(client, url, idx, total):
    logger.info(f"[{idx}/{total}] -> {url}")
    item = await get_meta(client, url)
    if not item: return

    v_id = item["id"]
    author = item.get("author", {}).get("uniqueId", "unknown")
    desc = clean_name(item.get("desc", "no_desc"))
    ctime = int(item.get("createTime", 0))
    date_str = datetime.fromtimestamp(ctime).strftime('%Y-%m-%d') if ctime else "unknown-date"

    file_prefix = f"{author}__{desc}__{date_str}__{v_id}"
    folder = Path(OUTPUT_DIR) / file_prefix
    folder.mkdir(parents=True, exist_ok=True)

    # ─── SAVE CAPTION & META ───
    (folder / f"RAW__meta__{file_prefix}.json").write_text(json.dumps(item, indent=2, ensure_ascii=False))
    
    caption_data = {
        "post_url": url, 
        "caption": item.get("desc", ""), 
        "date": date_str, 
        "video_id": v_id, 
        "author": author
    }
    (folder / f"caption__{file_prefix}.json").write_text(json.dumps(caption_data, indent=2, ensure_ascii=False))

    # ─── VIDEO/IMAGES LOGIC ───
    video_data = item.get("video", {})
    play_url = None
    bit_rate = video_data.get("bitrateInfo") or video_data.get("bitRateList") or []
    if bit_rate: play_url = bit_rate[0].get("PlayAddr", {}).get("UrlList", [None])[0]
    
    if not play_url:
        for key in ("downloadAddr", "playAddr"):
            val = video_data.get(key)
            if val: play_url = val[0] if isinstance(val, list) else val
            if play_url: break

    if play_url:
        await download_file(client, play_url, folder / f"video__{file_prefix}.mp4")
    else:
        images = item.get("imagePost", {}).get("images", [])
        for i, img in enumerate(images):
            img_url = img.get("imageURL", {}).get("urlList", [None])[0]
            if img_url: await download_file(client, img_url, folder / f"carousel_{i+1:03d}.jpg")

    # ─── AUDIO LOGIC ───
    music = item.get("music", {})
    audio_url = music.get("playUrl")
    if audio_url:
        if isinstance(audio_url, list): audio_url = audio_url[0]
        elif isinstance(audio_url, dict): audio_url = audio_url.get("urlList", [None])[0]
        if audio_url: await download_file(client, audio_url, folder / f"audio__{file_prefix}.mp3")

    # ─── COMMENTS ───
    await fetch_comments(client, v_id, folder, file_prefix)
    
    # ─── UPLOAD TO MEGA ───
    await asyncio.sleep(random.uniform(1, 4)) # Login collision se bachne ke liye
    upload_to_mega(folder, file_prefix)

async def main():
    logger.info(f"=== TikTok Scraper | Node {NODE_INDEX}/{TOTAL_NODES} ===")
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    if not os.path.exists(LINKS_FILE):
        logger.error(f"Links file not found: {LINKS_FILE}")
        return

    urls = [l.strip() for l in open(LINKS_FILE) if l.strip() and not l.startswith("#")]
    my_urls = [u for i, u in enumerate(urls) if i % TOTAL_NODES == NODE_INDEX]
    logger.info(f"Total: {len(urls)} | My share: {len(my_urls)}")

    async with httpx.AsyncClient(http2=False, timeout=60) as client:
        for idx, url in enumerate(my_urls, 1):
            try:
                await scrape_one(client, url, idx, len(my_urls))
            except Exception as e:
                logger.error(f"FAILED {url}: {e}")
            if idx < len(my_urls): 
                await asyncio.sleep(random.uniform(2.0, 5.0))

    logger.info("=== ALL DONE ===")

if __name__ == "__main__":
    asyncio.run(main())