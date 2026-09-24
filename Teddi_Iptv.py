#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEGA ULTRA IPTV PARSER v3.0

Объединяет идеи из трёх парсеров из исходного документа:
1) Mega Parser: приоритетные каналы, альтернативы, качество, latency.
2) Ksenia-style parser: нормализация, fuzzy matching, кодировки.
3) Ultra IPTV Checker: --discover, HTTP-check, ffprobe, score.

Назначение:
- собирать публичные M3U/M3U8/TXT-источники;
- объединять одинаковые каналы;
- находить альтернативные публичные потоки;
- проверять доступность;
- при --deep получать разрешение/кодек/битрейт через ffprobe;
- строить best/stable/online/all_with_alts;
- сохранять подробный results.json.

Установка:
    pip install aiohttp rich

Для --deep нужен ffprobe (обычно входит в пакет FFmpeg).

Примеры:
    python mega_ultra_iptv.py --discover --deep
    python mega_ultra_iptv.py -s mylist.m3u --discover --deep --top 5
    python mega_ultra_iptv.py -s mylist.m3u --no-check
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import aiohttp

try:
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeRemainingColumn,
    )
except ImportError:
    Console = None
    RichHandler = None


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36 VLC/3.0.20"
)

REQUEST_TIMEOUT = 18
STREAM_TIMEOUT = 8
DEFAULT_WORKERS = 60
DEFAULT_TOP_N = 5
DEFAULT_FUZZY = 0.82
FFPROBE_TIMEOUT = 12
MAX_ALTS_PER_CHANNEL = 25

PRIORITY_CHANNELS = [
    "приключения", "ключ", "клюка", "клюкатв",
    "изобретения кемерово", "iz тв кемерово", "из тв кемерово",
    "изобретения армавир", "iz тв армавир", "из тв армавир",
    "gln news 24", "gln news", "геленджик",
    "первый ростов", "первый ростов-на-дону",
    "твоя тюмень", "tvоя тюмень",
    "отв челябинск", "отв",
    "24kz", "24 kz", "ютв", "ю тв",
    "беларусь 24", "беларусь24",
    "союзный", "союзный канал",
    "сибирь 24", "сибирь24",
    "тюмень 1", "тюмень1",
    "новое новгородское", "ннт", "новгородское областное",
    "h1", "первый нижегородский",
    "кинокульт", "кино культ",
    "hollywood", "живи активно", "живи",
    "ретро тв", "ретро",
    "неизвестная планета", "overground planet",
    "жара", "жара tv", "ржд тв", "ржд",
    "загородная жизнь", "travelxp",
    "clubbing tv", "clubbing", "аппетитный", "rtg tv", "rtg",
]

ADULT_KEYWORDS = [
    "xxx", "porn", "porno", "erotica", "эротика", "эротический",
    "adult", "18+", "18 +", "+18", "sex", "sexy", "hentai",
    "brazzers", "playboy", "redlight", "red light", "private",
    "chaturbate", "onlyfans", "xhamster", "xvideos", "pornhub",
    "youporn", "redtube", "tube8", "spankbang", "xnxx",
    "nude", "naked", "strip", "striptease", "fetish", "bdsm",
    "hardcore", "softcore", "milf", "teen sex", "gay porn",
    "русская эротика", "русское порно", "adult channel", "adult tv",
    "dorcel", "private platinum", "hustler", "penthouse",
]

# Проверенные URL из исходного документа, без намеренного добавления
# новых частных/платных источников.
PUBLIC_INTERNET_SOURCES = [
    "https://raw.githubusercontent.com/IPTVRU2026/IPTVMIR/main/IPTV_MEGA_PLAYLIST.m3u",
    "https://gitverse.ru/api/repos/RUVIPIEN/IPTVMIR/raw/branch/main/IPTV_MEGA_PLAYLIST.m3u",

    "https://raw.githubusercontent.com/Monoloshka/iptv/main/BeeTV.m3u",
    "https://raw.githubusercontent.com/Monoloshka/iptv/main/full-iptv.m3u",
    "https://raw.githubusercontent.com/Monoloshka/iptv/main/tv.m3u",

    "https://dearbulut.github.io/iptv/playlists/best.m3u",
    "https://dearbulut.github.io/iptv/playlists/online.m3u",
    "https://dearbulut.github.io/iptv/playlists/index.m3u",
    "https://dearbulut.github.io/iptv/playlists/language/rus.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/ru.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/kz.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/by.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/ua.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/uz.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/kg.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/tj.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/mn.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/general.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/news.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/sports.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/movies.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/kids.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/music.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/entertainment.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/documentary.m3u",

    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/index.category.m3u",
    "https://iptv-org.github.io/iptv/index.country.m3u",
    "https://iptv-org.github.io/iptv/index.language.m3u",
    "https://iptv-org.github.io/iptv/languages/rus.m3u",
    "https://iptv-org.github.io/iptv/countries/ru.m3u",
    "https://iptv-org.github.io/iptv/countries/kz.m3u",
    "https://iptv-org.github.io/iptv/countries/by.m3u",
    "https://iptv-org.github.io/iptv/countries/ua.m3u",
    "https://iptv-org.github.io/iptv/countries/uz.m3u",
    "https://iptv-org.github.io/iptv/countries/kg.m3u",
    "https://iptv-org.github.io/iptv/countries/tj.m3u",
    "https://iptv-org.github.io/iptv/countries/tm.m3u",
    "https://iptv-org.github.io/iptv/countries/am.m3u",
    "https://iptv-org.github.io/iptv/countries/az.m3u",
    "https://iptv-org.github.io/iptv/countries/ge.m3u",
    "https://iptv-org.github.io/iptv/countries/md.m3u",
    "https://iptv-org.github.io/iptv/countries/mn.m3u",
    "https://iptv-org.github.io/iptv/regions/cis.m3u",
    "https://iptv-org.github.io/iptv/regions/cas.m3u",
    "https://iptv-org.github.io/iptv/categories/news.m3u",
    "https://iptv-org.github.io/iptv/categories/sports.m3u",
    "https://iptv-org.github.io/iptv/categories/movies.m3u",
    "https://iptv-org.github.io/iptv/categories/general.m3u",
    "https://iptv-org.github.io/iptv/categories/kids.m3u",
    "https://iptv-org.github.io/iptv/categories/music.m3u",
    "https://iptv-org.github.io/iptv/categories/entertainment.m3u",
    "https://iptv-org.github.io/iptv/categories/documentary.m3u",

    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/IPTVru.m3u",
    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/IPTVstable.m3u8",
    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/IPTVmir.m3u8",
    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/IPTVdonor.m3u",
    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/IPRadio.m3u",
    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/KseniaTV.m3u",

    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",

    "https://raw.githubusercontent.com/blackbirdstudiorus/IPTVPlay/main/IPTVPlay.m3u",
    "https://raw.githubusercontent.com/blackbirdstudiorus/IPTVPlay/main/KionPlus.m3u",
    "https://raw.githubusercontent.com/naggdd/iptv/main/ru.m3u",
    "https://raw.githubusercontent.com/naggdd/iptv/main/music.m3u",
    "https://raw.githubusercontent.com/naggdd/iptv/main/cartoons.m3u",
    "https://raw.githubusercontent.com/ngrch/iptv/ru.m3u",
    "https://raw.githubusercontent.com/romaxa55/world_ip_tv/main/output/index.m3u",
]


# ---------------------------------------------------------------------------
# MODELS
# ---------------------------------------------------------------------------

@dataclass
class StreamInfo:
    url: str
    source: str = ""
    source_priority: int = 0
    latency_ms: float = 99999.0
    http_ok: bool = False
    status_code: int = 0
    resolution: str = ""
    width: int = 0
    height: int = 0
    codec: str = ""
    bitrate_kbps: float = 0.0
    quality: str = "unknown"
    score: float = 0.0
    error: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Channel:
    name: str
    normalized: str
    group: str = "Undefined"
    logo: str = ""
    tvg_id: str = ""
    language: str = ""
    country: str = ""
    priority: bool = False
    streams: list[StreamInfo] = field(default_factory=list)

    @property
    def alive_streams(self) -> list[StreamInfo]:
        return sorted(
            [s for s in self.streams if s.http_ok],
            key=lambda s: s.score,
            reverse=True,
        )

    @property
    def best_stream(self) -> Optional[StreamInfo]:
        alive = self.alive_streams
        return alive[0] if alive else None


# ---------------------------------------------------------------------------
# NORMALIZATION / PARSING
# ---------------------------------------------------------------------------

QUALITY_RE = re.compile(
    r"[\s_-](4[Kk]|UHD|FHD|HD|SD|HEVC|H.?265|H.?264|AVC|"
    r"50[Ff]ps|60[Ff]ps|\d{3,4}[pP]|HQ|LQ|Full\s*HD)[\s_-]*",
    re.IGNORECASE,
)

EMOJI_RE = re.compile(
    "[" "\U0001F600-\U0001F64F" "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF" "\U0001F1E0-\U0001F1FF"
    "\U00002702-\U000027B0" "\U000024C2-\U0001F251" "]+",
    flags=re.UNICODE,
)

GEO_RE = re.compile(
    r"[\s_-](Geo[\s-]?blocked|Only\s*(RU|BY|KZ|UA|EU|US)|"
    r"\((RU|BY|KZ|UA)\))[\s_-]*",
    re.IGNORECASE,
)

BRACKETS_RE = re.compile(r"[\(\[\{].*?[\)\]\}]")


def normalize_name(name: str) -> str:
    if not name:
        return ""

    n = name.strip()
    n = EMOJI_RE.sub(" ", n)
    n = QUALITY_RE.sub(" ", n)
    n = GEO_RE.sub(" ", n)
    n = BRACKETS_RE.sub(" ", n)
    n = re.sub(r"[^\w\sа-яА-ЯёЁіїєґІЇЄҐ-]", " ", n, flags=re.UNICODE)
    n = re.sub(r"\s+", " ", n).strip().lower()
    return n


def is_adult(name: str, group: str = "") -> bool:
    text = f"{name} {group}".lower()
    return any(k in text for k in ADULT_KEYWORDS)


def is_priority(name: str) -> bool:
    n = normalize_name(name)
    return any(p in n for p in PRIORITY_CHANNELS)


def fuzzy_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def detect_quality(name: str, url: str = "", height: int = 0) -> str:
    text = f"{name} {url}".lower()
    if height >= 2160 or any(x in text for x in ("4k", "uhd", "2160")):
        return "4k"
    if height >= 1080 or any(x in text for x in ("fhd", "1080", "fullhd", "full hd")):
        return "fhd"
    if height >= 720 or any(x in text for x in ("hd", "720")):
        return "hd"
    if height >= 360 or any(x in text for x in ("sd", "576", "480", "360")):
        return "sd"
    return "unknown"


def detect_and_decode(raw: bytes) -> str:
    for enc in ("utf-8", "windows-1251", "cp1251", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            pass
    return raw.decode("utf-8", errors="ignore")


def parse_m3u(content: str) -> list[dict]:
    entries = []
    current = None

    for raw in content.splitlines():
        line = raw.strip()
        if not line or line.startswith("#EXTM3U"):
            continue

        if line.startswith("#EXTINF:"):
            current = {
                "name": "",
                "group": "Undefined",
                "logo": "",
                "tvg_id": "",
                "language": "",
                "country": "",
            }

            attrs = dict(re.findall(r'([\w-]+)="([^"]*)"', line))

            current["tvg_id"] = attrs.get("tvg-id", "") or attrs.get("tvg-name", "")
            current["logo"] = attrs.get("tvg-logo", "")
            current["group"] = attrs.get("group-title", "Undefined") or "Undefined"
            current["language"] = attrs.get("tvg-language", "") or attrs.get("lang", "")
            current["country"] = attrs.get("tvg-country", "")

            if "," in line:
                current["name"] = line.split(",", 1)[1].strip()
            continue

        if line.startswith("#"):
            continue

        if current and line.startswith(("http://", "https://", "rtmp://", "rtsp://", "udp://")):
            current["url"] = line
            entries.append(current)
            current = None

    return entries


def parse_txt(content: str) -> list[dict]:
    entries = []

    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        if "," in line:
            name, url = line.split(",", 1)
        elif "|" in line:
            name, url = line.split("|", 1)
        else:
            continue

        url = url.strip()
        if not url.startswith(("http://", "https://", "rtmp://", "rtsp://", "udp://")):
            continue

        entries.append({
            "name": name.strip(),
            "url": url,
            "group": "Undefined",
            "logo": "",
            "tvg_id": "",
            "language": "",
            "country": "",
        })

    return entries


# ---------------------------------------------------------------------------
# NETWORK
# ---------------------------------------------------------------------------

async def fetch_source(
    session: aiohttp.ClientSession,
    source: str,
    priority: int,
) -> tuple[str, Optional[str], int]:
    try:
        async with session.get(
            source,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            headers={"User-Agent": DEFAULT_USER_AGENT},
            allow_redirects=True,
        ) as resp:
            if resp.status != 200:
                return source, None, resp.status

            raw = await resp.read()
            return source, detect_and_decode(raw), resp.status

    except Exception:
        return source, None, 0


async def load_sources(sources: list[str], workers: int = 25) -> list[dict]:
    connector = aiohttp.TCPConnector(
        limit=workers,
        ttl_dns_cache=300,
        ssl=False,
    )

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    ) as session:

        tasks = [
            fetch_source(session, src, len(sources) - i)
            for i, src in enumerate(sources)
        ]

        results = await asyncio.gather(*tasks)

    all_entries = []

    for idx, (src, content, status) in enumerate(results):
        if not content:
            logging.warning("Источник недоступен [%s]: %s", status, src)
            continue

        if "#EXTM3U" in content[:500] or "#EXTINF" in content:
            parsed = parse_m3u(content)
        else:
            parsed = parse_txt(content)

        priority = len(sources) - idx

        for e in parsed:
            if is_adult(e.get("name", ""), e.get("group", "")):
                continue

            e["_source"] = src
            e["_priority"] = priority

        all_entries.extend(parsed)
        logging.info("Источник: %s → %d записей", src, len(parsed))

    return all_entries


# ---------------------------------------------------------------------------
# GROUPING / FUZZY / DEDUP
# ---------------------------------------------------------------------------

def build_channels(
    entries: list[dict],
    fuzzy_threshold: float = DEFAULT_FUZZY,
    max_per_name: int = MAX_ALTS_PER_CHANNEL,
) -> dict[str, Channel]:

    channels: dict[str, Channel] = {}
    seen_urls: set[str] = set()

    for e in entries:
        url = e.get("url", "").strip()
        name = e.get("name", "").strip()

        if not url or not name or url in seen_urls:
            continue

        seen_urls.add(url)

        norm = normalize_name(name)
        if not norm:
            continue

        # Сначала точное совпадение.
        key = norm

        if key not in channels:
            # Fuzzy-поиск среди уже созданных групп.
            best_key = None
            best_score = 0.0

            for existing in channels:
                score = fuzzy_ratio(norm, existing)
                if score >= fuzzy_threshold and score > best_score:
                    best_score = score
                    best_key = existing

            if best_key:
                key = best_key

        if key not in channels:
            channels[key] = Channel(
                name=name,
                normalized=key,
                group=e.get("group", "Undefined") or "Undefined",
                logo=e.get("logo", ""),
                tvg_id=e.get("tvg_id", ""),
                language=e.get("language", ""),
                country=e.get("country", ""),
                priority=is_priority(name),
            )

        ch = channels[key]

        # Берём более информативные метаданные.
        if not ch.logo and e.get("logo"):
            ch.logo = e["logo"]
        if not ch.tvg_id and e.get("tvg_id"):
            ch.tvg_id = e["tvg_id"]
        if ch.group == "Undefined" and e.get("group"):
            ch.group = e["group"]

        source_priority = int(e.get("_priority", 0))

        stream = StreamInfo(
            url=url,
            source=e.get("_source", ""),
            source_priority=source_priority,
            quality=detect_quality(name, url),
        )

        if len(ch.streams) < max_per_name:
            ch.streams.append(stream)

        ch.priority = ch.priority or is_priority(name)

    return channels


# ---------------------------------------------------------------------------
# CHECK / FFPROBE / SCORE
# ---------------------------------------------------------------------------

def run_ffprobe(url: str, timeout: int = FFPROBE_TIMEOUT) -> dict:
    if not shutil.which("ffprobe"):
        return {}

    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        "-show_format",
        "-probesize", "500000",
        "-analyzeduration", "2000000",
        "-timeout", str(timeout * 1_000_000),
        url,
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 2,
        )

        if result.returncode != 0:
            return {"error": (result.stderr or "ffprobe failed")[:160]}

        data = json.loads(result.stdout)
        video = next(
            (s for s in data.get("streams", []) if s.get("codec_type") == "video"),
            None,
        )
        fmt = data.get("format", {})

        if not video:
            return {}

        width = int(video.get("width") or 0)
        height = int(video.get("height") or 0)

        bitrate = 0.0
        if fmt.get("bit_rate"):
            try:
                bitrate = round(int(fmt["bit_rate"]) / 1000, 1)
            except Exception:
                pass

        return {
            "width": width,
            "height": height,
            "resolution": f"{width}x{height}" if width else "",
            "codec": video.get("codec_name", ""),
            "bitrate_kbps": bitrate,
        }

    except subprocess.TimeoutExpired:
        return {"error": "ffprobe timeout"}
    except Exception as exc:
        return {"error": str(exc)[:160]}


def calculate_score(stream: StreamInfo) -> float:
    if not stream.http_ok:
        return 0.0

    lat = stream.latency_ms

    if lat < 200:
        lat_score = 40
    elif lat < 500:
        lat_score = 35
    elif lat < 1000:
        lat_score = 25
    elif lat < 2000:
        lat_score = 15
    else:
        lat_score = 5

    h = stream.height

    if h >= 2160:
        res_score = 40
    elif h >= 1080:
        res_score = 35
    elif h >= 720:
        res_score = 25
    elif h >= 480:
        res_score = 15
    elif h > 0:
        res_score = 8
    else:
        res_score = 10

    br = stream.bitrate_kbps

    if br > 5000:
        br_score = 15
    elif br > 2500:
        br_score = 12
    elif br > 1000:
        br_score = 8
    elif br > 0:
        br_score = 4
    else:
        br_score = 5

    codec_bonus = 5 if stream.codec.lower() in {
        "h264", "avc", "hevc", "h265"
    } else 0

    # Небольшой бонус источнику с более высоким приоритетом.
    source_bonus = min(max(stream.source_priority, 0), 5)

    return lat_score + res_score + br_score + codec_bonus + source_bonus


async def check_stream(
    session: aiohttp.ClientSession,
    stream: StreamInfo,
    deep: bool,
    semaphore: asyncio.Semaphore,
) -> None:

    async with semaphore:
        start = time.perf_counter()

        try:
            async with session.get(
                stream.url,
                headers={"User-Agent": DEFAULT_USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=STREAM_TIMEOUT),
                allow_redirects=True,
                ssl=False,
            ) as resp:

                # Читаем небольшой фрагмент, чтобы отличить живой HTTP-ответ
                # от некоторых пустых/ошибочных endpoint'ов.
                await resp.content.read(1024)

                stream.latency_ms = round(
                    (time.perf_counter() - start) * 1000,
                    1,
                )
                stream.status_code = resp.status
                stream.http_ok = 200 <= resp.status < 400

                if not stream.http_ok:
                    stream.error = f"HTTP {resp.status}"

        except asyncio.TimeoutError:
            stream.latency_ms = round(
                (time.perf_counter() - start) * 1000,
                1,
            )
            stream.error = "timeout"

        except Exception as exc:
            stream.latency_ms = round(
                (time.perf_counter() - start) * 1000,
                1,
            )
            stream.error = str(exc)[:120]

        if stream.http_ok and deep:
            loop = asyncio.get_running_loop()
            probe = await loop.run_in_executor(
                None,
                run_ffprobe,
                stream.url,
            )

            if "error" not in probe:
                stream.width = probe.get("width", 0)
                stream.height = probe.get("height", 0)
                stream.resolution = probe.get("resolution", "")
                stream.codec = probe.get("codec", "")
                stream.bitrate_kbps = probe.get("bitrate_kbps", 0.0)
                stream.quality = detect_quality(
                    "",
                    stream.url,
                    stream.height,
                )
            elif probe.get("error"):
                stream.error = probe["error"]

        stream.score = calculate_score(stream)


async def check_channels(
    channels: dict[str, Channel],
    workers: int,
    deep: bool,
) -> None:

    total = sum(len(ch.streams) for ch in channels.values())
    semaphore = asyncio.Semaphore(workers)

    connector = aiohttp.TCPConnector(
        limit=workers,
        ttl_dns_cache=300,
        ssl=False,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    ) as session:

        if Console is not None:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task("Проверка потоков", total=total)

                async def wrapped(stream: StreamInfo):
                    await check_stream(session, stream, deep, semaphore)
                    progress.advance(task)

                await asyncio.gather(
                    *[
                        wrapped(stream)
                        for channel in channels.values()
                        for stream in channel.streams
                    ]
                )
        else:
            await asyncio.gather(
                *[
                    check_stream(session, stream, deep, semaphore)
                    for channel in channels.values()
                    for stream in channel.streams
                ]
            )


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------

def stream_label(stream: StreamInfo) -> str:
    q = stream.quality
    lat = "?" if stream.latency_ms >= 99999 else f"{stream.latency_ms:.0f}ms"
    return f"{q}|{lat}"


def sorted_channels(channels: dict[str, Channel]) -> list[Channel]:
    return sorted(
        channels.values(),
        key=lambda ch: (
            0 if ch.priority else 1,
            -(len(ch.alive_streams)),
            -(ch.best_stream.score if ch.best_stream else 0),
            ch.name.lower(),
        ),
    )


def write_m3u(
    channels: dict[str, Channel],
    path: Path,
    mode: str,
    top_n: int = DEFAULT_TOP_N,
) -> int:

    lines = [
        "#EXTM3U",
        '#EXTM3U x-tvg-url="https://iptvx.one/epg/epg.xml.gz" '
        'generated-by="MegaUltraIPTVParser"',
    ]

    count = 0

    for ch in sorted_channels(channels):
        alive = ch.alive_streams

        if mode == "best":
            streams = alive[:1] if alive else ch.streams[:1]
        elif mode == "stable":
            streams = [
                s for s in alive
                if s.latency_ms < 1000
            ][:top_n]
        elif mode == "online":
            streams = alive
        else:  # all
            streams = alive[:top_n] if alive else ch.streams[:top_n]

        for idx, stream in enumerate(streams):
            name = ch.name

            if ch.priority:
                name = "★ " + name

            if len(streams) > 1 or idx > 0:
                name += f" [ALT{idx + 1}|{stream_label(stream)}]"
            elif stream.latency_ms < 99999:
                name += f" [{stream_label(stream)}]"

            attrs = []

            if ch.tvg_id:
                attrs.append(f'tvg-id="{ch.tvg_id}"')

            if ch.logo:
                attrs.append(f'tvg-logo="{ch.logo}"')

            attrs.append(f'group-title="{ch.group}"')

            if stream.quality != "unknown":
                attrs.append(f'tvg-quality="{stream.quality}"')

            lines.append(
                f'#EXTINF:-1 {" ".join(attrs)},{name}'
            )
            lines.append(stream.url)
            count += 1

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return count


def write_json(channels: dict[str, Channel], path: Path) -> None:
    data = {}

    for key, ch in channels.items():
        data[key] = {
            "name": ch.name,
            "normalized": ch.normalized,
            "group": ch.group,
            "logo": ch.logo,
            "tvg_id": ch.tvg_id,
            "language": ch.language,
            "country": ch.country,
            "priority": ch.priority,
            "alternatives": len(ch.streams),
            "alive": len(ch.alive_streams),
            "best": ch.best_stream.to_dict() if ch.best_stream else None,
            "streams": [s.to_dict() for s in ch.streams],
        }

    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def print_summary(channels: dict[str, Channel]) -> None:
    total_channels = len(channels)
    total_streams = sum(len(c.streams) for c in channels.values())
    alive_streams = sum(len(c.alive_streams) for c in channels.values())
    priority_count = sum(1 for c in channels.values() if c.priority)

    print("\n" + "=" * 72)
    print("MEGA ULTRA IPTV — ГОТОВО")
    print("=" * 72)
    print(f"Уникальных каналов : {total_channels}")
    print(f"Всего потоков      : {total_streams}")
    print(f"Рабочих потоков    : {alive_streams}")
    print(f"Приоритетных       : {priority_count}")

    print("\nПриоритетные каналы:")
    for ch in sorted_channels(channels):
        if not ch.priority:
            continue

        best = ch.best_stream
        if best:
            print(
                f"  ★ {ch.name} | "
                f"{stream_label(best)} | "
                f"score={best.score:.1f}"
            )
        else:
            print(f"  ★ {ch.name} | offline")

    print("=" * 72)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Mega Ultra IPTV Parser: public playlist discovery, "
            "fuzzy grouping, health check and alternatives."
        )
    )

    parser.add_argument(
        "-s", "--source",
        action="append",
        default=[],
        help="M3U/M3U8/TXT URL или локальный файл. Можно указать несколько раз.",
    )

    parser.add_argument(
        "--discover",
        action="store_true",
        help="Добавить публичные источники из встроенного списка.",
    )

    parser.add_argument(
        "--deep",
        action="store_true",
        help="Проверять разрешение/кодек/битрейт через ffprobe.",
    )

    parser.add_argument(
        "--no-check",
        action="store_true",
        help="Не проверять потоки, только собрать и сгруппировать.",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Параллельных проверок: по умолчанию {DEFAULT_WORKERS}.",
    )

    parser.add_argument(
        "--fuzzy",
        type=float,
        default=DEFAULT_FUZZY,
        help=f"Порог fuzzy matching: по умолчанию {DEFAULT_FUZZY}.",
    )

    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Максимум альтернатив на канал: по умолчанию {DEFAULT_TOP_N}.",
    )

    parser.add_argument(
        "--max-alts",
        type=int,
        default=MAX_ALTS_PER_CHANNEL,
        help=f"Максимум сохранённых URL на канал: по умолчанию {MAX_ALTS_PER_CHANNEL}.",
    )

    parser.add_argument(
        "--output-dir",
        default="mega_ultra_output",
        help="Каталог результатов.",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Подробный лог.",
    )

    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    sources = list(dict.fromkeys(args.source))

    if args.discover:
        sources.extend(PUBLIC_INTERNET_SOURCES)
        sources = list(dict.fromkeys(sources))

    if not sources:
        raise SystemExit(
            "Укажи хотя бы источник: -s mylist.m3u "
            "или используй --discover"
        )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Источников: {len(sources)}")
    print("Загрузка публичных плейлистов...")

    entries = await load_sources(
        sources,
        workers=min(max(args.workers, 5), 100),
    )

    print(f"Сырых записей: {len(entries)}")

    channels = build_channels(
        entries,
        fuzzy_threshold=max(0.0, min(args.fuzzy, 1.0)),
        max_per_name=max(1, args.max_alts),
    )

    print(f"Уникальных каналов после группировки: {len(channels)}")

    if not args.no_check:
        print(
            f"Проверка потоков: workers={args.workers}, "
            f"deep={args.deep}"
        )

        await check_channels(
            channels,
            workers=max(1, min(args.workers, 200)),
            deep=args.deep,
        )

    files = {
        "best": out_dir / "best.m3u",
        "stable": out_dir / "stable.m3u",
        "online": out_dir / "online.m3u",
        "all": out_dir / "all_with_alts.m3u",
        "json": out_dir / "results.json",
    }

    n_best = write_m3u(channels, files["best"], "best", args.top)
    n_stable = write_m3u(channels, files["stable"], "stable", args.top)
    n_online = write_m3u(channels, files["online"], "online", args.top)
    n_all = write_m3u(channels, files["all"], "all", args.top)

    write_json(channels, files["json"])

    print("\nФайлы:")
    print(f"  best.m3u          → {n_best}")
    print(f"  stable.m3u        → {n_stable}")
    print(f"  online.m3u        → {n_online}")
    print(f"  all_with_alts.m3u → {n_all}")
    print(f"  results.json")

    print_summary(channels)


def main() -> None:
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
