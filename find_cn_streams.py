#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlparse, urlsplit, urlunsplit
from urllib.request import Request, urlopen

DEFAULT_USER_AGENT = "iptv-cn-finder/1.0"
CHINESE_LANGUAGE_CODES = {
    "chi",
    "cmn",
    "cdo",
    "cjy",
    "hak",
    "nan",
    "wuu",
    "yue",
    "zho",
}
CHINESE_REGION_CODES = {"CN", "HK", "MO", "TW", "SG", "MY"}
CHINESE_HINT_KEYWORDS = (
    "beijing",
    "cantonese",
    "cctv",
    "chinese",
    "dragon tv",
    "guangdong",
    "hong kong",
    "mandarin",
    "phoenix",
    "shanghai",
    "taiwan",
    "tvb",
    "中文",
    "华语",
    "台视",
    "国语",
    "央视",
    "广东",
    "本港台",
    "粤语",
    "香港",
)
M3U_CONTENT_TYPES = {
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "audio/mpegurl",
    "audio/x-mpegurl",
}
PLAYABLE_CONTENT_PREFIXES = ("audio/", "video/")
TEXT_SAMPLE_LIMIT = 65536
MEDIA_SAMPLE_LIMIT = 4096
CONTENT_SAMPLE_SIZE = 16 * 16
PROBE_ENVIRONMENTS = ("local", "cloud")
DEFAULT_PROBE_ENVIRONMENT = os.environ.get("IPTV_PROBE_ENV", "local")
PLAYLIST_MEDIA_SEQUENCE_RE = re.compile(r"#EXT-X-MEDIA-SEQUENCE:(\d+)", re.IGNORECASE)
PLAYLIST_TARGET_DURATION_RE = re.compile(r"#EXT-X-TARGETDURATION:(\d+)", re.IGNORECASE)
HAN_RE = re.compile(r"[\u3400-\u9FFF]")
EXTINF_ATTR_RE = re.compile(r'([A-Za-z0-9_-]+)="([^"]*)"')
IP_HOST_RE = re.compile(r"^(?:\d{1,3}\.){3}\d{1,3}$")
CCTV_OFFICIAL_JS_URL = "https://js.player.cntv.cn/creator/liveplayer.js"
CURATED_PUBLIC_M3U_URLS = {
    "suxuang": "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u",
    "guovin": "https://raw.githubusercontent.com/Guovin/TV/gd/output/result.m3u",
    "zbds": "https://live.zbds.top/tv/iptv4.m3u",
    "chinaiptv": "https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/cnTV_AutoUpdate.m3u8",
}
PUBLISHED_PLAYLIST_PATH = Path("m3u/chinese-public-verified.m3u")
PUBLISHED_BACKUP_PLAYLIST_PATH = Path("m3u/chinese-public-with-backups.m3u")
DEFAULT_HISTORY_PATH = Path("state/probe-history.json")
BLOCKED_CANDIDATE_URL_PATTERNS = (
    "iptv.catvod.com/live.php",
    "cdn.jsdelivr.net/gh/namegenliang/fast-github-access",
)
CCTV_OFFICIAL_WEBSITES = {
    "CCTV1.cn": "https://tv.cctv.com/live/cctv1/",
    "CCTV13.cn": "https://tv.cctv.com/live/cctv13/",
}
CCTV_CHANNEL_LABELS = {
    "CCTV1.cn": "CCTV-1 综合",
    "CCTV2.cn": "CCTV-2 财经",
    "CCTV3.cn": "CCTV-3 综艺",
    "CCTV4K.cn": "CCTV-4K 超高清",
    "CCTV5.cn": "CCTV-5 体育",
    "CCTV5Plus.cn": "CCTV-5+ 体育赛事",
    "CCTV6.cn": "CCTV-6 电影",
    "CCTV7.cn": "CCTV-7 国防军事",
    "CCTV8.cn": "CCTV-8 电视剧",
    "CCTV8K.cn": "CCTV-8K 超高清",
    "CCTV9.cn": "CCTV-9 纪录",
    "CCTV10.cn": "CCTV-10 科教",
    "CCTV11.cn": "CCTV-11 戏曲",
    "CCTV12.cn": "CCTV-12 社会与法",
    "CCTV13.cn": "CCTV-13 新闻",
    "CCTV14.cn": "CCTV-14 少儿",
    "CCTV15.cn": "CCTV-15 音乐",
    "CCTV16.cn": "CCTV-16 奥林匹克",
    "CCTV17.cn": "CCTV-17 农业农村",
}
SATELLITE_CHANNEL_LABELS = {
    "AnhuiSatelliteTV.cn": "安徽卫视",
    "AnhuiTV.cn": "安徽卫视",
    "BeijingSatelliteTV.cn": "北京卫视",
    "BingtuanSatelliteTV.cn": "兵团卫视",
    "ChongqingSatelliteTV.cn": "重庆卫视",
    "DragonTV.cn": "东方卫视",
    "FujianSoutheastTV.cn": "东南卫视",
    "FujianStraitsTV.cn": "海峡卫视",
    "GansuSatelliteTV.cn": "甘肃卫视",
    "GBASatelliteTV.cn": "大湾区卫视",
    "GuangdongSatelliteTV.cn": "广东卫视",
    "HainanSatelliteTV.cn": "海南卫视",
    "HebeiSatelliteTV.cn": "河北卫视",
    "HeilongjiangSatelliteTV.cn": "黑龙江卫视",
    "HenanSatelliteTV.cn": "河南卫视",
    "HubeiSatelliteTV.cn": "湖北卫视",
    "HunanSatelliteTV.cn": "湖南卫视",
    "InnerMongoliaSatelliteTV.cn": "内蒙古卫视",
    "JiangsuSatelliteTV.cn": "江苏卫视",
    "JiangxiSatelliteTV.cn": "江西卫视",
    "JilinSatelliteTV.cn": "吉林卫视",
    "KangbaTV.cn": "康巴卫视",
    "LiaoningSatelliteTV.cn": "辽宁卫视",
    "NingxiaSatelliteChannel.cn": "宁夏卫视",
    "QinghaiSatelliteTV.cn": "青海卫视",
    "ShaanxiSatelliteTV.cn": "陕西卫视",
    "ShandongSatelliteTV.cn": "山东卫视",
    "ShenzhenSatelliteTV.cn": "深圳卫视",
    "SichuanSatelliteTV.cn": "四川卫视",
    "TheGreaterBaySatelliteTV.cn": "大湾区卫视",
    "TianjinSatelliteTV.cn": "天津卫视",
    "XinjiangSatelliteTV.cn": "新疆卫视",
    "YanbianSatelliteTV.cn": "延边卫视",
    "YunnanSatelliteTV.cn": "云南卫视",
    "ZhejiangSatelliteTV.cn": "浙江卫视",
}
SUBCHANNEL_LABELS = {
    "LiaoningSports.local": "辽宁体育",
    "LiaoningMetro.local": "辽宁都市",
    "LiaoningMovie.local": "辽宁影视剧",
    "LiaoningPublic.local": "辽宁公共",
    "ShanghaiFinance.local": "第一财经",
    "ShanghaiNews.local": "上海新闻综合",
    "ShanghaiMetro.local": "上海都市",
    "ShanghaiSports.local": "五星体育",
    "ShanghaiDocumentary.local": "纪实人文",
    "ShanghaiDrama.local": "都市剧场",
    "ShanghaiLife.local": "生活时尚",
    "ShanghaiAnimation.local": "哈哈炫动",
    "ShanghaiMovie.local": "东方影视",
    "HunanCity.local": "湖南都市",
    "HunanEconomy.local": "湖南经视",
    "HunanDrama.local": "湖南电视剧",
    "HunanEntertainment.local": "湖南娱乐",
    "HunanPublic.local": "湖南公共",
    "JinyingDocumentary.local": "金鹰纪实",
    "JinyingCartoon.local": "金鹰卡通",
    "JiangsuSports.local": "江苏体育",
    "JiangsuPublic.local": "江苏公共",
    "JiangsuCity.local": "江苏城市",
    "JiangsuMovie.local": "江苏影视",
    "JiangsuEducation.local": "江苏教育",
    "YoumanCartoon.local": "优漫卡通",
    "GuangdongPearl.local": "广东珠江",
    "GuangdongSports.local": "广东体育",
    "GuangdongNews.local": "广东新闻",
    "GuangdongPeople.local": "广东民生",
    "GuangzhouGeneral.local": "广州综合",
    "GuangzhouMovie.local": "广州影视",
}
SUBCHANNEL_GROUPS = {
    "LiaoningSports.local": "辽宁台",
    "LiaoningMetro.local": "辽宁台",
    "LiaoningMovie.local": "辽宁台",
    "LiaoningPublic.local": "辽宁台",
    "ShanghaiFinance.local": "上海台",
    "ShanghaiNews.local": "上海台",
    "ShanghaiMetro.local": "上海台",
    "ShanghaiSports.local": "上海台",
    "ShanghaiDocumentary.local": "上海台",
    "ShanghaiDrama.local": "上海台",
    "ShanghaiLife.local": "上海台",
    "ShanghaiAnimation.local": "上海台",
    "ShanghaiMovie.local": "上海台",
    "HunanCity.local": "湖南台",
    "HunanEconomy.local": "湖南台",
    "HunanDrama.local": "湖南台",
    "HunanEntertainment.local": "湖南台",
    "HunanPublic.local": "湖南台",
    "JinyingDocumentary.local": "湖南台",
    "JinyingCartoon.local": "湖南台",
    "JiangsuSports.local": "江苏台",
    "JiangsuPublic.local": "江苏台",
    "JiangsuCity.local": "江苏台",
    "JiangsuMovie.local": "江苏台",
    "JiangsuEducation.local": "江苏台",
    "YoumanCartoon.local": "江苏台",
    "GuangdongPearl.local": "广东台",
    "GuangdongSports.local": "广东台",
    "GuangdongNews.local": "广东台",
    "GuangdongPeople.local": "广东台",
    "GuangzhouGeneral.local": "广东台",
    "GuangzhouMovie.local": "广东台",
}
TARGET_CHANNEL_LABELS = {**CCTV_CHANNEL_LABELS, **SATELLITE_CHANNEL_LABELS, **SUBCHANNEL_LABELS}
DISABLED_CHANNEL_IDS: set[str] = set()
GROUP_SORT_ORDER = {
    "央视": 0,
    "卫视": 1,
    "上海台": 2,
    "湖南台": 3,
    "江苏台": 4,
    "广东台": 5,
    "辽宁台": 6,
}
VOD_URL_PATTERNS = (
    "/tvod/",
    "playback",
    "catchup",
    "timeshift",
    "recorded",
    "record=",
)
LIVE_URL_HINTS = (
    ".m3u8",
    ".m3u",
    ".flv",
    ".ts",
)
DIRECT_FILE_URL_HINTS = (
    ".aac",
    ".m4a",
    ".mkv",
    ".mov",
    ".mp3",
    ".mp4",
)
PREFERRED_URL_PATTERNS_BY_TITLE = {
    "东方卫视": (
        "38.75.136.137:98/gslb/dsdqpub/dfwshd.m3u8",
        "112.27.235.94:8000/hls/28",
        "bp-resource-dfl.bestv.cn/148/3/video.m3u8",
    ),
    "北京卫视": (
        "183.215.134.239:19901/tsfile/live/0122_1.m3u8",
        "101.35.240.114:88/live.php?id=%E5%8C%97%E4%BA%AC%E5%8D%AB%E8%A7%86",
        "satellitepull.cnr.cn/live/wxbtv",
    ),
    "河北卫视": (
        "112.27.235.94:8000/hls/39",
        "event.pull.hebtv.com/jishi/weishi_tingyun",
    ),
    "湖南卫视": (
        "112.27.235.94:8000/hls/31",
        "101.35.240.114:88/live.php?id=%E6%B9%96%E5%8D%97%E5%8D%AB%E8%A7%864K",
    ),
    "东南卫视": ("112.27.235.94:8000/hls/38",),
    "辽宁卫视": ("112.27.235.94:8000/hls/47",),
    "浙江卫视": (
        "112.27.235.94:8000/hls/28",
        "ali-m-l.cztv.com/channels/lantian/channel001/1080p.m3u8",
        "play-qukan.cztv.com",
    ),
    "辽宁体育": ("dassby.qqff.top:99/live/%E8%BE%BD%E5%AE%81%E4%BD%93%E8%82%B2",),
    "辽宁公共": ("dassby.qqff.top:99/live/%E8%BE%BD%E5%AE%81%E5%85%AC%E5%85%B1",),
    "辽宁影视剧": ("dassby.qqff.top:99/live/%E8%BE%BD%E5%AE%81%E5%BD%B1%E8%A7%86%E5%89%A7",),
    "辽宁都市": ("ls.qingting.fm/live/1099/64k",),
    "第一财经": ("bp-livetytv.bestv.cn/ch/bestvdycj.m3u8",),
    "上海新闻综合": ("bp-livetytv.bestv.cn/ch/bestvxwzh.m3u8",),
    "上海都市": ("bp-livetytv.bestv.cn/ch/bestvdspd.m3u8",),
    "五星体育": ("bp-livetytv.bestv.cn/ch/bestvwxty.m3u8",),
    "纪实人文": ("bp-resource-dfl.bestv.cn/155/3/video.m3u8",),
    "湖南都市": ("stream1.freetv.fun/hu-nan-du-shi-9.m3u8",),
    "湖南经视": ("stream1.freetv.fun/hu-nan-jing-shi-1.m3u8",),
    "湖南电视剧": ("stream1.freetv.fun/hu-nan-dian-shi-ju-2.m3u8",),
    "湖南娱乐": ("stream1.freetv.fun/hu-nan-yu-le-3.m3u8",),
    "湖南公共": ("stream1.freetv.fun/hu-nan-gong-gong-7.m3u8",),
    "金鹰纪实": ("iptv.huuc.edu.cn/hls/gedocu.m3u8",),
    "江苏城市": ("gslbmgsplive.miguvideo.com/wd_r2/jstv/jschengshi/600/index.m3u8",),
    "江苏教育": ("gslbmgsplive.miguvideo.com/wd_r2/jstv/jsjiaoyu/600/index.m3u8",),
    "优漫卡通": ("stream1.freetv.fun/you-man-qia-tong-11.m3u8",),
    "广东珠江": ("cdn2.163189.xyz/live/gdzj/stream.m3u8",),
    "广东体育": ("cdn2.163189.xyz/live/gdty/stream.m3u8",),
    "广东新闻": ("hls-gateway.vpstv.net/streams/708873.m3u8",),
    "广州影视": ("stream1.freetv.fun/yan-zhou-ying-shi-25.m3u8",),
}
KNOWN_SLOW_SOURCE_PATTERNS = (
    "dassby.qqff.top:99/live/",
    "58.57.40.22:9901/tsfile/live/",
)
MANUAL_PREFERRED_CANDIDATES = (
    {
        "channel_id": "DragonTV.cn",
        "title": "东方卫视",
        "url": "http://38.75.136.137:98/gslb/dsdqpub/dfwshd.m3u8?auth=testpub",
    },
    {
        "channel_id": "BeijingSatelliteTV.cn",
        "title": "北京卫视",
        "url": "http://183.215.134.239:19901/tsfile/live/0122_1.m3u8?key=txiptv&playlive=1&authid=0",
    },
    {
        "channel_id": "HebeiSatelliteTV.cn",
        "title": "河北卫视",
        "url": "http://112.27.235.94:8000/hls/39/index.m3u8",
    },
    {
        "channel_id": "HunanSatelliteTV.cn",
        "title": "湖南卫视",
        "url": "http://112.27.235.94:8000/hls/31/index.m3u8",
    },
    {
        "channel_id": "FujianSoutheastTV.cn",
        "title": "东南卫视",
        "url": "http://112.27.235.94:8000/hls/38/index.m3u8",
    },
    {
        "channel_id": "LiaoningSatelliteTV.cn",
        "title": "辽宁卫视",
        "url": "http://112.27.235.94:8000/hls/47/index.m3u8",
    },
    {
        "channel_id": "ZhejiangSatelliteTV.cn",
        "title": "浙江卫视",
        "url": "http://112.27.235.94:8000/hls/28/index.m3u8",
    },
    {
        "channel_id": "LiaoningSports.local",
        "title": "辽宁体育",
        "url": "http://dassby.qqff.top:99/live/%E8%BE%BD%E5%AE%81%E4%BD%93%E8%82%B2/index.m3u8",
    },
    {
        "channel_id": "LiaoningPublic.local",
        "title": "辽宁公共",
        "url": "http://dassby.qqff.top:99/live/%E8%BE%BD%E5%AE%81%E5%85%AC%E5%85%B1/index.m3u8",
    },
    {
        "channel_id": "LiaoningMovie.local",
        "title": "辽宁影视剧",
        "url": "http://dassby.qqff.top:99/live/%E8%BE%BD%E5%AE%81%E5%BD%B1%E8%A7%86%E5%89%A7/index.m3u8",
    },
    {
        "channel_id": "LiaoningMetro.local",
        "title": "辽宁都市",
        "url": "https://ls.qingting.fm/live/1099/64k.m3u8",
    },
    {
        "channel_id": "ShanghaiFinance.local",
        "title": "第一财经",
        "url": "http://bp-livetytv.bestv.cn/ch/bestvdycj.m3u8",
    },
    {
        "channel_id": "ShanghaiNews.local",
        "title": "上海新闻综合",
        "url": "http://bp-livetytv.bestv.cn/ch/bestvxwzh.m3u8",
    },
    {
        "channel_id": "ShanghaiMetro.local",
        "title": "上海都市",
        "url": "http://bp-livetytv.bestv.cn/ch/bestvdspd.m3u8",
    },
    {
        "channel_id": "ShanghaiSports.local",
        "title": "五星体育",
        "url": "http://bp-livetytv.bestv.cn/ch/bestvwxty.m3u8",
    },
    {
        "channel_id": "ShanghaiDocumentary.local",
        "title": "纪实人文",
        "url": "http://bp-resource-dfl.bestv.cn/155/3/video.m3u8",
    },
    {
        "channel_id": "HunanCity.local",
        "title": "湖南都市",
        "url": "https://stream1.freetv.fun/hu-nan-du-shi-9.m3u8",
    },
    {
        "channel_id": "HunanEconomy.local",
        "title": "湖南经视",
        "url": "https://stream1.freetv.fun/hu-nan-jing-shi-1.m3u8",
    },
    {
        "channel_id": "HunanDrama.local",
        "title": "湖南电视剧",
        "url": "https://stream1.freetv.fun/hu-nan-dian-shi-ju-2.m3u8",
    },
    {
        "channel_id": "HunanEntertainment.local",
        "title": "湖南娱乐",
        "url": "https://stream1.freetv.fun/hu-nan-yu-le-3.m3u8",
    },
    {
        "channel_id": "HunanPublic.local",
        "title": "湖南公共",
        "url": "https://stream1.freetv.fun/hu-nan-gong-gong-7.m3u8",
    },
    {
        "channel_id": "JinyingDocumentary.local",
        "title": "金鹰纪实",
        "url": "http://iptv.huuc.edu.cn/hls/gedocu.m3u8",
    },
    {
        "channel_id": "YoumanCartoon.local",
        "title": "优漫卡通",
        "url": "https://stream1.freetv.fun/you-man-qia-tong-11.m3u8",
    },
    {
        "channel_id": "GuangdongPearl.local",
        "title": "广东珠江",
        "url": "http://cdn2.163189.xyz/live/gdzj/stream.m3u8",
    },
    {
        "channel_id": "GuangdongSports.local",
        "title": "广东体育",
        "url": "http://cdn2.163189.xyz/live/gdty/stream.m3u8",
    },
    {
        "channel_id": "GuangdongNews.local",
        "title": "广东新闻",
        "url": "https://hls-gateway.vpstv.net/streams/708873.m3u8",
    },
    {
        "channel_id": "GuangzhouMovie.local",
        "title": "广州影视",
        "url": "https://stream1.freetv.fun/yan-zhou-ying-shi-25.m3u8",
    },
)
TARGET_CHANNEL_ALIASES = {
    "CCTV1.cn": ("CCTV-1", "CCTV1", "CCTV-1综合", "CCTV1综合", "央视一套", "央视综合"),
    "CCTV2.cn": ("CCTV-2", "CCTV2", "CCTV-2财经", "CCTV2财经", "央视二套"),
    "CCTV3.cn": ("CCTV-3", "CCTV3", "CCTV-3综艺", "CCTV3综艺", "央视三套"),
    "CCTV4K.cn": ("CCTV-4K", "CCTV4K"),
    "CCTV5.cn": ("CCTV-5", "CCTV5", "CCTV-5体育", "CCTV5体育"),
    "CCTV5Plus.cn": ("CCTV-5+", "CCTV5+", "CCTV5PLUS", "CCTV-5+体育赛事", "CCTV5+体育赛事"),
    "CCTV6.cn": ("CCTV-6", "CCTV6", "CCTV-6电影", "CCTV6电影"),
    "CCTV7.cn": ("CCTV-7", "CCTV7", "CCTV-7国防军事", "CCTV7国防军事"),
    "CCTV8.cn": ("CCTV-8", "CCTV8", "CCTV-8电视剧", "CCTV8电视剧"),
    "CCTV8K.cn": ("CCTV-8K", "CCTV8K"),
    "CCTV9.cn": ("CCTV-9", "CCTV9", "CCTV-9纪录", "CCTV9纪录"),
    "CCTV10.cn": ("CCTV-10", "CCTV10", "CCTV-10科教", "CCTV10科教"),
    "CCTV11.cn": ("CCTV-11", "CCTV11", "CCTV-11戏曲", "CCTV11戏曲"),
    "CCTV12.cn": ("CCTV-12", "CCTV12", "CCTV-12社会与法", "CCTV12社会与法"),
    "CCTV13.cn": ("CCTV-13", "CCTV13", "CCTV-13新闻", "CCTV13新闻", "央视新闻"),
    "CCTV14.cn": ("CCTV-14", "CCTV14", "CCTV-14少儿", "CCTV14少儿"),
    "CCTV15.cn": ("CCTV-15", "CCTV15", "CCTV-15音乐", "CCTV15音乐"),
    "CCTV16.cn": ("CCTV-16", "CCTV16", "CCTV-16奥林匹克", "CCTV16奥林匹克"),
    "CCTV17.cn": ("CCTV-17", "CCTV17", "CCTV-17农业农村", "CCTV17农业农村"),
    "DragonTV.cn": ("东方卫视", "东方卫视4K", "上海卫视", "上海东方卫视"),
    "BeijingSatelliteTV.cn": ("北京卫视", "北京卫视4K", "北京卫视4K超"),
    "HunanSatelliteTV.cn": ("湖南卫视", "湖南卫视4K"),
    "JiangsuSatelliteTV.cn": ("江苏卫视", "江苏卫视4K"),
    "ZhejiangSatelliteTV.cn": ("浙江卫视", "浙江卫视4K"),
    "ShenzhenSatelliteTV.cn": ("深圳卫视", "深圳卫视4K"),
    "LiaoningSatelliteTV.cn": ("辽宁卫视",),
    "AnhuiSatelliteTV.cn": ("安徽卫视",),
    "ShandongSatelliteTV.cn": ("山东卫视", "山东卫视4K"),
    "GuangdongSatelliteTV.cn": ("广东卫视", "广东卫视4K"),
    "SichuanSatelliteTV.cn": ("四川卫视", "四川卫视4K"),
    "TianjinSatelliteTV.cn": ("天津卫视",),
    "HebeiSatelliteTV.cn": ("河北卫视",),
    "HenanSatelliteTV.cn": ("河南卫视",),
    "HubeiSatelliteTV.cn": ("湖北卫视",),
    "ChongqingSatelliteTV.cn": ("重庆卫视",),
    "FujianSoutheastTV.cn": ("东南卫视",),
    "HeilongjiangSatelliteTV.cn": ("黑龙江卫视",),
    "LiaoningSports.local": ("辽宁体育",),
    "LiaoningMetro.local": ("辽宁都市",),
    "LiaoningMovie.local": ("辽宁影视", "辽宁影视剧"),
    "LiaoningPublic.local": ("辽宁公共",),
    "ShanghaiFinance.local": ("第一财经", "上海第一财经", "东方财经"),
    "ShanghaiNews.local": ("上海新闻综合", "上海新闻"),
    "ShanghaiMetro.local": ("上海都市",),
    "ShanghaiSports.local": ("五星体育",),
    "ShanghaiDocumentary.local": ("纪实人文", "上海纪实人文", "新纪实"),
    "ShanghaiDrama.local": ("都市剧场",),
    "ShanghaiLife.local": ("生活时尚",),
    "ShanghaiAnimation.local": ("哈哈炫动", "炫动卡通"),
    "ShanghaiMovie.local": ("东方影视",),
    "HunanCity.local": ("湖南都市",),
    "HunanEconomy.local": ("湖南经视",),
    "HunanDrama.local": ("湖南电视剧",),
    "HunanEntertainment.local": ("湖南娱乐", "湖南娱乐频道"),
    "HunanPublic.local": ("湖南公共",),
    "JinyingDocumentary.local": ("金鹰纪实",),
    "JinyingCartoon.local": ("金鹰卡通",),
    "JiangsuSports.local": ("江苏体育", "江苏体育休闲"),
    "JiangsuPublic.local": ("江苏公共",),
    "JiangsuCity.local": ("江苏城市",),
    "JiangsuMovie.local": ("江苏影视",),
    "JiangsuEducation.local": ("江苏教育",),
    "YoumanCartoon.local": ("优漫卡通",),
    "GuangdongPearl.local": ("广东珠江", "珠江卫视"),
    "GuangdongSports.local": ("广东体育",),
    "GuangdongNews.local": ("广东新闻",),
    "GuangdongPeople.local": ("广东民生",),
    "GuangzhouGeneral.local": ("广州综合",),
    "GuangzhouMovie.local": ("广州影视",),
}
CONTEXTUAL_CHANNEL_ALIAS_RULES = (
    (
        "上海",
        {
            "新闻综合": "ShanghaiNews.local",
            "新闻": "ShanghaiNews.local",
        },
    ),
)


@dataclasses.dataclass(frozen=True)
class Candidate:
    source: str
    url: str
    title: str
    channel_id: str | None = None
    feed_id: str | None = None
    country: str | None = None
    languages: tuple[str, ...] = ()
    categories: tuple[str, ...] = ()
    quality: str | None = None
    user_agent: str | None = None
    referrer: str | None = None
    group_title: str | None = None
    logo: str | None = None
    website: str | None = None
    channel_group: str | None = None

    def request_headers(self) -> dict[str, str]:
        headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.6",
            "Connection": "close",
            "User-Agent": self.user_agent or DEFAULT_USER_AGENT,
        }
        if self.referrer:
            headers["Referer"] = self.referrer
        return headers


@dataclasses.dataclass(frozen=True)
class ProbeResult:
    ok: bool
    status: int | None
    content_type: str | None
    detail: str
    elapsed_ms: int
    final_url: str | None = None
    via_ffprobe: bool = False
    playlist_ms: int | None = None
    media_ms: int | None = None
    startup_score: int = 0
    live_score: int = 0
    content_score: int = 50
    history_local_score: float = 0.0
    history_cloud_score: float = 0.0
    anomaly_flags: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class FetchResult:
    status: int | None
    content_type: str | None
    final_url: str
    body: bytes


@dataclasses.dataclass(frozen=True)
class PlaylistSnapshot:
    media_sequence: int | None
    target_duration: int | None
    segment_keys: tuple[str, ...]


class CacheStore:
    def __init__(self, root: Path | None, ttl_seconds: int) -> None:
        self.root = root
        self.ttl_seconds = ttl_seconds
        if self.root:
            self.root.mkdir(parents=True, exist_ok=True)

    def load_bytes(self, url: str, timeout: float) -> bytes:
        if not self.root:
            return fetch_bytes(url, timeout=timeout)
        cache_path = self.root / hashlib.sha256(url.encode("utf-8")).hexdigest()
        if cache_path.exists():
            age_seconds = time.time() - cache_path.stat().st_mtime
            if age_seconds <= self.ttl_seconds:
                return cache_path.read_bytes()
        data = fetch_bytes(url, timeout=timeout)
        cache_path.write_bytes(data)
        return data


class HistoryStore:
    def __init__(self, path: Path | None) -> None:
        self.path = path
        self.payload: dict[str, Any] = {
            "version": 1,
            "updated_at": None,
            "streams": {},
        }
        if not self.path or not self.path.exists():
            return
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if isinstance(loaded, dict) and isinstance(loaded.get("streams"), dict):
            self.payload = loaded

    def _default_stats(self) -> dict[str, Any]:
        return {
            "runs": 0,
            "successes": 0,
            "failures": 0,
            "ffprobe_successes": 0,
            "anomaly_hits": 0,
            "elapsed_total_ms": 0,
            "playlist_total_ms": 0,
            "media_total_ms": 0,
            "startup_score_total": 0,
            "live_score_total": 0,
            "content_score_total": 0,
            "last_ok": False,
            "last_detail": "",
            "last_updated": None,
        }

    def _entry(self, candidate: Candidate) -> dict[str, Any]:
        streams = self.payload.setdefault("streams", {})
        entry = streams.setdefault(
            candidate.url,
            {
                "title": candidate.title,
                "channel_id": candidate.channel_id,
                "group": candidate.channel_group,
                "source": candidate.source,
                "environments": {},
            },
        )
        entry["title"] = candidate.title
        entry["channel_id"] = candidate.channel_id
        entry["group"] = candidate.channel_group
        entry["source"] = candidate.source
        return entry

    def stats(self, url: str, environment: str) -> dict[str, Any]:
        entry = self.payload.get("streams", {}).get(url, {})
        envs = entry.get("environments", {})
        stats = envs.get(environment)
        return stats if isinstance(stats, dict) else {}

    def score(self, url: str, environment: str) -> float:
        stats = self.stats(url, environment)
        runs = int(stats.get("runs", 0) or 0)
        if runs <= 0:
            return 0.0
        successes = int(stats.get("successes", 0) or 0)
        success_rate = successes / runs
        avg_elapsed = int(stats.get("elapsed_total_ms", 0) or 0) / runs
        avg_startup = int(stats.get("startup_score_total", 0) or 0) / runs
        avg_live = int(stats.get("live_score_total", 0) or 0) / runs
        avg_content = int(stats.get("content_score_total", 0) or 0) / runs
        anomaly_rate = int(stats.get("anomaly_hits", 0) or 0) / runs
        ffprobe_rate = int(stats.get("ffprobe_successes", 0) or 0) / runs
        if avg_elapsed <= 1200:
            speed_component = 18.0
        elif avg_elapsed <= 2500:
            speed_component = 14.0
        elif avg_elapsed <= 5000:
            speed_component = 10.0
        elif avg_elapsed <= 9000:
            speed_component = 6.0
        else:
            speed_component = 2.0
        score = (
            success_rate * 55.0
            + ffprobe_rate * 10.0
            + speed_component
            + avg_startup * 0.12
            + avg_live * 0.18
            + avg_content * 0.1
            - anomaly_rate * 15.0
        )
        return round(max(0.0, min(100.0, score)), 2)

    def record(self, candidate: Candidate, probe: ProbeResult, environment: str) -> None:
        environment = environment if environment in PROBE_ENVIRONMENTS else "local"
        entry = self._entry(candidate)
        envs = entry.setdefault("environments", {})
        stats = envs.setdefault(environment, self._default_stats())
        stats["runs"] += 1
        if probe.ok:
            stats["successes"] += 1
        else:
            stats["failures"] += 1
        if probe.ok and probe.via_ffprobe:
            stats["ffprobe_successes"] += 1
        stats["anomaly_hits"] += len(probe.anomaly_flags)
        stats["elapsed_total_ms"] += max(0, probe.elapsed_ms)
        if probe.playlist_ms is not None:
            stats["playlist_total_ms"] += max(0, probe.playlist_ms)
        if probe.media_ms is not None:
            stats["media_total_ms"] += max(0, probe.media_ms)
        stats["startup_score_total"] += probe.startup_score
        stats["live_score_total"] += probe.live_score
        stats["content_score_total"] += probe.content_score
        stats["last_ok"] = probe.ok
        stats["last_detail"] = probe.detail
        stats["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def save(self) -> None:
        if not self.path:
            return
        self.payload["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect public IPTV candidates, filter Chinese-language channels, "
            "probe availability, and export a verified M3U playlist."
        )
    )
    parser.add_argument(
        "--provider",
        action="append",
        choices=("iptv-org", "cctv-official", "curated-public", "published", "manual-preferred"),
        default=["iptv-org", "cctv-official", "curated-public", "published", "manual-preferred"],
        help="Candidate source provider. Repeat to add more providers.",
    )
    parser.add_argument(
        "--remote-m3u",
        action="append",
        default=[],
        help="Extra remote M3U playlist URL to merge into the candidate pool.",
    )
    parser.add_argument(
        "--local-m3u",
        action="append",
        default=[],
        help="Extra local M3U playlist file to merge into the candidate pool.",
    )
    parser.add_argument(
        "--out",
        default="output/chinese-public-verified.m3u",
        help="Output M3U path for verified streams.",
    )
    parser.add_argument(
        "--report",
        default="output/chinese-public-report.json",
        help="Output JSON report path for probe results.",
    )
    parser.add_argument(
        "--backup-out",
        default="output/chinese-public-with-backups.m3u",
        help="Output M3U path that keeps the primary source plus backups per channel.",
    )
    parser.add_argument(
        "--backup-count",
        type=int,
        default=3,
        help="How many verified sources to keep per channel in the backup playlist.",
    )
    parser.add_argument(
        "--history",
        default=str(DEFAULT_HISTORY_PATH),
        help="Persistent JSON history used for long-term stability scoring.",
    )
    parser.add_argument(
        "--probe-environment",
        choices=PROBE_ENVIRONMENTS,
        default=DEFAULT_PROBE_ENVIRONMENT if DEFAULT_PROBE_ENVIRONMENT in PROBE_ENVIRONMENTS else "local",
        help="Label this run as local or cloud for separate historical scoring.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=300,
        help="Maximum number of ranked candidates to probe. Use 0 for exhaustive mode.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=8.0,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(32, max(8, (os.cpu_count() or 4) * 4)),
        help="Concurrent probe workers.",
    )
    parser.add_argument(
        "--min-quality",
        type=int,
        default=0,
        help="Drop streams below this numeric quality value, for example 720.",
    )
    parser.add_argument(
        "--allow-ip-hosts",
        action="store_true",
        help="Keep streams served from raw IP addresses instead of domain names.",
    )
    parser.add_argument(
        "--cache-dir",
        default=".cache",
        help="Directory used to cache provider downloads. Empty string disables cache.",
    )
    parser.add_argument(
        "--cache-ttl",
        type=int,
        default=3600,
        help="Provider cache TTL in seconds.",
    )
    parser.add_argument(
        "--ffprobe",
        action="store_true",
        help="Use ffprobe when available for a deeper validation pass.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=1,
        help="Retry transient network failures this many times.",
    )
    parser.add_argument(
        "--stability-checks",
        type=int,
        default=2,
        help="Require this many successful probe passes for a channel to be kept.",
    )
    parser.add_argument(
        "--live-sequence-delay",
        type=float,
        default=2.5,
        help="Seconds to wait before rechecking an HLS media playlist for sequence movement.",
    )
    parser.add_argument(
        "--content-check-timeout",
        type=float,
        default=6.0,
        help="Timeout for ffmpeg-based content anomaly detection on verified candidates.",
    )
    parser.add_argument(
        "--include-nsfw",
        action="store_true",
        help="Keep NSFW or xxx-tagged entries.",
    )
    parser.add_argument(
        "--keep-failures",
        action="store_true",
        help="Keep failed entries in the JSON report.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress logs to stderr.",
    )
    return parser.parse_args()


def log(message: str, verbose: bool = True) -> None:
    if verbose:
        print(message, file=sys.stderr)


def fetch_bytes(url: str, timeout: float, headers: dict[str, str] | None = None) -> bytes:
    request = Request(url, headers=headers or {"User-Agent": DEFAULT_USER_AGENT})
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def fetch_json(url: str, cache: CacheStore, timeout: float) -> Any:
    return json.loads(cache.load_bytes(url, timeout=timeout).decode("utf-8"))


def fetch_text(url: str, cache: CacheStore, timeout: float) -> str:
    return cache.load_bytes(url, timeout=timeout).decode("utf-8", errors="replace")


def contains_han(text: str | None) -> bool:
    return bool(text and HAN_RE.search(text))


def text_looks_chinese(text: str | None) -> bool:
    if not text:
        return False
    normalized = text.lower()
    return contains_han(text) or any(keyword in normalized for keyword in CHINESE_HINT_KEYWORDS)


def normalize_url(url: str) -> str:
    value = url.strip()
    if not value:
        return value
    try:
        parts = urlsplit(value)
    except ValueError:
        return value
    if not parts.scheme or not parts.netloc:
        return value
    try:
        query = urlencode(parse_qsl(parts.query, keep_blank_values=True), doseq=True)
    except ValueError:
        query = quote(parts.query, safe="=&/%:@+$,;?-._~")
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            quote(parts.path or "/", safe="/%:@+$,;=-._~"),
            query,
            quote(parts.fragment, safe=""),
        )
    )


def url_has_suffix(url: str, suffixes: tuple[str, ...]) -> bool:
    lowered = url.lower()
    return any(lowered.endswith(suffix) or f"{suffix}?" in lowered for suffix in suffixes)


def url_looks_like_hls(url: str) -> bool:
    lowered = url.lower()
    return ".m3u8" in lowered or ".m3u" in lowered


def url_looks_like_vod(url: str) -> bool:
    lowered = url.lower()
    if url_has_suffix(lowered, DIRECT_FILE_URL_HINTS):
        return True
    return any(pattern in lowered for pattern in VOD_URL_PATTERNS)


def live_url_rank(url: str) -> int:
    lowered = url.lower()
    if ".m3u8" in lowered:
        return 3
    if any(hint in lowered for hint in LIVE_URL_HINTS):
        return 2
    return 1 if lowered.startswith(("http://", "https://")) else 0


def is_blocked_candidate_url(url: str) -> bool:
    lowered = url.lower()
    return any(pattern in lowered for pattern in BLOCKED_CANDIDATE_URL_PATTERNS)


def safe_tuple(values: Iterable[str] | None) -> tuple[str, ...]:
    return tuple(sorted({value for value in (values or []) if value}))


def quality_value(quality: str | None) -> int:
    if not quality:
        return 0
    match = re.search(r"(\d+)", quality)
    return int(match.group(1)) if match else 0


def quality_tier(quality: str | None) -> int:
    value = quality_value(quality)
    if value >= 1080:
        return 3
    if value >= 720:
        return 2
    if value >= 480:
        return 1
    return 0


def infer_quality(*texts: str | None) -> str | None:
    searchable = " ".join(text.lower() for text in texts if text)
    if "8k" in searchable:
        return "4320p"
    if "4k" in searchable or "uhd" in searchable or "超高清" in searchable:
        return "2160p"
    if "1080" in searchable or "高清" in searchable:
        return "1080p"
    return None


def host_is_ip_address(url: str) -> bool:
    hostname = urlparse(url).hostname or ""
    return bool(IP_HOST_RE.fullmatch(hostname))


def canonicalize_channel_alias(text: str | None) -> str:
    if not text:
        return ""
    value = text.strip().lower()
    for token in (
        "超高清",
        "超清",
        "高清",
        "uhd",
        "fhd",
        "hd",
        "标清",
        "频道",
        "电视台",
        "直播",
        "卫视频道",
        "央视台",
        "卫视台",
    ):
        value = value.replace(token, "")
    value = value.replace("＋", "+")
    value = re.sub(r"[\s_\-()/\[\]【】·:：,，.。'\"|]+", "", value)
    value = re.sub(r"\d{3,4}p", "", value)
    value = value.replace("4k超", "").replace("4k", "").replace("8k", "")
    return value


CONTEXTUAL_ALIAS_TO_ID = [
    (
        canonicalize_channel_alias(context),
        {
            canonicalize_channel_alias(alias): channel_id
            for alias, channel_id in mapping.items()
            if canonicalize_channel_alias(alias)
        },
    )
    for context, mapping in CONTEXTUAL_CHANNEL_ALIAS_RULES
]


CHANNEL_ALIAS_TO_ID = {
    canonicalize_channel_alias(alias): channel_id
    for channel_id, aliases in {
        **{channel_id: (label, *TARGET_CHANNEL_ALIASES.get(channel_id, ())) for channel_id, label in TARGET_CHANNEL_LABELS.items()}
    }.items()
    for alias in aliases
    if canonicalize_channel_alias(alias)
}


def source_priority(source: str) -> int:
    if source == "manual-preferred":
        return 4
    if source == "published":
        return 3
    if source.startswith("local:"):
        return 2
    if source.startswith("remote:"):
        return 1
    return 0


def source_is_known_slow(url: str) -> bool:
    lowered = url.lower()
    return any(pattern in lowered for pattern in KNOWN_SLOW_SOURCE_PATTERNS)


def latency_rank(elapsed_ms: int) -> int:
    if elapsed_ms <= 1200:
        return 5
    if elapsed_ms <= 2500:
        return 4
    if elapsed_ms <= 5000:
        return 3
    if elapsed_ms <= 9000:
        return 2
    if elapsed_ms <= 15000:
        return 1
    return 0


def candidate_rank(candidate: Candidate) -> tuple[int, int, int, int, int, int, int, int, int, int]:
    preferred_patterns = PREFERRED_URL_PATTERNS_BY_TITLE.get(candidate.title, ())
    preferred_rank = int(any(pattern in candidate.url for pattern in preferred_patterns))
    live_rank = live_url_rank(candidate.url)
    non_vod_rank = int(not url_looks_like_vod(candidate.url))
    language_match = int(bool(set(candidate.languages) & CHINESE_LANGUAGE_CODES))
    region_match = int(candidate.country in CHINESE_REGION_CODES)
    han_title = int(contains_han(candidate.title))
    domain_match = int(not host_is_ip_address(candidate.url))
    https_match = int(candidate.url.startswith("https://"))
    return (
        source_priority(candidate.source),
        preferred_rank,
        non_vod_rank,
        live_rank,
        language_match,
        domain_match,
        https_match,
        region_match,
        quality_tier(candidate.quality),
        han_title,
    )


def target_channel_group(channel_id: str | None) -> str | None:
    if not channel_id:
        return None
    if channel_id in DISABLED_CHANNEL_IDS:
        return None
    if channel_id in SUBCHANNEL_GROUPS:
        return SUBCHANNEL_GROUPS[channel_id]
    if channel_id in CCTV_CHANNEL_LABELS:
        return "央视"
    if channel_id in SATELLITE_CHANNEL_LABELS:
        return "卫视"
    return None


def is_target_channel(channel: dict[str, Any] | None) -> bool:
    if not channel:
        return False
    if channel.get("country") != "CN":
        return False
    channel_id = channel.get("id")
    return channel_id in TARGET_CHANNEL_LABELS and channel_id not in DISABLED_CHANNEL_IDS


def choose_display_title(
    channel_id: str | None,
    stream_title: str | None,
    channel: dict[str, Any] | None,
) -> str:
    if channel_id in DISABLED_CHANNEL_IDS:
        return ""
    if channel_id in TARGET_CHANNEL_LABELS:
        return TARGET_CHANNEL_LABELS[channel_id]

    preferred = [stream_title or ""]
    if channel:
        preferred.extend(channel.get("alt_names") or [])
        preferred.append(channel.get("name") or "")
    for name in preferred:
        if contains_han(name):
            return name.strip()
    return (stream_title or (channel or {}).get("name") or "").strip()


def verified_item_rank(
    item: tuple[Candidate, ProbeResult],
) -> tuple[int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int]:
    candidate, probe = item
    preferred_patterns = PREFERRED_URL_PATTERNS_BY_TITLE.get(candidate.title, ())
    preferred_rank = 0
    for index, pattern in enumerate(preferred_patterns):
        if pattern in candidate.url:
            preferred_rank = len(preferred_patterns) - index
            break
    probe_confidence = int("slow" not in probe.detail.lower())
    ffprobe_video = int(probe.via_ffprobe and "video" in probe.detail.lower())
    fast_probe = latency_rank(probe.elapsed_ms)
    clean_content = int(not any(flag in probe.anomaly_flags for flag in ("black-frame", "frozen-frames", "ended-playlist")))
    return (
        ffprobe_video,
        clean_content,
        preferred_rank,
        source_priority(candidate.source),
        int(probe.history_local_score * 100),
        int(probe.history_cloud_score * 100),
        probe.live_score,
        probe.content_score,
        probe.startup_score,
        probe_confidence,
        fast_probe,
        int(not url_looks_like_vod(candidate.url)),
        live_url_rank(candidate.url),
        int(not source_is_known_slow(candidate.url)),
        int(not host_is_ip_address(candidate.url)),
        int(candidate.url.startswith("https://")),
        quality_tier(candidate.quality),
        -len(probe.anomaly_flags),
        -probe.elapsed_ms,
    )


def collapse_verified_items(
    verified_items: list[tuple[Candidate, ProbeResult]],
) -> tuple[list[tuple[Candidate, ProbeResult]], list[list[tuple[Candidate, ProbeResult]]]]:
    by_title: dict[str, list[tuple[Candidate, ProbeResult]]] = {}
    for item in verified_items:
        title = item[0].title
        by_title.setdefault(title, []).append(item)

    grouped = []
    for items in by_title.values():
        items.sort(key=verified_item_rank, reverse=True)
        grouped.append(items)

    grouped.sort(
        key=lambda items: (
            GROUP_SORT_ORDER.get(items[0][0].channel_group or "", 99),
            items[0][0].channel_group or "",
            items[0][0].title,
        )
    )
    return [items[0] for items in grouped], grouped


def best_candidate(existing: Candidate, challenger: Candidate) -> Candidate:
    return challenger if candidate_rank(challenger) > candidate_rank(existing) else existing


def dedupe_candidates(candidates: Iterable[Candidate]) -> list[Candidate]:
    deduped: dict[str, Candidate] = {}
    for candidate in candidates:
        deduped[candidate.url] = (
            best_candidate(deduped[candidate.url], candidate)
            if candidate.url in deduped
            else candidate
        )
    return list(deduped.values())


def build_candidate(
    *,
    source: str,
    url: str,
    title: str,
    channel_id: str | None = None,
    feed_id: str | None = None,
    country: str | None = None,
    languages: Iterable[str] | None = None,
    categories: Iterable[str] | None = None,
    quality: str | None = None,
    user_agent: str | None = None,
    referrer: str | None = None,
    group_title: str | None = None,
    logo: str | None = None,
    website: str | None = None,
    channel_group: str | None = None,
) -> Candidate:
    return Candidate(
        source=source,
        url=normalize_url(url),
        title=title.strip() or url.strip(),
        channel_id=channel_id,
        feed_id=feed_id,
        country=country,
        languages=safe_tuple(languages),
        categories=safe_tuple(categories),
        quality=quality,
        user_agent=user_agent,
        referrer=referrer,
        group_title=group_title,
        logo=logo,
        website=website,
        channel_group=channel_group,
    )


def load_iptv_org_candidates(
    cache: CacheStore,
    timeout: float,
    include_nsfw: bool,
    min_quality: int,
    allow_ip_hosts: bool,
) -> list[Candidate]:
    channels = fetch_json("https://iptv-org.github.io/api/channels.json", cache, timeout)
    feeds = fetch_json("https://iptv-org.github.io/api/feeds.json", cache, timeout)
    streams = fetch_json("https://iptv-org.github.io/api/streams.json", cache, timeout)

    channels_by_id = {item["id"]: item for item in channels}
    feeds_by_key = {(item["channel"], item["id"]): item for item in feeds if item.get("channel")}

    candidates: list[Candidate] = []
    for stream in streams:
        url = normalize_url(stream.get("url") or "")
        if not url or urlparse(url).scheme not in {"http", "https"}:
            continue
        if is_blocked_candidate_url(url):
            continue
        if not allow_ip_hosts and host_is_ip_address(url):
            continue
        if quality_value(stream.get("quality")) < min_quality:
            continue

        channel = channels_by_id.get(stream.get("channel")) if stream.get("channel") else None
        if not is_target_channel(channel):
            continue
        feed = (
            feeds_by_key.get((stream.get("channel"), stream.get("feed")))
            if stream.get("channel") and stream.get("feed")
            else None
        )

        categories = safe_tuple((channel or {}).get("categories"))
        if not include_nsfw:
            if (channel or {}).get("is_nsfw"):
                continue
            if "xxx" in categories:
                continue

        channel_id = (channel or {}).get("id")
        title = choose_display_title(channel_id, stream.get("title"), channel)
        languages = safe_tuple((feed or {}).get("languages"))
        country = (channel or {}).get("country")
        channel_group = target_channel_group(channel_id)
        if not channel_group:
            continue

        candidates.append(
            build_candidate(
                source="iptv-org",
                url=url,
                title=title,
                channel_id=channel_id,
                feed_id=(feed or {}).get("id"),
                country=country,
                languages=languages or ("zho",),
                categories=categories,
                quality=stream.get("quality"),
                user_agent=stream.get("user_agent"),
                referrer=stream.get("referrer"),
                group_title=channel_group,
                website=(channel or {}).get("website"),
                channel_group=channel_group,
            )
        )
    return dedupe_candidates(candidates)


def build_cctv_official_variants(url: str) -> list[tuple[str, str, str]]:
    variants: list[tuple[str, str, str]] = []
    if "cdrmldcctv1_1/index.m3u8" in url:
        variants.append(("CCTV1.cn", "CCTV-1 综合", url))
        variants.append(
            (
                "CCTV13.cn",
                "CCTV-13 新闻",
                url.replace("cdrmldcctv1_1/index.m3u8", "cdrmldcctv13_1/index.m3u8"),
            )
        )
    elif "ldcctv1_2/index.m3u8" in url:
        variants.append(("CCTV1.cn", "CCTV-1 综合", url))
        variants.append(
            (
                "CCTV13.cn",
                "CCTV-13 新闻",
                url.replace("ldcctv1_2/index.m3u8", "ldcctv13_2/index.m3u8"),
            )
        )
    return variants


def load_cctv_official_candidates(
    cache: CacheStore,
    timeout: float,
    min_quality: int,
) -> list[Candidate]:
    js_text = fetch_text(CCTV_OFFICIAL_JS_URL, cache, timeout)
    raw_urls = sorted(
        {
            match
            for match in re.findall(r"http://[^\"']+index\.m3u8", js_text)
            if "ldncctv" in match
            and (
                "ldcctv1_2/index.m3u8" in match
                or "cdrmldcctv1_1/index.m3u8" in match
            )
        }
    )

    candidates: list[Candidate] = []
    for raw_url in raw_urls:
        for channel_id, title, variant_url in build_cctv_official_variants(raw_url):
            quality = "1080p" if "cdrm" in variant_url else "360p"
            if quality_value(quality) < min_quality:
                continue
            channel_group = target_channel_group(channel_id)
            candidates.append(
                build_candidate(
                    source="cctv-official",
                    url=variant_url,
                    title=title,
                    channel_id=channel_id,
                    country="CN",
                    languages=("zho",),
                    quality=quality,
                    group_title=channel_group,
                    website=CCTV_OFFICIAL_WEBSITES.get(channel_id),
                    channel_group=channel_group,
                )
            )
    return dedupe_candidates(candidates)


def match_target_channel_id(*texts: str | None, context: str | None = None) -> str | None:
    for text in texts:
        key = canonicalize_channel_alias(text)
        if not key:
            continue
        channel_id = CHANNEL_ALIAS_TO_ID.get(key)
        if channel_id and channel_id not in DISABLED_CHANNEL_IDS:
            return channel_id
    context_key = canonicalize_channel_alias(context)
    if context_key:
        for context_alias, mapping in CONTEXTUAL_ALIAS_TO_ID:
            if context_alias and context_alias in context_key:
                for text in texts:
                    key = canonicalize_channel_alias(text)
                    channel_id = mapping.get(key)
                    if channel_id and channel_id not in DISABLED_CHANNEL_IDS:
                        return channel_id
    return None


def extract_inline_headers(attrs: dict[str, str]) -> tuple[str | None, str | None]:
    user_agent = attrs.get("http-user-agent") or attrs.get("user-agent")
    referrer = (
        attrs.get("http-referrer")
        or attrs.get("referrer")
        or attrs.get("referer")
    )
    raw_header = attrs.get("http-header") or ""
    if raw_header and not referrer:
        for chunk in raw_header.split("&"):
            key, _, value = chunk.partition("=")
            if key.strip().lower() == "referer" and value:
                referrer = value.strip()
                break
    return user_agent, referrer


def parse_extinf(line: str) -> tuple[dict[str, str], str]:
    payload = line[len("#EXTINF:") :]
    info_blob, _, title = payload.partition(",")
    attrs = {key.lower(): value for key, value in EXTINF_ATTR_RE.findall(info_blob)}
    return attrs, title.strip()


def load_m3u_file(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def load_extra_m3u_candidates(
    content: str,
    source_name: str,
    include_nsfw: bool,
    min_quality: int,
    allow_ip_hosts: bool,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    pending_attrs: dict[str, str] = {}
    pending_title = ""
    pending_user_agent: str | None = None
    pending_referrer: str | None = None

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF:"):
            pending_attrs, pending_title = parse_extinf(line)
            pending_user_agent = None
            pending_referrer = None
            continue
        if line.startswith("#EXTVLCOPT:"):
            option = line[len("#EXTVLCOPT:") :]
            if option.startswith("http-user-agent="):
                pending_user_agent = option.split("=", 1)[1].strip()
            elif option.startswith("http-referrer="):
                pending_referrer = option.split("=", 1)[1].strip()
            continue
        if line.startswith("#"):
            continue

        url = normalize_url(line)
        if urlparse(url).scheme not in {"http", "https"}:
            continue
        if is_blocked_candidate_url(url):
            pending_attrs = {}
            pending_title = ""
            pending_user_agent = None
            pending_referrer = None
            continue
        if not allow_ip_hosts and host_is_ip_address(url):
            pending_attrs = {}
            pending_title = ""
            pending_user_agent = None
            pending_referrer = None
            continue

        inline_user_agent, inline_referrer = extract_inline_headers(pending_attrs)
        user_agent = pending_user_agent or inline_user_agent
        referrer = pending_referrer or inline_referrer
        raw_title = pending_title or pending_attrs.get("tvg-name") or url
        group_title = pending_attrs.get("group-title")
        country = pending_attrs.get("tvg-country") or None
        quality = pending_attrs.get("tvg-quality") or infer_quality(
            raw_title,
            pending_attrs.get("tvg-name"),
            pending_attrs.get("tvg-id"),
            group_title,
        )
        if quality_value(quality) < min_quality:
            pending_attrs = {}
            pending_title = ""
            pending_user_agent = None
            pending_referrer = None
            continue

        if not include_nsfw:
            searchable = " ".join([raw_title, group_title or ""]).lower()
            if "xxx" in searchable or "adult" in searchable:
                pending_attrs = {}
                pending_title = ""
                pending_user_agent = None
                pending_referrer = None
                continue

        searchable = " ".join(
            [
                raw_title,
                group_title or "",
                pending_attrs.get("tvg-name", ""),
                pending_attrs.get("tvg-id", ""),
            ]
        )
        channel_id = match_target_channel_id(
            raw_title,
            pending_attrs.get("tvg-name"),
            pending_attrs.get("tvg-id"),
            context=group_title,
        )
        if not channel_id:
            pending_attrs = {}
            pending_title = ""
            pending_user_agent = None
            pending_referrer = None
            continue
        title = TARGET_CHANNEL_LABELS[channel_id]
        channel_group = target_channel_group(channel_id)

        candidates.append(
            build_candidate(
                source=source_name,
                url=url,
                title=title,
                channel_id=channel_id,
                country=country or "CN",
                languages=("zho",) if contains_han(searchable) else (),
                quality=quality,
                user_agent=user_agent,
                referrer=referrer,
                group_title=channel_group,
                logo=pending_attrs.get("tvg-logo") or None,
                channel_group=channel_group,
            )
        )
        pending_attrs = {}
        pending_title = ""
        pending_user_agent = None
        pending_referrer = None
    return dedupe_candidates(candidates)


def load_curated_public_candidates(
    cache: CacheStore,
    timeout: float,
    include_nsfw: bool,
    min_quality: int,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    for name, url in CURATED_PUBLIC_M3U_URLS.items():
        text = fetch_text(url, cache, timeout=timeout)
        candidates.extend(
            load_extra_m3u_candidates(
                text,
                source_name=f"curated:{name}",
                include_nsfw=include_nsfw,
                min_quality=min_quality,
                allow_ip_hosts=True,
            )
        )
    return dedupe_candidates(candidates)


def load_published_candidates(
    include_nsfw: bool,
    min_quality: int,
    allow_ip_hosts: bool,
) -> list[Candidate]:
    if not PUBLISHED_PLAYLIST_PATH.exists():
        return []
    return load_extra_m3u_candidates(
        load_m3u_file(PUBLISHED_PLAYLIST_PATH),
        source_name="published",
        include_nsfw=include_nsfw,
        min_quality=min_quality,
        allow_ip_hosts=allow_ip_hosts,
    )


def load_manual_preferred_candidates() -> list[Candidate]:
    candidates: list[Candidate] = []
    for item in MANUAL_PREFERRED_CANDIDATES:
        channel_id = item["channel_id"]
        channel_group = target_channel_group(channel_id)
        if not channel_group:
            continue
        candidates.append(
            build_candidate(
                source="manual-preferred",
                url=item["url"],
                title=item["title"],
                channel_id=channel_id,
                country="CN",
                languages=("zho",),
                quality="1080p",
                group_title=channel_group,
                channel_group=channel_group,
            )
        )
    return dedupe_candidates(candidates)


def choose_playlist_target(playlist_text: str) -> tuple[str | None, bool]:
    items = [line.strip() for line in playlist_text.splitlines() if line.strip() and not line.startswith("#")]
    is_master = "#EXT-X-STREAM-INF" in playlist_text
    if not items:
        return None, is_master
    return (items[0] if is_master else items[-1]), is_master


def playlist_is_ended(playlist_text: str) -> bool:
    normalized = playlist_text.upper()
    return "#EXT-X-ENDLIST" in normalized or "#EXT-X-PLAYLIST-TYPE:VOD" in normalized


def playlist_segments(playlist_text: str) -> tuple[str, ...]:
    return tuple(
        line.strip()
        for line in playlist_text.splitlines()
        if line.strip() and not line.startswith("#")
    )


def parse_playlist_snapshot(playlist_text: str) -> PlaylistSnapshot:
    segments = playlist_segments(playlist_text)
    media_sequence_match = PLAYLIST_MEDIA_SEQUENCE_RE.search(playlist_text)
    target_duration_match = PLAYLIST_TARGET_DURATION_RE.search(playlist_text)
    return PlaylistSnapshot(
        media_sequence=int(media_sequence_match.group(1)) if media_sequence_match else None,
        target_duration=int(target_duration_match.group(1)) if target_duration_match else None,
        segment_keys=segments[-3:],
    )


def compute_startup_score(playlist_ms: int | None, media_ms: int | None) -> int:
    def bucket(value: int | None) -> int:
        if value is None:
            return 10
        if value <= 400:
            return 50
        if value <= 900:
            return 44
        if value <= 1600:
            return 36
        if value <= 2600:
            return 28
        if value <= 4500:
            return 20
        if value <= 8000:
            return 12
        return 4

    playlist_score = bucket(playlist_ms)
    media_score = bucket(media_ms)
    return min(100, int(playlist_score * 0.4 + media_score * 0.6))


def merge_probe_results(base: ProbeResult, extra: ProbeResult) -> ProbeResult:
    detail_parts = [base.detail]
    if extra.detail and extra.detail not in detail_parts:
        detail_parts.append(extra.detail)
    anomaly_flags = tuple(sorted(set(base.anomaly_flags + extra.anomaly_flags)))
    return ProbeResult(
        ok=base.ok and extra.ok,
        status=extra.status or base.status,
        content_type=extra.content_type or base.content_type,
        detail="; ".join(part for part in detail_parts if part),
        elapsed_ms=max(base.elapsed_ms, extra.elapsed_ms),
        final_url=extra.final_url or base.final_url,
        via_ffprobe=base.via_ffprobe or extra.via_ffprobe,
        playlist_ms=extra.playlist_ms if extra.playlist_ms is not None else base.playlist_ms,
        media_ms=extra.media_ms if extra.media_ms is not None else base.media_ms,
        startup_score=max(base.startup_score, extra.startup_score),
        live_score=max(base.live_score, extra.live_score),
        content_score=min(base.content_score, extra.content_score),
        history_local_score=max(base.history_local_score, extra.history_local_score),
        history_cloud_score=max(base.history_cloud_score, extra.history_cloud_score),
        anomaly_flags=anomaly_flags,
    )


def assess_playlist_progress(
    playlist_url: str,
    playlist_text: str,
    headers: dict[str, str],
    timeout: float,
    sequence_delay: float,
) -> tuple[int, tuple[str, ...], str, int]:
    if playlist_is_ended(playlist_text):
        return 0, ("ended-playlist",), "playlist looks like ended vod", 0

    snapshot = parse_playlist_snapshot(playlist_text)
    if not snapshot.segment_keys:
        return 15, ("empty-playlist",), "playlist has no media segments", 0

    wait_seconds = max(1.2, sequence_delay)
    if snapshot.target_duration:
        wait_seconds = min(wait_seconds, max(1.2, snapshot.target_duration / 2))
    time.sleep(wait_seconds)

    try:
        follow_up, fetch_ms = timed_http_fetch(
            playlist_url,
            headers,
            timeout,
            max_bytes=TEXT_SAMPLE_LIMIT,
        )
    except (TimeoutError, URLError, OSError) as error:
        return 25, ("recheck-timeout",), f"playlist recheck slow ({error})", 0

    if not ok_status(follow_up.status):
        return 20, ("recheck-http",), f"playlist recheck http {follow_up.status}", fetch_ms

    follow_text = follow_up.body.decode("utf-8", errors="ignore")
    if playlist_is_ended(follow_text):
        return 0, ("ended-playlist",), "playlist recheck looks like ended vod", fetch_ms

    follow_snapshot = parse_playlist_snapshot(follow_text)
    if (
        snapshot.media_sequence is not None
        and follow_snapshot.media_sequence is not None
        and follow_snapshot.media_sequence > snapshot.media_sequence
    ):
        return 100, (), "media sequence advanced", fetch_ms
    if follow_snapshot.segment_keys != snapshot.segment_keys:
        return 85, (), "playlist segments rotated", fetch_ms
    if len(set(follow_snapshot.segment_keys)) <= 1:
        return 25, ("repeating-segments",), "playlist repeats the same segment", fetch_ms
    return 45, ("stale-playlist",), "playlist did not prove forward movement", fetch_ms


def probe_hls_candidate(candidate: Candidate, timeout: float, sequence_delay: float) -> ProbeResult:
    headers = candidate.request_headers()
    start = time.perf_counter()
    try:
        top, top_ms = timed_http_fetch(candidate.url, headers, timeout, max_bytes=TEXT_SAMPLE_LIMIT)
        kind = classify_response(top.content_type, top.final_url, top.body)
        if not ok_status(top.status):
            return ProbeResult(
                ok=False,
                status=top.status,
                content_type=top.content_type,
                detail="unexpected http status",
                elapsed_ms=int((time.perf_counter() - start) * 1000),
                final_url=top.final_url,
                playlist_ms=top_ms,
            )
        if kind != "hls":
            return ProbeResult(
                ok=False,
                status=top.status,
                content_type=top.content_type,
                detail="endpoint is not a validated live playlist",
                elapsed_ms=int((time.perf_counter() - start) * 1000),
                final_url=top.final_url,
                playlist_ms=top_ms,
            )

        playlist_text = top.body.decode("utf-8", errors="ignore")
        if playlist_is_ended(playlist_text):
            return ProbeResult(
                ok=False,
                status=top.status,
                content_type=top.content_type,
                detail="playlist looks like ended vod, not live tv",
                elapsed_ms=int((time.perf_counter() - start) * 1000),
                final_url=top.final_url,
                playlist_ms=top_ms,
            )

        child, is_master = choose_playlist_target(playlist_text)
        detail = "playlist reachable"
        final_url = top.final_url
        media_ms: int | None = None
        progress_url = top.final_url
        progress_text = playlist_text
        anomaly_flags: tuple[str, ...] = ()

        if child:
            child_url = normalize_url(urljoin(top.final_url, child))
            try:
                child_fetch, child_ms = timed_http_fetch(
                    child_url,
                    headers,
                    timeout,
                    max_bytes=TEXT_SAMPLE_LIMIT if is_master else MEDIA_SAMPLE_LIMIT,
                    range_request=not is_master,
                )
            except (TimeoutError, URLError, OSError) as error:
                return slow_playable_result(
                    status=top.status,
                    content_type=top.content_type,
                    detail=f"playlist reachable; child fetch slow but may still be playable ({error})",
                    elapsed_ms=int((time.perf_counter() - start) * 1000),
                    final_url=child_url,
                    playlist_ms=top_ms,
                )

            child_kind = classify_response(child_fetch.content_type, child_fetch.final_url, child_fetch.body)
            final_url = child_fetch.final_url

            if is_master and child_kind == "hls":
                child_playlist_text = child_fetch.body.decode("utf-8", errors="ignore")
                if playlist_is_ended(child_playlist_text):
                    return ProbeResult(
                        ok=False,
                        status=child_fetch.status,
                        content_type=child_fetch.content_type,
                        detail="variant playlist looks like ended vod, not live tv",
                        elapsed_ms=int((time.perf_counter() - start) * 1000),
                        final_url=child_fetch.final_url,
                        playlist_ms=top_ms,
                        media_ms=child_ms,
                    )
                progress_url = child_fetch.final_url
                progress_text = child_playlist_text
                grandchild, _ = choose_playlist_target(child_playlist_text)
                if not grandchild:
                    media_ms = child_ms
                    detail = "master playlist reachable"
                else:
                    segment_url = normalize_url(urljoin(child_fetch.final_url, grandchild))
                    try:
                        segment_fetch, segment_ms = timed_http_fetch(
                            segment_url,
                            headers,
                            timeout,
                            max_bytes=MEDIA_SAMPLE_LIMIT,
                            range_request=True,
                        )
                    except (TimeoutError, URLError, OSError) as error:
                        return slow_playable_result(
                            status=child_fetch.status,
                            content_type=child_fetch.content_type,
                            detail=f"variant playlist reachable; segment slow but may still be playable ({error})",
                            elapsed_ms=int((time.perf_counter() - start) * 1000),
                            final_url=child_fetch.final_url,
                            playlist_ms=top_ms,
                            media_ms=child_ms,
                        )
                    ok = ok_status(segment_fetch.status) and bool(segment_fetch.body)
                    if not ok:
                        return ProbeResult(
                            ok=False,
                            status=segment_fetch.status,
                            content_type=segment_fetch.content_type,
                            detail="variant segment fetch failed",
                            elapsed_ms=int((time.perf_counter() - start) * 1000),
                            final_url=segment_fetch.final_url,
                            playlist_ms=top_ms,
                            media_ms=segment_ms,
                        )
                    detail = "variant segment reachable"
                    final_url = segment_fetch.final_url
                    media_ms = segment_ms
            else:
                if not is_master and not child_fetch.body and ok_status(child_fetch.status):
                    return slow_playable_result(
                        status=child_fetch.status,
                        content_type=child_fetch.content_type,
                        detail="playlist reachable; segment response empty but may still be playable",
                        elapsed_ms=int((time.perf_counter() - start) * 1000),
                        final_url=child_fetch.final_url,
                        playlist_ms=top_ms,
                        media_ms=child_ms,
                    )
                ok = ok_status(child_fetch.status) and bool(child_fetch.body)
                if not ok:
                    return ProbeResult(
                        ok=False,
                        status=child_fetch.status,
                        content_type=child_fetch.content_type,
                        detail="playlist segment fetch failed",
                        elapsed_ms=int((time.perf_counter() - start) * 1000),
                        final_url=child_fetch.final_url,
                        playlist_ms=top_ms,
                        media_ms=child_ms,
                    )
                detail = "playlist segment reachable"
                media_ms = child_ms

        live_score, progress_flags, progress_detail, _progress_ms = assess_playlist_progress(
            progress_url,
            progress_text,
            headers,
            timeout,
            sequence_delay,
        )
        anomaly_flags = tuple(sorted(set(progress_flags)))
        return ProbeResult(
            ok=True,
            status=top.status,
            content_type=top.content_type,
            detail=f"{detail}; {progress_detail}" if progress_detail else detail,
            elapsed_ms=int((time.perf_counter() - start) * 1000),
            final_url=final_url,
            playlist_ms=top_ms,
            media_ms=media_ms,
            startup_score=compute_startup_score(top_ms, media_ms),
            live_score=live_score,
            anomaly_flags=anomaly_flags,
        )
    except HTTPError as error:
        return ProbeResult(
            ok=False,
            status=error.code,
            content_type=error.headers.get("Content-Type") if error.headers else None,
            detail=f"http error: {error.code}",
            elapsed_ms=int((time.perf_counter() - start) * 1000),
            final_url=error.geturl() if hasattr(error, "geturl") else candidate.url,
        )
    except (TimeoutError, URLError, OSError) as error:
        return ProbeResult(
            ok=False,
            status=None,
            content_type=None,
            detail=f"network error: {error}",
            elapsed_ms=int((time.perf_counter() - start) * 1000),
            final_url=candidate.url,
        )
    except Exception as error:  # noqa: BLE001
        return ProbeResult(
            ok=False,
            status=None,
            content_type=None,
            detail=f"probe error: {error}",
            elapsed_ms=int((time.perf_counter() - start) * 1000),
            final_url=candidate.url,
        )


def validate_live_playlist(candidate: Candidate, timeout: float, sequence_delay: float) -> ProbeResult | None:
    if not url_looks_like_hls(candidate.url):
        return None
    return probe_hls_candidate(candidate, timeout, sequence_delay)


def slow_playable_result(
    *,
    status: int | None,
    content_type: str | None,
    detail: str,
    elapsed_ms: int,
    final_url: str | None,
    playlist_ms: int | None = None,
    media_ms: int | None = None,
) -> ProbeResult:
    return ProbeResult(
        ok=True,
        status=status or 200,
        content_type=content_type,
        detail=detail,
        elapsed_ms=elapsed_ms,
        final_url=final_url,
        playlist_ms=playlist_ms,
        media_ms=media_ms,
        startup_score=compute_startup_score(playlist_ms, media_ms),
        live_score=20,
        anomaly_flags=("slow-source",),
    )


def classify_response(content_type: str | None, final_url: str, body: bytes) -> str:
    normalized_type = (content_type or "").split(";")[0].strip().lower()
    lowered_url = final_url.lower()
    prefix = body[:256].lstrip()
    if normalized_type in M3U_CONTENT_TYPES or lowered_url.endswith(".m3u8") or prefix.startswith(b"#EXTM3U"):
        return "hls"
    if lowered_url.endswith(".mpd") or b"<mpd" in prefix.lower():
        return "dash"
    if normalized_type.startswith(PLAYABLE_CONTENT_PREFIXES):
        return "media"
    if lowered_url.endswith((".aac", ".flv", ".m4a", ".mp3", ".mp4", ".ts")):
        return "media"
    if body:
        return "generic"
    return "unknown"


def http_fetch(
    url: str,
    headers: dict[str, str],
    timeout: float,
    *,
    max_bytes: int,
    range_request: bool = False,
) -> FetchResult:
    request_headers = dict(headers)
    if range_request:
        request_headers["Range"] = f"bytes=0-{max_bytes - 1}"
    request = Request(url, headers=request_headers)
    with urlopen(request, timeout=timeout) as response:
        body = response.read(max_bytes)
        status = getattr(response, "status", None)
        content_type = response.headers.get("Content-Type")
        return FetchResult(
            status=status,
            content_type=content_type,
            final_url=response.geturl(),
            body=body,
        )


def timed_http_fetch(
    url: str,
    headers: dict[str, str],
    timeout: float,
    *,
    max_bytes: int,
    range_request: bool = False,
) -> tuple[FetchResult, int]:
    started = time.perf_counter()
    fetch = http_fetch(
        url,
        headers,
        timeout,
        max_bytes=max_bytes,
        range_request=range_request,
    )
    return fetch, int((time.perf_counter() - started) * 1000)


def ok_status(status: int | None) -> bool:
    return status is not None and 200 <= status < 400


def should_retry_probe(result: ProbeResult) -> bool:
    return result.status is None or (result.status >= 500 if result.status is not None else False)


def run_ffprobe(candidate: Candidate, timeout: float) -> ProbeResult | None:
    ffprobe_path = shutil.which("ffprobe")
    if not ffprobe_path:
        return None

    command = [
        ffprobe_path,
        "-v",
        "error",
        "-show_entries",
        "stream=codec_type:format=format_name",
        "-of",
        "json",
        candidate.url,
    ]
    if candidate.user_agent:
        command[1:1] = ["-user_agent", candidate.user_agent]
    if candidate.referrer:
        command[1:1] = ["-headers", f"Referer: {candidate.referrer}\r\n"]

    start = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as error:
        return ProbeResult(
            ok=False,
            status=None,
            content_type=None,
            detail=f"ffprobe error: {error}",
            elapsed_ms=int((time.perf_counter() - start) * 1000),
            via_ffprobe=True,
        )

    elapsed_ms = int((time.perf_counter() - start) * 1000)
    if completed.returncode != 0:
        detail = completed.stderr.strip().splitlines()[-1] if completed.stderr.strip() else "ffprobe failed"
        return ProbeResult(
            ok=False,
            status=None,
            content_type=None,
            detail=detail,
            elapsed_ms=elapsed_ms,
            via_ffprobe=True,
        )

    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        payload = {}
    streams = payload.get("streams") or []
    stream_types = {stream.get("codec_type") for stream in streams if stream.get("codec_type")}
    has_video = "video" in stream_types
    has_audio = "audio" in stream_types
    format_name = ((payload.get("format") or {}).get("format_name") or "").lower()
    mp4_like_format = any(name in format_name for name in ("mov,mp4", "mp4", "mov"))
    if not has_video:
        return ProbeResult(
            ok=False,
            status=None,
            content_type="ffprobe",
            detail="ffprobe found no video stream",
            elapsed_ms=elapsed_ms,
            final_url=candidate.url,
            via_ffprobe=True,
        )
    if url_looks_like_vod(candidate.url) and mp4_like_format:
        return ProbeResult(
            ok=False,
            status=200,
            content_type="ffprobe",
            detail="ffprobe detected mp4-style container on a vod-like url",
            elapsed_ms=elapsed_ms,
            final_url=candidate.url,
            via_ffprobe=True,
        )
    return ProbeResult(
        ok=True,
        status=200,
        content_type=f"ffprobe:{format_name or 'unknown'}",
        detail="ffprobe detected video+audio streams" if has_audio else "ffprobe detected video stream",
        elapsed_ms=elapsed_ms,
        final_url=candidate.url,
        via_ffprobe=True,
    )


def probe_candidate_once(
    candidate: Candidate,
    timeout: float,
    use_ffprobe: bool,
    sequence_delay: float,
) -> ProbeResult:
    if use_ffprobe:
        ffprobe_result = run_ffprobe(candidate, timeout)
        if ffprobe_result and ffprobe_result.ok:
            playlist_check = validate_live_playlist(candidate, timeout, sequence_delay)
            if playlist_check and not playlist_check.ok:
                return playlist_check
            if playlist_check:
                return merge_probe_results(ffprobe_result, playlist_check)
            return ffprobe_result
        if ffprobe_result and any(
            marker in ffprobe_result.detail.lower()
            for marker in ("no video stream", "vod-like url")
        ):
            return ffprobe_result

    if url_looks_like_hls(candidate.url):
        return probe_hls_candidate(candidate, timeout, sequence_delay)

    headers = candidate.request_headers()
    start = time.perf_counter()
    try:
        top, top_ms = timed_http_fetch(candidate.url, headers, timeout, max_bytes=TEXT_SAMPLE_LIMIT)
        kind = classify_response(top.content_type, top.final_url, top.body)

        if not ok_status(top.status):
            return ProbeResult(
                ok=False,
                status=top.status,
                content_type=top.content_type,
                detail="unexpected http status",
                elapsed_ms=int((time.perf_counter() - start) * 1000),
                final_url=top.final_url,
                playlist_ms=top_ms,
            )

        if kind == "hls":
            return probe_hls_candidate(candidate, timeout, sequence_delay)

        if kind == "dash":
            ok = b"<mpd" in top.body[:TEXT_SAMPLE_LIMIT].lower()
            return ProbeResult(
                ok=ok,
                status=top.status,
                content_type=top.content_type,
                detail="mpd manifest reachable" if ok else "dash manifest malformed",
                elapsed_ms=int((time.perf_counter() - start) * 1000),
                final_url=top.final_url,
                playlist_ms=top_ms,
                startup_score=compute_startup_score(top_ms, top_ms),
                live_score=50 if ok else 0,
            )

        if kind == "media":
            lowered_final = (top.final_url or "").lower()
            normalized_type = (top.content_type or "").split(";")[0].strip().lower()
            if lowered_final.endswith((".m4a", ".mov", ".mp3", ".mp4")) or "mp4" in normalized_type:
                return ProbeResult(
                    ok=False,
                    status=top.status,
                    content_type=top.content_type,
                    detail="static media file, not live tv",
                    elapsed_ms=int((time.perf_counter() - start) * 1000),
                    final_url=top.final_url,
                    playlist_ms=top_ms,
                )
            ok = bool(top.body)
            return ProbeResult(
                ok=ok,
                status=top.status,
                content_type=top.content_type,
                detail="media endpoint reachable" if ok else "empty media response",
                elapsed_ms=int((time.perf_counter() - start) * 1000),
                final_url=top.final_url,
                playlist_ms=top_ms,
                media_ms=top_ms,
                startup_score=compute_startup_score(top_ms, top_ms),
                live_score=40 if ok else 0,
            )

        if kind == "generic":
            return ProbeResult(
                ok=False,
                status=top.status,
                content_type=top.content_type,
                detail="generic http body is not a validated stream",
                elapsed_ms=int((time.perf_counter() - start) * 1000),
                final_url=top.final_url,
                playlist_ms=top_ms,
            )

        return ProbeResult(
            ok=False,
            status=top.status,
            content_type=top.content_type,
            detail="unknown response format",
            elapsed_ms=int((time.perf_counter() - start) * 1000),
            final_url=top.final_url,
            playlist_ms=top_ms,
        )
    except HTTPError as error:
        return ProbeResult(
            ok=False,
            status=error.code,
            content_type=error.headers.get("Content-Type") if error.headers else None,
            detail=f"http error: {error.code}",
            elapsed_ms=int((time.perf_counter() - start) * 1000),
            final_url=error.geturl() if hasattr(error, "geturl") else candidate.url,
        )
    except (TimeoutError, URLError, OSError) as error:
        return ProbeResult(
            ok=False,
            status=None,
            content_type=None,
            detail=f"network error: {error}",
            elapsed_ms=int((time.perf_counter() - start) * 1000),
            final_url=candidate.url,
        )
    except Exception as error:  # noqa: BLE001
        return ProbeResult(
            ok=False,
            status=None,
            content_type=None,
            detail=f"probe error: {error}",
            elapsed_ms=int((time.perf_counter() - start) * 1000),
            final_url=candidate.url,
        )


def probe_candidate(
    candidate: Candidate,
    timeout: float,
    use_ffprobe: bool,
    retries: int,
    stability_checks: int,
    sequence_delay: float,
) -> ProbeResult:
    result = probe_candidate_once(candidate, timeout, use_ffprobe, sequence_delay)
    retry_count = 0
    while not result.ok and retry_count < retries and should_retry_probe(result):
        retry_count += 1
        result = probe_candidate_once(candidate, timeout, use_ffprobe, sequence_delay)

    if not result.ok:
        return result

    for check_index in range(1, max(1, stability_checks)):
        follow_up = probe_candidate_once(candidate, timeout, use_ffprobe, sequence_delay)
        if not follow_up.ok:
            if follow_up.status is None and "timed out" in follow_up.detail.lower():
                return ProbeResult(
                    ok=True,
                    status=result.status,
                    content_type=result.content_type,
                    detail=f"{result.detail}; slow follow-up timeout tolerated",
                    elapsed_ms=max(result.elapsed_ms, follow_up.elapsed_ms),
                    final_url=result.final_url,
                    via_ffprobe=result.via_ffprobe or follow_up.via_ffprobe,
                    playlist_ms=result.playlist_ms,
                    media_ms=result.media_ms,
                    startup_score=result.startup_score,
                    live_score=result.live_score,
                    content_score=result.content_score,
                    history_local_score=result.history_local_score,
                    history_cloud_score=result.history_cloud_score,
                    anomaly_flags=tuple(sorted(set(result.anomaly_flags + follow_up.anomaly_flags))),
                )
            return ProbeResult(
                ok=False,
                status=follow_up.status,
                content_type=follow_up.content_type,
                detail=f"stability check {check_index + 1} failed: {follow_up.detail}",
                elapsed_ms=max(result.elapsed_ms, follow_up.elapsed_ms),
                final_url=follow_up.final_url,
                via_ffprobe=result.via_ffprobe or follow_up.via_ffprobe,
                playlist_ms=follow_up.playlist_ms if follow_up.playlist_ms is not None else result.playlist_ms,
                media_ms=follow_up.media_ms if follow_up.media_ms is not None else result.media_ms,
                startup_score=max(result.startup_score, follow_up.startup_score),
                live_score=min(result.live_score, follow_up.live_score),
                content_score=min(result.content_score, follow_up.content_score),
                history_local_score=max(result.history_local_score, follow_up.history_local_score),
                history_cloud_score=max(result.history_cloud_score, follow_up.history_cloud_score),
                anomaly_flags=tuple(sorted(set(result.anomaly_flags + follow_up.anomaly_flags))),
            )
        result = ProbeResult(
            ok=True,
            status=result.status,
            content_type=result.content_type,
            detail=f"{result.detail}; stable x{check_index + 1}",
            elapsed_ms=max(result.elapsed_ms, follow_up.elapsed_ms),
            final_url=result.final_url,
            via_ffprobe=result.via_ffprobe or follow_up.via_ffprobe,
            playlist_ms=result.playlist_ms if result.playlist_ms is not None else follow_up.playlist_ms,
            media_ms=result.media_ms if result.media_ms is not None else follow_up.media_ms,
            startup_score=max(result.startup_score, follow_up.startup_score),
            live_score=min(result.live_score, follow_up.live_score) if follow_up.live_score else result.live_score,
            content_score=min(result.content_score, follow_up.content_score),
            history_local_score=max(result.history_local_score, follow_up.history_local_score),
            history_cloud_score=max(result.history_cloud_score, follow_up.history_cloud_score),
            anomaly_flags=tuple(sorted(set(result.anomaly_flags + follow_up.anomaly_flags))),
        )
    return result


def run_ffmpeg_content_probe(candidate: Candidate, timeout: float) -> tuple[int, tuple[str, ...]]:
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        return 50, ()

    command = [
        ffmpeg_path,
        "-v",
        "error",
        "-i",
        candidate.url,
        "-an",
        "-vf",
        "fps=1/2,scale=16:16,format=gray",
        "-frames:v",
        "2",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "gray",
        "-",
    ]
    if candidate.user_agent:
        command[1:1] = ["-user_agent", candidate.user_agent]
    if candidate.referrer:
        command[1:1] = ["-headers", f"Referer: {candidate.referrer}\r\n"]

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return 60, ("content-check-timeout",)
    except OSError:
        return 50, ()

    if completed.returncode != 0 or not completed.stdout:
        return 45, ("content-check-empty",)

    frame_size = CONTENT_SAMPLE_SIZE
    frames = [
        completed.stdout[index : index + frame_size]
        for index in range(0, len(completed.stdout), frame_size)
        if len(completed.stdout[index : index + frame_size]) == frame_size
    ]
    if not frames:
        return 45, ("content-check-empty",)

    anomaly_flags: list[str] = []
    brightness = [sum(frame) / frame_size for frame in frames]
    if any(level < 8 for level in brightness):
        anomaly_flags.append("black-frame")
    if len(frames) >= 2 and frames[0] == frames[1]:
        anomaly_flags.append("frozen-frames")

    score = 100 - (35 * len(anomaly_flags))
    return max(10, score), tuple(sorted(set(anomaly_flags)))


def annotate_content_scores(
    verified_items: list[tuple[Candidate, ProbeResult]],
    timeout: float,
    workers: int,
) -> list[tuple[Candidate, ProbeResult]]:
    if not verified_items:
        return verified_items

    annotated: list[tuple[Candidate, ProbeResult]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(workers, 8))) as executor:
        future_to_item = {
            executor.submit(run_ffmpeg_content_probe, candidate, timeout): (candidate, probe)
            for candidate, probe in verified_items
        }
        for future in concurrent.futures.as_completed(future_to_item):
            candidate, probe = future_to_item[future]
            content_score, anomaly_flags = future.result()
            combined_flags = tuple(sorted(set(probe.anomaly_flags + anomaly_flags)))
            annotated.append(
                (
                    candidate,
                    dataclasses.replace(
                        probe,
                        content_score=content_score,
                        anomaly_flags=combined_flags,
                    ),
                )
            )
    annotated.sort(
        key=lambda item: (
            GROUP_SORT_ORDER.get(item[0].channel_group or "", 99),
            item[0].channel_group or "",
            item[0].title,
            item[0].url,
        )
    )
    return annotated


def attach_history_scores(
    items: list[tuple[Candidate, ProbeResult]],
    history: HistoryStore,
) -> list[tuple[Candidate, ProbeResult]]:
    attached: list[tuple[Candidate, ProbeResult]] = []
    for candidate, probe in items:
        attached.append(
            (
                candidate,
                dataclasses.replace(
                    probe,
                    history_local_score=history.score(candidate.url, "local"),
                    history_cloud_score=history.score(candidate.url, "cloud"),
                ),
            )
        )
    return attached


def format_group_title(candidate: Candidate) -> str | None:
    if candidate.group_title in {"央视", "卫视"}:
        return candidate.group_title
    if candidate.group_title:
        return candidate.group_title
    pieces = [candidate.country]
    if candidate.languages:
        pieces.append("+".join(candidate.languages))
    elif candidate.categories:
        pieces.append(candidate.categories[0])
    value = "/".join(piece for piece in pieces if piece)
    return value or None


def escape_attr(value: str) -> str:
    return value.replace('"', "'")


def append_m3u_entry(
    lines: list[str],
    candidate: Candidate,
    display_title: str,
    group_title: str | None = None,
) -> None:
    attributes = []
    if candidate.channel_id:
        attributes.append(f'tvg-id="{escape_attr(candidate.channel_id)}"')
    attributes.append(f'tvg-name="{escape_attr(display_title)}"')
    if candidate.logo:
        attributes.append(f'tvg-logo="{escape_attr(candidate.logo)}"')
    rendered_group = group_title if group_title is not None else format_group_title(candidate)
    if rendered_group:
        attributes.append(f'group-title="{escape_attr(rendered_group)}"')
    lines.append(f'#EXTINF:-1 {" ".join(attributes)},{display_title}')
    if candidate.user_agent:
        lines.append(f"#EXTVLCOPT:http-user-agent={candidate.user_agent}")
    if candidate.referrer:
        lines.append(f"#EXTVLCOPT:http-referrer={candidate.referrer}")
    lines.append(candidate.url)


def write_m3u(path: Path, verified_items: list[tuple[Candidate, ProbeResult]]) -> None:
    lines = ["#EXTM3U"]
    for candidate, _probe in verified_items:
        append_m3u_entry(lines, candidate, candidate.title)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_backup_m3u(
    path: Path,
    grouped_items: list[list[tuple[Candidate, ProbeResult]]],
    backup_count: int,
) -> None:
    lines = ["#EXTM3U"]
    for items in grouped_items:
        for index, (candidate, _probe) in enumerate(items[: max(1, backup_count)]):
            display_title = candidate.title if index == 0 else f"{candidate.title} 备用{index}"
            append_m3u_entry(lines, candidate, display_title, format_group_title(candidate))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_report(
    path: Path,
    verified_items: list[tuple[Candidate, ProbeResult]],
    grouped_items: list[list[tuple[Candidate, ProbeResult]]],
    failed_items: list[tuple[Candidate, ProbeResult]],
    keep_failures: bool,
    probe_environment: str,
    history_path: Path | None,
    backup_count: int,
) -> None:
    group_counts: dict[str, int] = {}
    for candidate, _probe in verified_items:
        group_name = candidate.channel_group or "未分组"
        group_counts[group_name] = group_counts.get(group_name, 0) + 1
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "probe_environment": probe_environment,
        "history_path": str(history_path) if history_path else None,
        "success_count": len(verified_items),
        "failure_count": len(failed_items),
        "group_counts": group_counts,
        "verified": [
            {
                "candidate": dataclasses.asdict(candidate),
                "probe": dataclasses.asdict(probe),
            }
            for candidate, probe in verified_items
        ],
        "backups": {
            items[0][0].title: [
                {
                    "priority": index + 1,
                    "candidate": dataclasses.asdict(candidate),
                    "probe": dataclasses.asdict(probe),
                }
                for index, (candidate, probe) in enumerate(items[: max(1, backup_count)])
            ]
            for items in grouped_items
        },
    }
    if keep_failures:
        payload["failed"] = [
            {
                "candidate": dataclasses.asdict(candidate),
                "probe": dataclasses.asdict(probe),
            }
            for candidate, probe in failed_items
        ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_candidates(args: argparse.Namespace, cache: CacheStore) -> list[Candidate]:
    candidates: list[Candidate] = []
    providers = list(dict.fromkeys(args.provider))

    if "iptv-org" in providers:
        candidates.extend(
            load_iptv_org_candidates(
                cache=cache,
                timeout=args.timeout,
                include_nsfw=args.include_nsfw,
                min_quality=args.min_quality,
                allow_ip_hosts=args.allow_ip_hosts,
            )
        )

    if "cctv-official" in providers:
        candidates.extend(
            load_cctv_official_candidates(
                cache=cache,
                timeout=args.timeout,
                min_quality=args.min_quality,
            )
        )

    if "curated-public" in providers:
        candidates.extend(
            load_curated_public_candidates(
                cache=cache,
                timeout=args.timeout,
                include_nsfw=args.include_nsfw,
                min_quality=args.min_quality,
            )
        )

    if "published" in providers:
        candidates.extend(
            load_published_candidates(
                include_nsfw=args.include_nsfw,
                min_quality=args.min_quality,
                allow_ip_hosts=True,
            )
        )

    if "manual-preferred" in providers:
        candidates.extend(load_manual_preferred_candidates())

    for remote_url in args.remote_m3u:
        text = fetch_text(remote_url, cache, timeout=args.timeout)
        candidates.extend(
            load_extra_m3u_candidates(
                text,
                source_name=f"remote:{remote_url}",
                include_nsfw=args.include_nsfw,
                min_quality=args.min_quality,
                allow_ip_hosts=args.allow_ip_hosts,
            )
        )

    for local_path in args.local_m3u:
        text = load_m3u_file(Path(local_path))
        candidates.extend(
            load_extra_m3u_candidates(
                text,
                source_name=f"local:{local_path}",
                include_nsfw=args.include_nsfw,
                min_quality=args.min_quality,
                allow_ip_hosts=args.allow_ip_hosts,
            )
        )

    deduped = dedupe_candidates(candidates)
    deduped.sort(key=candidate_rank, reverse=True)
    if args.limit > 0:
        deduped = deduped[: args.limit]
    return deduped


def probe_all(
    candidates: list[Candidate],
    timeout: float,
    workers: int,
    use_ffprobe: bool,
    retries: int,
    stability_checks: int,
    sequence_delay: float,
    verbose: bool,
) -> tuple[list[tuple[Candidate, ProbeResult]], list[tuple[Candidate, ProbeResult]]]:
    verified: list[tuple[Candidate, ProbeResult]] = []
    failed: list[tuple[Candidate, ProbeResult]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_candidate = {
            executor.submit(
                probe_candidate,
                candidate,
                timeout,
                use_ffprobe,
                retries,
                stability_checks,
                sequence_delay,
            ): candidate
            for candidate in candidates
        }
        total = len(future_to_candidate)
        completed = 0
        for future in concurrent.futures.as_completed(future_to_candidate):
            candidate = future_to_candidate[future]
            probe = future.result()
            completed += 1
            if probe.ok:
                verified.append((candidate, probe))
            else:
                failed.append((candidate, probe))
            if verbose and (completed == total or completed % 25 == 0):
                log(
                    f"progress {completed}/{total} "
                    f"(ok={len(verified)} fail={len(failed)})",
                    verbose=True,
                )

    verified.sort(
        key=lambda item: (candidate_rank(item[0]), -item[1].elapsed_ms),
        reverse=True,
    )
    failed.sort(
        key=lambda item: (candidate_rank(item[0]), -item[1].elapsed_ms),
        reverse=True,
    )
    return verified, failed


def main() -> int:
    args = parse_args()
    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    cache = CacheStore(cache_dir, ttl_seconds=args.cache_ttl)
    history_path = Path(args.history) if args.history else None
    history = HistoryStore(history_path)

    candidates = load_candidates(args, cache)
    log(f"loaded {len(candidates)} ranked candidates", verbose=args.verbose)

    verified, failed = probe_all(
        candidates,
        timeout=args.timeout,
        workers=args.workers,
        use_ffprobe=args.ffprobe,
        retries=max(0, args.retries),
        stability_checks=max(1, args.stability_checks),
        sequence_delay=max(0.5, args.live_sequence_delay),
        verbose=args.verbose,
    )
    verified = annotate_content_scores(
        verified,
        timeout=max(1.0, args.content_check_timeout),
        workers=args.workers,
    )
    for candidate, probe in [*verified, *failed]:
        history.record(candidate, probe, args.probe_environment)
    history.save()
    verified = attach_history_scores(verified, history)
    failed = attach_history_scores(failed, history)
    verified, grouped_verified = collapse_verified_items(verified)

    out_path = Path(args.out)
    backup_out_path = Path(args.backup_out)
    report_path = Path(args.report)
    write_m3u(out_path, verified)
    write_backup_m3u(backup_out_path, grouped_verified, max(1, args.backup_count))
    write_report(
        report_path,
        verified,
        grouped_verified,
        failed,
        keep_failures=args.keep_failures,
        probe_environment=args.probe_environment,
        history_path=history_path,
        backup_count=max(1, args.backup_count),
    )

    print(
        json.dumps(
            {
                "candidates": len(candidates),
                "verified": len(verified),
                "failed": len(failed),
                "m3u": str(out_path),
                "backup_m3u": str(backup_out_path),
                "report": str(report_path),
            },
            ensure_ascii=False,
        )
    )
    return 0 if verified else 1


if __name__ == "__main__":
    raise SystemExit(main())
