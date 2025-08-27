# main.py  —  追加要件対応版
import os
import sys
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dateutil import parser as duparser

import gspread

# 高精度なUnicode正規表現（任意）
try:
    import regex as re_u  # pip install regex
except Exception:
    re_u = None

# 半角統一用
import unicodedata
try:
    import jaconv  # pip install jaconv
except Exception:
    jaconv = None

# Gemini（任意）
try:
    import google.generativeai as genai
except Exception:
    genai = None


# ====== 設定 ======
INPUT_SPREADSHEET_ID = os.getenv(
    "INPUT_SPREADSHEET_ID",
    "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"  # 入力
)
OUTPUT_SPREADSHEET_ID = os.getenv(
    "OUTPUT_SPREADSHEET_ID",
    "1bi9U5y5k0EqF4lTgISSPvh8H_2dc8PUA2U3W0gulRbM"  # 出力
)
INPUT_SHEETS = ["MSN", "Google", "Yahoo"]

# 出力列（A〜I）:
# A=ソース, B=タイトル, C=URL, D=投稿日, E=引用元, F=ポジネガ, G=カテゴリ, H=重複確認用タイトル, I=有料カテゴリ（新設）
OUTPUT_HEADERS = [
    "ソース", "タイトル", "URL", "投稿日", "引用元",
    "ポジネガ", "カテゴリ", "重複確認用タイトル", "有料カテゴリ"  # 追加
]

JST = ZoneInfo("Asia/Tokyo")


def jst_now():
    return datetime.now(tz=JST)


def yymmdd_jst(dt: datetime) -> str:
    return dt.strftime("%y%m%d")


def calc_time_window(now_jst: datetime):
    """
    抽出範囲：前日15:00（含む）〜 当日14:59:59（含む）
    """
    end = now_jst.replace(hour=14, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
    return start, end


def parse_sheet_datetime_to_jst(val):
    """
    C列「投稿日」を JST の datetime に変換。
    - 数値（シリアル）：1899-12-30 起点→JST
    - 文字列：dateutilで柔軟パース（TZ無→JST想定）
    """
    if val is None or str(val).strip() == "":
        return None

    # 数値シリアル
    try:
        serial = float(val)
        base = datetime(1899, 12, 30, tzinfo=timezone.utc)
        dt_utc = base + timedelta(days=serial)
        return dt_utc.astimezone(JST)
    except Exception:
        pass

    # 文字列
    try:
        dt = duparser.parse(str(val), fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JST)
        else:
            dt = dt.astimezone(JST)
        return dt
    except Exception:
        return None


def format_compact_jst(dt: datetime) -> str:
    """
    投稿日の書式を `25/8/20 15:01` に統一（年=下2桁、月日=ゼロ埋め無し）
    """
    return f"{dt:%y}/{dt.month}/{dt.day} {dt:%H:%M}"


# --- 文字種の半角統一（カタカナ・数字・英字） ---
def to_hankaku_kana_ascii_digit(s: str) -> str:
    """
    ・数字/英字は NFKC で全角→半角へ
    ・カタカナは jaconv があれば z2h(kana=True) で半角化
      （なければ長音等は残るが、実害を最小化）
    """
    if not s:
        return ""
    # 数字・英字は NFKC で半角化（全角→ASCII）
    s_nfkc = unicodedata.normalize("NFKC", s)

    # カタカナ半角化（可能なら）
    if jaconv is not None:
        # ascii/digit も True にして安全側で全半角混在を解消
        s_nfkc = jaconv.z2h(s_nfkc, kana=True, digit=True, ascii=True)
    # なお jaconv が無い場合は、カタカナのみ完全には半角化できない
    return s_nfkc


def normalize_title_for_dup(s: str) -> str:
    """
    H列（重複確認用）生成：
      1) カタカナ・数字・アルファベットを半角へ統一
      2) 記号・括弧類・空白類を包括除去（“”, (), （）, 《》, ［］, 引用符, ダッシュ, 長音記号 等）
      3) 余分な区切りを除いて比較用のシンプル文字列を出力
    """
    if not s:
        return ""

    # 1) 半角統一
    s = to_hankaku_kana_ascii_digit(s)

    # 2) 記号類の除去
    if re_u:
        # \p{P}=句読点, \p{S}=記号, \p{Z}=区切り（スペース等）, \p{Cc}=制御
        # ここに長音・各種ハイフン・ダッシュの互換字も包含される
        s = re_u.sub(r'[\p{P}\p{S}\p{Z}\p{Cc}]+', '', s)
    else:
        # フォールバック：主要な記号と括弧を網羅
        import re
        dash_chars = r'\-\u2212\u2010\u2011\u2012\u2013\u2014\u2015\uFF0D\u30FC\uFF70'  # - − ‐ - ‒ – — ― － ー ｰ
        pattern = (
            r'[\s"'          # 空白, "
            r"'\u201C\u201D\u2018\u2019"  # “ ” ‘ ’
            r'\(\)\[\]{}<>]'              # 半角括弧
            r'|[、。・,…:;!?！？／/\\|＋+＊*.,]'  # 句読点・記号
            r'|[【】＜＞「」『』《》〔〕［］｛｝（）]'  # 全角括弧
            r'|[' + dash_chars + r']'      # ハイフン・ダッシュ・長音
        )
        s = re.sub(pattern, "", s)

    return s


def service_account_client_from_env():
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if not creds_json:
        print("❌ 環境変数 GOOGLE_CREDENTIALS が設定されていません。", file=sys.stderr)
        sys.exit(2)
    try:
        info = json.loads(creds_json)
    except json.JSONDecodeError as e:
        print(f"❌ GOOGLE_CREDENTIALS がJSONとして読み取れません: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        gc = gspread.service_account_from_dict(info)
        return gc
    except Exception as e:
        print(f"❌ Google 認証に失敗: {e}", file=sys.stderr)
        sys.exit(2)


def open_sheet_by_id(gc, spreadsheet_id: str):
    try:
        sh = gc.open_by_key(spreadsheet_id)
        return sh
    except Exception as e:
        print(f"❌ スプレッドシートを開けませんでした（{spreadsheet_id}）: {e}", file=sys.stderr)
