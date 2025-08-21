import os
import sys
import json
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dateutil import parser as duparser

import gspread
from gspread.utils import rowcol_to_a1

# ====== 設定 ======
# 入出力スプレッドシートIDは環境変数から取得（無ければ既定値として質問のIDを使用）
INPUT_SPREADSHEET_ID = os.getenv(
    "INPUT_SPREADSHEET_ID",
    "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"
)
OUTPUT_SPREADSHEET_ID = os.getenv(
    "OUTPUT_SPREADSHEET_ID",
    "1bi9U5y5k0EqF4lTgISSPvh8H_2dc8PUA2U3W0gulRbM"
)

# 入力側のシート名（順番を維持）
INPUT_SHEETS = ["MSN", "Google", "Yahoo"]

# 出力ヘッダー
OUTPUT_HEADERS = ["ソース", "タイトル", "URL", "投稿日", "引用元"]  # A〜E列

# JST
JST = ZoneInfo("Asia/Tokyo")


def jst_now():
    return datetime.now(tz=JST)


def yymmdd_jst(dt: datetime) -> str:
    return dt.strftime("%y%m%d")


def calc_time_window(now_jst: datetime):
    """
    抽出範囲：前日15:00（含む）〜 当日14:59:59（含む）
    """
    # 当日 14:59:59
    end = now_jst.replace(hour=14, minute=59, second=59, microsecond=0)
    if now_jst > end:
        # すでに当日15時前後を過ぎていても、範囲は常に「前日15:00〜当日14:59:59」
        pass
    else:
        # 14:59:59 より前の時間なら、当日が今日、前日は昨日でOK
        pass
    # 前日15:00
    start = (end - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
    return start, end


def parse_sheet_datetime_to_jst(val):
    """
    Google Sheets の C列「投稿日」を JST の datetime に変換。
    - 数値（シリアル）の場合：1899-12-30 起点
    - 文字列の場合：dateutil で柔軟パース（タイムゾーン無ければ JST とみなす）
    - 空/不正は None
    """
    if val is None:
        return None

    # 数値（シリアル）対応
    try:
        # 数値っぽければ float に
        serial = float(val)
        # Google/Excel シリアル: 1899-12-30 を day 0 とする
        base = datetime(1899, 12, 30, tzinfo=timezone.utc)  # まずUTC起点
        dt_utc = base + timedelta(days=serial)
        # ただしシリアルは時差なしの「ローカル」扱いに近いこともあるため、
        # 実務では JST 直付けの方が直感に合うケースが多い。
        # ここでは JST に変換して返す。
        return dt_utc.astimezone(JST)
    except Exception:
        pass

    # 文字列パース
    try:
        dt = duparser.parse(str(val), fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JST)
        else:
            dt = dt.astimezone(JST)
        return dt
    except Exception:
        return None


def service_account_client_from_env():
    """
    環境変数 GOOGLE_CREDENTIALS（JSON文字列）を使って gspread Client を作成
    """
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
        sys.exit(3)


def ensure_output_worksheet(sh_out, title: str):
    """
    出力用ワークシート（当日 yymmdd）を確保。無ければ作成してヘッダを書き込む。
    返り値：gspread.Worksheet
    """
    ws = None
    try:
        ws = sh_out.worksheet(title)
    except gspread.WorksheetNotFound:
        # 新規作成：列幅はとりあえず5列
        ws = sh_out.add_worksheet(title=title, rows=1000, cols=5)
        ws.append_row(OUTPUT_HEADERS, value_input_option="USER_ENTERED")
    return ws


def read_existing_urls(ws_out):
    """
    既存のURL（C列）をセットで返す（ヘッダー除く）
    """
    values = ws_out.get_all_values()
    urls = set()
    for i, row in enumerate(values):
        if i == 0:
            continue  # ヘッダ
        if len(row) >= 3:
            url = (row[2] or "").strip()
            if url:
                urls.add(url)
    return urls


def collect_rows_from_input(sh_in, start_jst: datetime, end_jst: datetime):
    """
    入力スプレッドシートから、時間範囲に一致する行を
    MSN → Google → Yahoo の順で抽出して返す。
    返り値: List[List[str]] で出力形式（A:ソース, B:タイトル, C:URL, D:投稿日, E:引用元）
    """
    out_rows = []

    for sheet_name in INPUT_SHEETS:
        try:
            ws = sh_in.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            print(f"⚠ 入力側にシート '{sheet_name}' が見つかりません。スキップします。")
            continue

        values = ws.get_all_values()  # A:D を含む全体を取得（軽量のため列範囲指定でもOK）
        if not values:
            continue

        # 1行目ヘッダと想定
        for i, row in enumerate(values):
            if i == 0:
                continue
            # A:タイトル, B:URL, C:投稿日, D:引用元
            title = row[0].strip() if len(row) > 0 else ""
            url = row[1].strip() if len(row) > 1 else ""
            posted_raw = row[2].strip() if len(row) > 2 else ""
            source_name = row[3].strip() if len(row) > 3 else ""

            if not title or not url or not posted_raw:
                continue

            posted_dt = parse_sheet_datetime_to_jst(posted_raw)
            if posted_dt is None:
                continue

            if start_jst <= posted_dt <= end_jst:
                # 出力形式：[ソース, タイトル, URL, 投稿日(元の文字列), 引用元]
                out_rows.append([sheet_name, title, url, posted_raw, source_name])

    return out_rows


def append_rows_dedup(ws_out, rows, existing_urls):
    """
    既存URLと重複しないものだけをまとめて追記
    """
    new_rows = [r for r in rows if (r[2] not in existing_urls)]
    if not new_rows:
        print("✅ 追加対象の新規ニュースはありません（すべて既存URLと重複）。")
        return 0

    # まとめて追記
    ws_out.append_rows(new_rows, value_input_option="USER_ENTERED")
    print(f"📝 追加 {len(new_rows)} 件")
    return len(new_rows)


def main():
    now = jst_now()
    date_sheet = yymmdd_jst(now)
    start_jst, end_jst = calc_time_window(now)

    print(f"📅 期間: {start_jst.strftime('%Y/%m/%d %H:%M:%S')} 〜 {end_jst.strftime('%Y/%m/%d %H:%M:%S')} (JST)")
    print(f"🗒 出力シート名: {date_sheet}")

    gc = service_account_client_from_env()

    sh_in = open_sheet_by_id(gc, INPUT_SPREADSHEET_ID)
    sh_out = open_sheet_by_id(gc, OUTPUT_SPREADSHEET_ID)

    # 入力から抽出（順序は MSN→Google→Yahoo を維持）
    extracted = collect_rows_from_input(sh_in, start_jst, end_jst)
    print(f"🔎 抽出合計: {len(extracted)} 件")

    # 出力ワークシート確保
    ws_out = ensure_output_worksheet(sh_out, date_sheet)

    # 既存URL収集（C列）
    existing = read_existing_urls(ws_out)
    print(f"🧮 既存URL数: {len(existing)} 件")

    # 追記（重複除外）
    added = append_rows_dedup(ws_out, extracted, existing)

    print("✅ 完了")
    if added:
        print(f"✨ 新規追加: {added} 件")
    else:
        print("✨ 追加なし")


if __name__ == "__main__":
    main()
