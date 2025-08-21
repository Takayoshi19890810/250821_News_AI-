import os
import sys
import json
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dateutil import parser as duparser

import gspread
from gspread.utils import rowcol_to_a1

# ─────────────────────────────────────────
# 追加: Gemini（任意・APIキーがあれば実行）
try:
    import google.generativeai as genai
except Exception:
    genai = None
# ─────────────────────────────────────────

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

# 出力列: A=ソース, B=タイトル, C=URL, D=投稿日(整形), E=引用元, F=ポジネガ, G=カテゴリ, H=重複確認用タイトル
OUTPUT_HEADERS = ["ソース", "タイトル", "URL", "投稿日", "引用元", "ポジネガ", "カテゴリ", "重複確認用タイトル"]

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
    - 数値（シリアル）の場合：1899-12-30 起点→JSTに
    - 文字列の場合：dateutilで柔軟パース（TZ無ければJST想定）
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
    ① 投稿日の書式を `25/8/20 15:01` のようにする（年は下2桁、月日をゼロ埋めしない）
    """
    return f"{dt:%y}/{dt.month}/{dt.day} {dt:%H:%M}"


def normalize_title_for_dup(s: str) -> str:
    """
    ② 重複確認用にH列へ転記する正規化タイトル:
       記号（() [] 【】 <> "" 『』 「」 ！! ？? ; : 、。 … ー など）と全角/半角スペースを削除
    """
    if not s:
        return ""
    # 記号とスペース（全角/半角）を除去
    pattern = r'[\s\(\)\[\]【】＜＞<>「」『』"\'！!？\?;:、。…・ー—–\-｜|＋+＊*／/\\.,]+'
    return re.sub(pattern, "", s)


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
        sys.exit(3)


def ensure_output_worksheet(sh_out, title: str):
    """
    出力ワークシート（当日 yymmdd）を確保。無ければ新規作成＋ヘッダ。
    """
    try:
        ws = sh_out.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh_out.add_worksheet(title=title, rows=1000, cols=len(OUTPUT_HEADERS))
        ws.append_row(OUTPUT_HEADERS, value_input_option="USER_ENTERED")
    return ws


def read_existing_urls(ws_out):
    """
    既存URL（C列）をセットで返す（ヘッダ除く）
    """
    values = ws_out.get_all_values()
    urls = set()
    for i, row in enumerate(values):
        if i == 0:
            continue
        if len(row) >= 3:
            url = (row[2] or "").strip()
            if url:
                urls.add(url)
    return urls


def collect_rows_from_input(sh_in, start_jst: datetime, end_jst: datetime):
    """
    入力（MSN→Google→Yahoo）から範囲一致を抽出。
    出力形式: [ソース, タイトル, URL, 投稿日(整形文字列), 引用元, "", "", 正規化タイトル]
    """
    out_rows = []

    for sheet_name in INPUT_SHEETS:
        try:
            ws = sh_in.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            print(f"⚠ 入力側にシート '{sheet_name}' が見つかりません。スキップします。")
            continue

        values = ws.get_all_values()
        if not values:
            continue

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
                posted_fmt = format_compact_jst(posted_dt)  # ①ここで整形
                norm_title = normalize_title_for_dup(title)  # ②H列用
                out_rows.append([sheet_name, title, url, posted_fmt, source_name, "", "", norm_title])

    return out_rows


def append_rows_dedup(ws_out, rows, existing_urls):
    """
    既存URLと重複しないものだけ追記
    """
    new_rows = [r for r in rows if (r[2] not in existing_urls)]
    if not new_rows:
        print("✅ 追加対象の新規ニュースはありません（すべて既存URLと重複）。")
        return 0
    ws_out.append_rows(new_rows, value_input_option="USER_ENTERED")
    print(f"📝 追加 {len(new_rows)} 件")
    return len(new_rows)


def classify_with_gemini(ws_out):
    """
    ③ B列タイトルをもとに、F列（ポジネガ）/ G列（カテゴリ）をGeminiで分類。
       - 既にF/Gが埋まっている行はスキップ
       - H列（正規化タイトル）は空なら埋める
    """
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key or genai is None:
        print("ℹ Gemini分類はスキップ（APIキー未設定 or ライブラリ未インストール）。")
        return

    genai.configure(api_key=api_key)

    values = ws_out.get_all_values()
    if len(values) <= 1:
        return

    # 収集: 分類対象（F/Gが未記入）
    items = []  # (row_idx, title)
    h_updates = []  # {'range': 'Hn', 'values': [[norm]]}
    for i, row in enumerate(values):
        if i == 0:
            continue
        row_idx = i + 1
        title = row[1] if len(row) > 1 else ""
        f_val = row[5] if len(row) > 5 else ""
        g_val = row[6] if len(row) > 6 else ""
        h_val = row[7] if len(row) > 7 else ""

        if title:
            # H列が空なら埋める
            if not h_val:
                norm = normalize_title_for_dup(title)
                h_updates.append({"range": f"H{row_idx}", "values": [[norm]]})
            # F/Gが空なら分類対象
            if not f_val or not g_val:
                items.append((row_idx, title))

    # 先にH列の欠損をまとめて更新
    if h_updates:
        ws_out.batch_update(h_updates, value_input_option="USER_ENTERED")

    if not items:
        print("ℹ 分類対象の行はありません。")
        return

    # バッチ分割（長文防止）
    BATCH = 40
    updates = []
    for start in range(0, len(items), BATCH):
        batch = items[start : start + BATCH]
        payload = [{"row": r, "title": t} for (r, t) in batch]

        system_prompt = """
あなたは敏腕雑誌記者です。次のタイトル一覧を以下の視点で分類してください。
①ポジティブ、ネガティブ、ニュートラルの判別（「ポジティブ」「ネガティブ」「ニュートラル」のいずれか）。
②記事のカテゴリーの判別（最も関連が高い1つだけ）：
- 会社：企業の施策や生産、販売台数など。ニッサン、トヨタ、ホンダ、スバル、マツダ、スズキ、ミツビシ、ダイハツは()付で企業名を記載。それ以外はその他。
- 車：クルマの名称が含まれているもの（会社名だけの場合は車に分類しない）。新型/現行/旧型+名称を()付で記載（例：新型リーフ、現行セレナ、旧型スカイライン）。日産以外は「車（競合）」。
- 技術（EV）：電気自動車の技術。
- 技術（e-POWER）
- 技術（e-4ORCE）：4WD/2WD/AWD関連。
- 技術（AD/ADAS）
- 技術：上記以外の技術。
- モータースポーツ
- 株式
- 政治・経済
- スポーツ
- その他

出力は必ず **JSON配列**。各要素は { "row": 行番号, "sentiment": "ポジティブ|ネガティブ|ニュートラル", "category": "カテゴリ名"} の形式のみで返してください。
入力のタイトル文字列は一切変更しないでください。
""".strip()

        # モデル呼び出し
        try:
            model = genai.GenerativeModel("gemini-1.5-flash")
            prompt = system_prompt + "\n\n" + json.dumps(payload, ensure_ascii=False, indent=2)
            resp = model.generate_content(prompt)
            text = resp.text or ""
            # JSON抽出（コードブロック対応）
            m = re.search(r"\[.*\]", text, flags=re.DOTALL)
            json_text = m.group(0) if m else text
            result = json.loads(json_text)

            for obj in result:
                row_idx = int(obj.get("row"))
                sentiment = str(obj.get("sentiment", "")).strip()
                category = str(obj.get("category", "")).strip()
                if sentiment or category:
                    updates.append({
                        "range": f"F{row_idx}:G{row_idx}",
                        "values": [[sentiment, category]]
                    })
        except Exception as e:
            print(f"⚠ Gemini応答の解析に失敗: {e}")

    if updates:
        ws_out.batch_update(updates, value_input_option="USER_ENTERED")
        print(f"✨ Gemini分類を {len(updates)} 行に反映しました。")
    else:
        print("ℹ Gemini分類の更新はありませんでした。")


def main():
    now = jst_now()
    date_sheet = yymmdd_jst(now)
    start_jst, end_jst = calc_time_window(now)

    print(f"📅 期間: {start_jst.strftime('%Y/%m/%d %H:%M:%S')} 〜 {end_jst.strftime('%Y/%m/%d %H:%M:%S')} (JST)")
    print(f"🗒 出力シート名: {date_sheet}")

    gc = service_account_client_from_env()
    sh_in = open_sheet_by_id(gc, INPUT_SPREADSHEET_ID)
    sh_out = open_sheet_by_id(gc, OUTPUT_SPREADSHEET_ID)

    # 入力から抽出
    extracted = collect_rows_from_input(sh_in, start_jst, end_jst)
    print(f"🔎 抽出合計: {len(extracted)} 件")

    # 出力ワークシート確保
    ws_out = ensure_output_worksheet(sh_out, date_sheet)

    # 既存URL収集（C列）
    existing = read_existing_urls(ws_out)
    print(f"🧮 既存URL数: {len(existing)} 件")

    # 追記（重複除外）
    added = append_rows_dedup(ws_out, extracted, existing)

    # ③ Gemini分類 & ② H列の欠損補完を実施
    classify_with_gemini(ws_out)

    print("✅ 完了")
    if added:
        print(f"✨ 新規追加: {added} 件")
    else:
        print("✨ 追加なし")


if __name__ == "__main__":
    main()
