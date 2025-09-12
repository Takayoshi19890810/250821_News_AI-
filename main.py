# main.py — 新規作成時のみ左端（index=0）へ。既存は移動しない版
# --------------------------------------------------------
# 使い方例:
#   SHEET_ID=<スプレッドシートID> GOOGLE_CREDENTIALS='<SA JSON>' python main.py --date 250912
#   ※--date を省略すると、JSTの今日の日付(yymmdd)でシートを用意します。
#
# 認証:
#   - GOOGLE_CREDENTIALS : サービスアカウントJSONの文字列（推奨）
#   - または GOOGLE_APPLICATION_CREDENTIALS : JSONファイルのパス
#
# シート指定:
#   - --sheet-id オプション、または環境変数 SHEET_ID
#
# 仕様:
#   - 対象日付のシートが「存在しない」場合のみ → 左端(index=0)に作成（Base複製 or 空シート作成）
#   - 対象日付のシートが「既に存在」する場合 → 位置は変更しない（移動しない）

import os
import json
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread

# Baseが無い場合に使うヘッダ行（必要に応じて調整）
OUTPUT_HEADERS = [
    "No",               # A
    "タイトル",         # B
    "URL",              # C
    "投稿日時",         # D (YYYY/MM/DD HH:MM)
    "ニュース元",       # E (Yahoo / Google / MSN など)
    "ポジネガ",         # F
    "カテゴリ",         # G
    "重複確認用タイトル",# H
    "有料"              # I（有料記事判定用）
]

JST = ZoneInfo("Asia/Tokyo")


def get_default_date_str() -> str:
    """JST現在日時から yymmdd 文字列を返す。"""
    return datetime.now(JST).strftime("%y%m%d")


def load_gspread_client():
    """
    gspread クライアントを初期化して返す。
    - GOOGLE_CREDENTIALS に JSON 文字列が入っている場合はそれを使用
    - それ以外は GOOGLE_APPLICATION_CREDENTIALS のファイルパスを参照
    """
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if creds_json:
        sa_info = json.loads(creds_json)
        return gspread.service_account_from_dict(sa_info)

    if creds_path and os.path.exists(creds_path):
        return gspread.service_account(filename=creds_path)

    raise RuntimeError(
        "サービスアカウント認証情報が見つかりません。\n"
        "環境変数 GOOGLE_CREDENTIALS（JSON文字列）または "
        "GOOGLE_APPLICATION_CREDENTIALS（JSONファイルパス）を設定してください。"
    )


def open_spreadsheet(gc: gspread.client.Client, sheet_id: str):
    """スプレッドシートを開いて返す。"""
    if not sheet_id:
        raise ValueError("スプレッドシートIDが指定されていません。--sheet-id または環境変数 SHEET_ID を設定してください。")
    return gc.open_by_key(sheet_id)


def duplicate_base_or_create_blank(sh, title: str):
    """
    Baseシートがあれば複製して `title` に改名し、左端へ配置。
    無い場合は空シートを作成してヘッダを書き、左端へ配置。
    """
    try:
        base_ws = sh.worksheet("Base")
        # Baseを複製（複製直後は末尾にできることが多い）
        new_ws = base_ws.duplicate(new_sheet_name=title)
        # 新規作成時のみ左端へ移動
        new_ws.update_index(0)
        return new_ws
    except gspread.WorksheetNotFound:
        # Baseが無い場合は空シートを左端へ作成し、ヘッダを投入
        new_ws = sh.add_worksheet(title=title, rows=2000, cols=len(OUTPUT_HEADERS), index=0)
        new_ws.append_row(OUTPUT_HEADERS, value_input_option="USER_ENTERED")
        return new_ws


def ensure_output_worksheet(sh, title: str):
    """
    出力用ワークシート（例: '250912'）を確保。
    - 既存の場合: そのまま返す（位置は変更しない）
    - 無い場合  : Base複製 or 空シート作成（どちらも左端に配置）
    """
    try:
        ws = sh.worksheet(title)
        # 既存は移動しない
        return ws
    except gspread.WorksheetNotFound:
        return duplicate_base_or_create_blank(sh, title)


def parse_args():
    parser = argparse.ArgumentParser(description="ニュース用: 日付シート作成（新規時のみ左端）ユーティリティ")
    parser.add_argument(
        "--sheet-id",
        type=str,
        default=os.environ.get("SHEET_ID"),
        help="対象スプレッドシートID（未指定なら環境変数 SHEET_ID を参照）",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="シート名の日付文字列（yymmdd）。未指定ならJSTの今日を使用",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    date_title = args.date or get_default_date_str()

    print(f"🔧 対象シートID: {args.sheet_id or '(環境変数SHEET_ID未設定)'}")
    print(f"📄 対象ワークシート名: {date_title}")

    gc = load_gspread_client()
    sh = open_spreadsheet(gc, args.sheet_id)

    # 日付シートを確保（新規時のみ左端）
    pre_titles = [ws.title for ws in sh.worksheets()]
    created = date_title not in pre_titles

    ws = ensure_output_worksheet(sh, date_title)

    if created:
        print(f"✅ 新規作成 → 左端に配置しました。現在のindex={sh.worksheets().index(ws)}（左端=0）")
    else:
        print("ℹ️ 既存シートを使用（位置は変更していません）。")

    # --- ここから下は既存のニュース抽出＆書き込み処理を呼び出してください ---
    # 例:
    # rows_to_append = [
    #     [1, "タイトル例", "https://example.com", "2025/09/12 14:30", "Yahoo", "", "", "タイトル例（正規化）", ""]
    # ]
    # if rows_to_append:
    #     ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
    # -----------------------------------------------------------------------------


if __name__ == "__main__":
    main()
