# main.py — 日付シートを常に左端（index=0）へ配置する版
# --------------------------------------------------------
# 使い方例:
#   SHEET_ID=<スプレッドシートID> GOOGLE_CREDENTIALS='<SA JSON>' python main.py --date 250912
#   ※--date を省略すると、JSTの今日の日付(yymmdd)でシートを用意します。
#
# 前提:
#   - サービスアカウントJSONは環境変数 GOOGLE_CREDENTIALS（JSON文字列）または
#     GOOGLE_APPLICATION_CREDENTIALS（ファイルパス）のどちらかで与えてください。
#   - シートIDは環境変数 SHEET_ID または --sheet-id オプションで指定できます。
#   - 既存運用どおり「Base」シートがあれば、それを複製して日付名シートを作成します。
#     無い場合は、空シートを作成し、ヘッダを書き込みます。
#
# 変更ポイント:
#   - 新規作成時: add_worksheet(..., index=0) で一番左に作成
#   - 既存の場合: update_index(0) で左端へ移動

import os
import json
import argparse
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import gspread

# 任意：ヘッダ行（Baseが無い場合に使用）
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
    now = datetime.now(JST)
    return now.strftime("%y%m%d")


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
        "サービスアカウント認証情報が見つかりません。"
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
        # Base を複製（複製は右端にできることが多い）
        new_ws = base_ws.duplicate(new_sheet_name=title)
        # 左端へ移動
        new_ws.update_index(0)
        return new_ws
    except gspread.WorksheetNotFound:
        # Base が無い場合は新規作成（左端）
        new_ws = sh.add_worksheet(title=title, rows=2000, cols=len(OUTPUT_HEADERS), index=0)
        # ヘッダをセット
        new_ws.append_row(OUTPUT_HEADERS, value_input_option="USER_ENTERED")
        return new_ws


def ensure_output_worksheet(sh, title: str):
    """
    出力用ワークシート（例: '250912'）を確保し、必ず左端（index=0）に配置する。
    - 既存の場合: 左端へ移動のみ
    - 無い場合  : Base複製 or 空シート作成（いずれも左端）
    """
    try:
        ws = sh.worksheet(title)
        # 既存でも左端へ
        ws.update_index(0)
        return ws
    except gspread.WorksheetNotFound:
        return duplicate_base_or_create_blank(sh, title)


def parse_args():
    parser = argparse.ArgumentParser(description="ニュース用 日付シート左端配置ユーティリティ")
    parser.add_argument("--sheet-id", type=str, default=os.environ.get("SHEET_ID"), help="対象スプレッドシートID")
    parser.add_argument("--date", type=str, default=None, help="シート名の日付文字列（yymmdd）。未指定ならJSTの今日を使用")
    return parser.parse_args()


def main():
    args = parse_args()
    date_title = args.date or get_default_date_str()

    print(f"🔧 対象シートID: {args.sheet_id or '(環境変数SHEET_ID未設定)'}")
    print(f"📄 生成/移動するワークシート名: {date_title}")

    gc = load_gspread_client()
    sh = open_spreadsheet(gc, args.sheet_id)

    # 日付シートを確保し、左端へ
    ws = ensure_output_worksheet(sh, date_title)

    # ここから下は、あなたの既存「ニュース抽出＆書き込みロジック」を呼び出してください。
    # 例:
    # rows_to_append = [
    #     [1, "タイトル例", "https://example.com", "2025/09/12 14:30", "Yahoo", "", "", "タイトル例（正規化）", ""]
    # ]
    # if rows_to_append:
    #     ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")

    # 動作確認向けメッセージ
    idx = sh.worksheets().index(ws)  # 0 なら先頭
    print(f"✅ ワークシート '{date_title}' を index={idx}（左端が0）に配置しました。")


if __name__ == "__main__":
    main()
