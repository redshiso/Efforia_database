import re
import base64
import io
from pathlib import Path
from urllib.parse import urlencode
import streamlit as st
from PIL import Image
import mysql.connector
import pandas as pd
import altair as alt
import openpyxl

st.set_page_config(page_title="エフフォーリア産駒データベース", layout="wide")

st.markdown("""
<style>
div[data-testid="stHorizontalBlock"] div[data-testid="stButton"] button {
    border: 2px solid #1a73e8 !important;
    border-radius: 6px !important;
    color: #1a73e8 !important;
    background: #f0f4ff !important;
    background-color: #f0f4ff !important;
    font-weight: 500 !important;
    white-space: normal !important;
    overflow: visible !important;
    text-overflow: unset !important;
    width: 100% !important;
    min-width: 120px !important;
}
div[data-testid="stHorizontalBlock"] div[data-testid="stButton"] button:hover {
    background: #1a73e8 !important;
    background-color: #1a73e8 !important;
    color: white !important;
}
.analysis-card {
    background: #f0f6ff; border-left: 4px solid #4a90d9;
    border-radius: 0 8px 8px 0; padding: 16px 20px;
    margin: 16px 0; font-size: 0.97em; line-height: 1.8; color: #222;
}
.note-box {
    background: #fffbf0; border-left: 4px solid #f0a500;
    border-radius: 0 8px 8px 0; padding: 16px 20px;
    margin: 16px 0; font-size: 0.95em; line-height: 1.8; color: #333;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# DB接続
# ─────────────────────────────────────────
db_config = {
    'user':                st.secrets["DB_USER"],
    'password':            st.secrets["DB_PASSWORD"],
    'host':                st.secrets["DB_HOST"],
    'port':                st.secrets["DB_PORT"],
    'database':            st.secrets["DB_NAME"],
    'ssl_disabled':        False,
    'ssl_verify_cert':     False,
    'ssl_verify_identity': False
}

def get_connection():
    return mysql.connector.connect(**db_config)

def compress_image(file_bytes, max_px=1920, quality=85):
    img = Image.open(io.BytesIO(file_bytes))
    img = img.convert("RGB")
    img.thumbnail((max_px, max_px), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue(), "image/jpeg"

@st.cache_data(ttl=300, show_spinner=False)
def _cached_query(sql, params):
    conn = get_connection()
    df = pd.read_sql(sql, conn, params=list(params) if params else None)
    conn.close()
    return df

def run_query(sql, params=None):
    # Streamlitは開いていないタブの中身も毎回実行するため、
    # 同一クエリはキャッシュから返してDBへの往復を減らす
    return _cached_query(sql, tuple(params) if params else ())

def clear_query_cache():
    _cached_query.clear()

def run_write(sql, params=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params or [])
    conn.commit(); cursor.close(); conn.close()
    clear_query_cache()

@st.cache_resource(show_spinner=False)
def ensure_schema():
    """スキーマ補正。プロセス起動時に一度だけ実行し、毎回のALTER発行を避ける。"""
    for stmt in [
        "ALTER TABLE horse_images ADD COLUMN photographer VARCHAR(100) DEFAULT NULL",
        "ALTER TABLE horses MODIFY COLUMN breeder_id INT DEFAULT NULL",
    ]:
        try:
            run_write(stmt)
        except Exception:
            pass  # 列がすでに存在する / すでにNULL許容の場合は無視
    return True

ensure_schema()

# ─────────────────────────────────────────
# 分析ノート
# ─────────────────────────────────────────
def get_note(axis_key):
    try:
        df = run_query("SELECT note_text FROM analysis_notes WHERE axis_key=%s", [axis_key])
        return df.iloc[0]['note_text'] if not df.empty else ""
    except Exception:
        return ""

def save_note(axis_key, text):
    run_write("""
        INSERT INTO analysis_notes (axis_key, note_text)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE note_text=%s, updated_at=CURRENT_TIMESTAMP
    """, [axis_key, text, text])

# ─────────────────────────────────────────
# 分析画像
# ─────────────────────────────────────────
def get_analysis_images(axis_key):
    try:
        return run_query(
            "SELECT image_id, caption, image_data, mime_type FROM analysis_images WHERE axis_key=%s ORDER BY created_at",
            [axis_key]
        )
    except Exception:
        return pd.DataFrame()

def save_analysis_image(axis_key, caption, image_bytes, mime_type):
    b64 = base64.b64encode(image_bytes).decode('utf-8')
    run_write(
        "INSERT INTO analysis_images (axis_key, caption, image_data, mime_type) VALUES (%s,%s,%s,%s)",
        [axis_key, caption, b64, mime_type]
    )

def delete_analysis_image(image_id):
    run_write("DELETE FROM analysis_images WHERE image_id=%s", [image_id])

# ─────────────────────────────────────────
# 産駒全体サマリー
# ─────────────────────────────────────────
def render_overall_summary(category="全て"):
    # 開催区分の絞り込みはサブクエリ側で行い、登録頭数は常に全産駒を数える
    loc_cond   = "" if category == "全て" else "WHERE t.location = %s"
    loc_params = []  if category == "全て" else [category]
    df = run_query(f"""
        SELECT
            COUNT(DISTINCT h.horse_id)                                            AS 登録頭数,
            COUNT(re.entry_id)                                                    AS 総出走数,
            COALESCE(SUM(CASE WHEN re.final_rank=1  THEN 1 ELSE 0 END),0)         AS 総勝利数,
            COALESCE(SUM(CASE WHEN re.final_rank<=3 THEN 1 ELSE 0 END),0)         AS 総複勝数,
            COUNT(DISTINCT CASE WHEN re.entry_id IS NOT NULL THEN h.horse_id END) AS 出走経験頭数,
            COUNT(DISTINCT CASE WHEN re.final_rank=1 THEN h.horse_id END)         AS 勝利経験頭数
        FROM horses h
        LEFT JOIN (
            SELECT re.entry_id, re.horse_id, re.final_rank
            FROM raceentries re
            JOIN races  r ON re.race_id=r.race_id
            JOIN tracks t ON r.track_id=t.track_id
            {loc_cond}
        ) re ON h.horse_id=re.horse_id
        WHERE h.sire_id=222
    """, loc_params)
    s = df.iloc[0]
    total_starts  = int(s['総出走数'])
    wins          = int(s['総勝利数'])
    placed        = int(s['総複勝数'])
    starters      = int(s['出走経験頭数'])
    winners       = int(s['勝利経験頭数'])
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("登録頭数",     f"{int(s['登録頭数'])}頭")
    c2.metric("出走経験頭数", f"{starters}頭")
    c3.metric("総出走数",     f"{total_starts}回")
    c4.metric("勝ち上がり率", f"{winners/starters*100:.1f}%" if starters else "―",
              help=f"1勝以上した頭数 {winners}頭 ÷ 出走経験頭数 {starters}頭")
    c5.metric("勝率",         f"{wins/total_starts*100:.1f}%" if total_starts else "―")
    c6.metric("複勝率",       f"{placed/total_starts*100:.1f}%" if total_starts else "―")

# ─────────────────────────────────────────
# 産駒分析グラフ＋考察＋画像
# ─────────────────────────────────────────
def render_analysis_section(axis_key, top_n=15, year_from=2024, year_to=2026,
                            foal_year_from=None, foal_year_to=None, category="全て"):
    axis_map = {
        '母父別':   ('hf.broodmare_sire_name', '母父'),
        '生産者別': ('hf.breeder_name',         '生産者'),
        '騎手別':   ('j.jockey_name',            '騎手'),
        '馬主別':   ('re.owner',                 '馬主'),
    }
    col_expr, col_alias = axis_map[axis_key]
    loc_cond   = "" if category == "全て" else "AND t.location = %s"
    loc_params = []  if category == "全て" else [category]
    sql = f"""
        SELECT
            {col_expr}                                                       AS `{col_alias}`,
            COUNT(DISTINCT h.horse_id)                                       AS 頭数,
            COUNT(re.entry_id)                                               AS 出走数,
            COALESCE(SUM(CASE WHEN re.final_rank=1  THEN 1 ELSE 0 END),0)   AS 勝利数,
            COALESCE(SUM(CASE WHEN re.final_rank<=3 THEN 1 ELSE 0 END),0)   AS 複勝数
        FROM horses h
        LEFT JOIN horses_formatted hf ON h.horse_id=hf.horse_id
        LEFT JOIN raceentries re       ON h.horse_id=re.horse_id
        LEFT JOIN races r              ON re.race_id=r.race_id
        LEFT JOIN tracks t             ON r.track_id=t.track_id
        LEFT JOIN jockeys  j           ON re.jockey_id=j.jockey_id
        LEFT JOIN trainers tr          ON re.trainer_id=tr.trainer_id
        WHERE h.sire_id=222 AND {col_expr} IS NOT NULL
          AND YEAR(r.race_date) BETWEEN {year_from} AND {year_to}
          {loc_cond}
          {"AND YEAR(h.date_of_birth) BETWEEN " + str(foal_year_from) + " AND " + str(foal_year_to) if foal_year_from and foal_year_to else ""}
        GROUP BY {col_expr}
        HAVING 出走数 > 0
        ORDER BY 出走数 DESC
        LIMIT {top_n}
    """
    df = run_query(sql, loc_params)
    if df.empty:
        st.info("データがありません。"); return

    df['勝率(%)']  = (df['勝利数'] / df['出走数'] * 100).round(1)
    df['複勝率(%)'] = (df['複勝数'] / df['出走数'] * 100).round(1)
    sort_order = df[col_alias].tolist()

    metric = st.radio("表示指標", ["勝利数","出走数","勝率(%)","複勝率(%)"],
                      horizontal=True, key=f"metric_{axis_key}")
    chart = alt.Chart(df).mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
        x=alt.X(f'{col_alias}:N', sort=sort_order, title=None,
                axis=alt.Axis(labelAngle=-40, labelOverlap=False)),
        y=alt.Y(f'{metric}:Q', title=metric),
        color=alt.Color(f'{metric}:Q', scale=alt.Scale(scheme='blues'), legend=None),
        tooltip=[col_alias,'頭数','出走数','勝利数','勝率(%)','複勝率(%)']
    ).properties(height=360)
    st.altair_chart(chart, width="stretch")

    with st.expander("詳細テーブル"):
        st.dataframe(df[[col_alias,'頭数','出走数','勝利数','複勝数','勝率(%)','複勝率(%)']],
                     hide_index=True)

    note = get_note(axis_key)
    if note:
        st.markdown(f"<div class='note-box'><strong>考察</strong><br>{note}</div>",
                    unsafe_allow_html=True)

    df_imgs = get_analysis_images(axis_key)
    if not df_imgs.empty:
        img_cols = st.columns(min(len(df_imgs), 3))
        for i, (_, img_row) in enumerate(df_imgs.iterrows()):
            with img_cols[i % 3]:
                st.image(
                    f"data:{img_row['mime_type']};base64,{img_row['image_data']}",
                    caption=img_row['caption'] or "",
                    width="stretch"
                )
                if st.session_state.is_admin:
                    if st.button("削除", key=f"del_aimg_{img_row['image_id']}"):
                        delete_analysis_image(int(img_row['image_id'])); st.rerun()

    if st.session_state.is_admin:
        with st.expander(f"「{axis_key}」の考察・画像を編集"):
            st.markdown("**考察テキスト**")
            new_note = st.text_area("考察を入力（Markdown対応）", value=note,
                                    height=150, key=f"note_input_{axis_key}")
            if st.button("考察を保存", key=f"save_note_{axis_key}", type="primary"):
                save_note(axis_key, new_note)
                st.success("保存しました！"); st.rerun()
            st.markdown("---")
            st.markdown("**画像を追加**")
            uploaded = st.file_uploader("画像ファイル（JPG / PNG）",
                                        type=["jpg","jpeg","png"],
                                        key=f"upload_{axis_key}")
            img_caption = st.text_input("キャプション（任意）", key=f"caption_{axis_key}")
            if st.button("画像を追加", key=f"add_img_{axis_key}"):
                if uploaded:
                    mime = "image/png" if uploaded.name.endswith(".png") else "image/jpeg"
                    save_analysis_image(axis_key, img_caption, uploaded.read(), mime)
                    st.success("画像を追加しました！"); st.rerun()
                else:
                    st.warning("画像ファイルを選択してください。")

# ─────────────────────────────────────────
# 記事本文レンダリング
# {{image:ラベル}} / {{image:ラベル:サイズ}} / {{graph:軸}} に対応
# ─────────────────────────────────────────
def render_article_content(content, images_dict):
    tag_pattern = r'(\{\{(?:image|graph):[^}]+\}\})'
    parts = re.split(tag_pattern, content)
    for part in parts:
        # {{graph:軸}} タグ
        m_graph = re.match(r'\{\{graph:(.+?)\}\}', part)
        if m_graph:
            axis = m_graph.group(1).strip()
            if axis in ['母父別', '生産者別', '騎手別']:
                st.markdown(
                    f"<div style='background:#f0f6ff;border-radius:10px;padding:16px;margin:20px 0'>"
                    f"<p style='color:#4a90d9;font-size:0.8em;font-weight:600;margin:0 0 8px'>{axis}</p>",
                    unsafe_allow_html=True
                )
                render_analysis_section(axis, top_n=10)
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                st.warning(f"不明なグラフ軸: '{axis}'")
            continue

        # {{image:ラベル}} / {{image:ラベル:サイズ}} タグ
        m_image = re.match(r'\{\{image:([^}]+)\}\}', part)
        if m_image:
            tokens = m_image.group(1).strip().split(':', 1)
            label = tokens[0]
            size  = tokens[1] if len(tokens) > 1 else None
            if label in images_dict:
                img = images_dict[label]
                if size:
                    caption_html = f'<br><small>{img["caption"]}</small>' if img['caption'] else ''
                    st.markdown(
                        f'<div style="text-align:center">'
                        f'<img src="data:{img["mime"]};base64,{img["data"]}" style="width:{size};">'
                        f'{caption_html}</div>',
                        unsafe_allow_html=True
                    )
                else:
                    img_bytes = base64.b64decode(img['data'])
                    st.image(img_bytes, caption=img['caption'] or None, width="stretch")
            else:
                st.warning(f"画像 '{{{{image:{label}}}}}' が見つかりません")
            continue

        # 通常テキスト
        if part.strip():
            st.markdown(part, unsafe_allow_html=True)

# ─────────────────────────────────────────
# session_state 初期化
# ─────────────────────────────────────────
for k, v in [('page','list'), ('selected_horse_id',None),
             ('selected_horse_name',""), ('selected_article_id',None),
             ('edit_article_id',None), ('is_admin',False)]:
    if k not in st.session_state:
        st.session_state[k] = v


def go_detail(horse_id, horse_name):
    st.session_state.selected_horse_id   = horse_id
    st.session_state.selected_horse_name = horse_name
    st.session_state.page = 'detail'

def go_list():
    st.session_state.page = 'list'
    # 一覧テーブルの行選択を解除しておかないと、戻った瞬間に再び詳細へ飛んでしまう
    st.session_state.pop('horse_table', None)

def go_article(article_id):
    st.session_state.selected_article_id = article_id
    st.session_state.page = 'article'

def go_list_article_tab():
    st.session_state.page = 'list'
    st.session_state.selected_article_id = None

# ─────────────────────────────────────────
# 条件検索のURL共有
# 検索条件をクエリパラメータに載せ、URLだけで同じ条件を再現できるようにする
# ─────────────────────────────────────────
CS_OPTIONS = {
    "surface":   ["芝", "ダート"],
    "condition": ["良", "稍重", "重", "不良"],
    "gender":    ["牡", "牝", "騸"],
    "class":     ["新馬","未勝利","1勝クラス","2勝クラス","3勝クラス","オープン","G3","G2","G1"],
    "style":     ["逃げ", "先行", "差し", "追込"],
}
CS_CATEGORIES = ["全て", "中央", "地方", "海外"]
# track / dir は選択肢がDB由来のため、値の検証はウィジェット生成直前に行う
CS_MULTI_PARAMS = list(CS_OPTIONS) + ["track", "dir"]
CS_INT_RANGE = {
    "dist_from":   (800, 4300), "dist_to":   (800, 4300),
    "year_from":   (2020, 2030), "year_to":   (2020, 2030),
    "weight_from": (0, 700),     "weight_to": (0, 700),
}
CS_DEFAULTS = {
    "dist_from": 800,  "dist_to": 4300,
    "year_from": 2024, "year_to": 2026,
    "weight_from": 0,  "weight_to": 700,
    "jockey": "", "trainer": "", "category": "全て",
}

def restore_search_from_url():
    """URLのクエリパラメータを条件検索ウィジェットの初期値としてsession_stateへ復元する。"""
    qp = st.query_params
    restored = False
    for name in CS_MULTI_PARAMS:
        if name not in qp:
            continue
        vals = [v for v in qp.get_all(name) if v]
        if name in CS_OPTIONS:                      # 不正な値はここで捨てる
            vals = [v for v in vals if v in CS_OPTIONS[name]]
        if vals:
            st.session_state[f"cs_{name}"] = vals; restored = True
    for name, (lo, hi) in CS_INT_RANGE.items():
        if name not in qp:
            continue
        try:
            st.session_state[f"cs_{name}"] = max(lo, min(hi, int(qp[name]))); restored = True
        except ValueError:
            pass                                    # 数値でなければ無視してデフォルトのまま
    for name in ("jockey", "trainer"):
        if name in qp:
            st.session_state[f"cs_{name}"] = qp[name]; restored = True
    if qp.get("category") in CS_CATEGORIES:
        st.session_state["cs_category"] = qp["category"]; restored = True
    if "weight_filter" in qp:
        st.session_state["cs_weight_filter"] = qp["weight_filter"] == "1"; restored = True
    return restored

def sync_search_to_url():
    """現在の検索条件をURLへ書き戻す。デフォルト値は省略してURLを短く保つ。"""
    params = {}
    for name in CS_MULTI_PARAMS:
        vals = st.session_state.get(f"cs_{name}") or []
        if vals:
            params[name] = list(vals)
    for name in list(CS_INT_RANGE) + ["jockey", "trainer", "category"]:
        val = st.session_state.get(f"cs_{name}", CS_DEFAULTS[name])
        if val not in (None, "") and val != CS_DEFAULTS[name]:
            params[name] = str(val)
    if st.session_state.get("cs_weight_filter"):
        params["weight_filter"] = "1"
    # 実際に変化したときだけ書き込む（毎回の書き換えを避ける）
    encoded = urlencode(params, doseq=True)
    if st.session_state.get("_cs_url") != encoded:
        st.session_state["_cs_url"] = encoded
        st.query_params.from_dict(params)

def reset_search_conditions():
    """検索条件をすべて初期値に戻し、URLからも条件を取り除く。"""
    for name in CS_MULTI_PARAMS + list(CS_INT_RANGE) + \
                ["jockey", "trainer", "category", "weight_filter"]:
        st.session_state.pop(f"cs_{name}", None)
    st.session_state.pop("_cs_url", None)
    st.query_params.clear()

# ══════════════════════════════════════════
# 記事詳細ページ
# ══════════════════════════════════════════
if st.session_state.page == 'article':
    article_id = st.session_state.selected_article_id
    st.button("← コラム一覧に戻る", on_click=go_list_article_tab)
    st.markdown("---")
    try:
        df_article = run_query("""
            SELECT article_id, title, content,
                   DATE_FORMAT(created_at,'%Y年%m月%d日 %H:%i') AS post_date
            FROM articles WHERE article_id=%s
        """, [article_id])
        if df_article.empty:
            st.warning("記事が見つかりませんでした。")
        else:
            row = df_article.iloc[0]
            aid = int(row['article_id'])

            # この記事の画像を取得
            df_imgs = run_query(
                "SELECT label, caption, image_data, mime_type FROM article_images WHERE article_id=%s",
                [aid]
            )
            images_dict = {
                r['label']: {'data': r['image_data'], 'mime': r['mime_type'], 'caption': r['caption']}
                for _, r in df_imgs.iterrows()
            }

            # 編集モード
            if st.session_state.is_admin and st.session_state.edit_article_id == aid:
                st.markdown(f"**「{row['title']}」を編集中**")
                with st.form(key=f"edit_form_{aid}"):
                    et = st.text_input("タイトル", value=row['title'])
                    ec = st.text_area("本文", value=row['content'], height=400)
                    c1, c2 = st.columns([1, 1])
                    if c1.form_submit_button("更新する", type="primary"):
                        run_write("UPDATE articles SET title=%s,content=%s WHERE article_id=%s", [et, ec, aid])
                        st.session_state.edit_article_id = None; st.rerun()
                    if c2.form_submit_button("キャンセル"):
                        st.session_state.edit_article_id = None; st.rerun()
            else:
                st.title(row['title'])
                st.caption(f"公開日時: {row['post_date']}")
                st.markdown("---")

                render_article_content(row['content'], images_dict)

                if st.session_state.is_admin:
                    st.markdown("---")
                    c1, c2, _ = st.columns([1, 1, 8])
                    if c1.button("編集"):
                        st.session_state.edit_article_id = aid; st.rerun()
                    if c2.button("削除"):
                        run_write("DELETE FROM article_images WHERE article_id=%s", [aid])
                        run_write("DELETE FROM articles WHERE article_id=%s", [aid])
                        go_list_article_tab(); st.rerun()

                    # 画像管理
                    st.markdown("---")
                    st.subheader("画像管理")
                    st.caption("本文中に `{{image:ラベル}}` で全幅表示、`{{image:ラベル:50%}}` のようにサイズ指定も可能です。")

                    df_imgs_admin = run_query(
                        "SELECT image_id, label, caption FROM article_images WHERE article_id=%s ORDER BY image_id",
                        [aid]
                    )
                    if not df_imgs_admin.empty:
                        st.markdown("**登録済み画像**")
                        for _, img_row in df_imgs_admin.iterrows():
                            col_lbl, col_del = st.columns([9, 1])
                            cap = img_row['caption'] or '（キャプションなし）'
                            col_lbl.markdown(f"`{{{{image:{img_row['label']}}}}}` — {cap}")
                            if col_del.button("削除", key=f"del_img_{img_row['image_id']}"):
                                run_write("DELETE FROM article_images WHERE image_id=%s",
                                          [int(img_row['image_id'])])
                                st.rerun()

                    with st.expander("＋ 画像をアップロード"):
                        label_input   = st.text_input("ラベル（半角英数字推奨）", key=f"img_label_{aid}",
                                                       help="例: fig1 → 本文中に {{image:fig1}} と記述")
                        caption_input = st.text_input("キャプション（任意）", key=f"img_caption_{aid}")
                        uploaded      = st.file_uploader("画像ファイル",
                                                          type=["png","jpg","jpeg","gif","webp"],
                                                          key=f"img_upload_{aid}")
                        if st.button("アップロード", key=f"img_upload_btn_{aid}"):
                            if uploaded and label_input:
                                img_b64 = base64.b64encode(uploaded.read()).decode('utf-8')
                                run_write(
                                    "INSERT INTO article_images (article_id, label, caption, image_data, mime_type) "
                                    "VALUES (%s,%s,%s,%s,%s)",
                                    [aid, label_input, caption_input or None, img_b64, uploaded.type]
                                )
                                st.success(f"アップロード完了。本文中に `{{{{image:{label_input}}}}}` と記述すると表示されます。")
                                st.rerun()
                            else:
                                st.warning("ラベルと画像ファイルを指定してください。")

    except Exception as e:
        st.error(f"記事の読み込みに失敗しました: {e}")

# ══════════════════════════════════════════
# 馬詳細ページ
# ══════════════════════════════════════════
elif st.session_state.page == 'detail':
    horse_id   = st.session_state.selected_horse_id
    horse_name = st.session_state.selected_horse_name

    st.button("← 一覧に戻る", on_click=go_list)
    st.title(f"{horse_name}")
    st.markdown("---")

    # 写真 ＋ 基本情報 ＋ 血統
    try:
        df_profile = run_query("""
            SELECT hf.date_of_birth AS 生年月日, hf.gender AS 性別, hf.color AS 毛色,
                   hf.bloodline AS 血統,
                   COALESCE(
                       (SELECT re2.owner FROM raceentries re2
                        JOIN races r2 ON re2.race_id = r2.race_id
                        WHERE re2.horse_id = hf.horse_id
                        ORDER BY r2.race_date DESC LIMIT 1),
                       h.Owner
                   ) AS 馬主,
                   hf.breeder_name AS 生産者,
                   hf.sire_name AS 父, hf.dam_name AS 母,
                   hf.broodmare_sire_name AS 母父, hf.trainer_name AS 調教師,
                   tr.region AS region_raw
            FROM horses_formatted hf
            JOIN horses h ON hf.horse_id=h.horse_id
            LEFT JOIN trainers tr ON h.trainer_id = tr.trainer_id
            WHERE hf.horse_id=%s
        """, [horse_id])
        if not df_profile.empty:
            p = df_profile.iloc[0]

            region = p['region_raw']
            if region in ['美浦', '栗東']:
                shozoku = region
            elif region:
                shozoku = f"地方（{region}）"
            else:
                shozoku = '―'

            df_horse_img = run_query(
                "SELECT image_data, mime_type, photographer FROM horse_images WHERE horse_id=%s", [horse_id]
            )
            col_photo, col_info, col_blood = st.columns([2, 3, 3])

            with col_photo:
                st.subheader("写真")
                if not df_horse_img.empty:
                    img_row = df_horse_img.iloc[0]
                    img_bytes = base64.b64decode(img_row['image_data'])
                    st.image(img_bytes, width="stretch")
                    photographer = img_row.get('photographer') or ''
                    if photographer:
                        st.markdown(
                            f"<div style='text-align:right; color:#888; font-size:0.8em;'>"
                            f"📷 {photographer}</div>",
                            unsafe_allow_html=True
                        )
                    if st.session_state.is_admin:
                        new_photographer = st.text_input(
                            "撮影者名", value=photographer, key="photographer_input"
                        )
                        col_save, col_del = st.columns(2)
                        if col_save.button("撮影者を保存", key="save_photographer"):
                            run_write(
                                "UPDATE horse_images SET photographer=%s WHERE horse_id=%s",
                                [new_photographer or None, horse_id]
                            )
                            st.rerun()
                        if col_del.button("写真を削除", key="del_horse_img"):
                            run_write("DELETE FROM horse_images WHERE horse_id=%s", [horse_id])
                            st.rerun()
                else:
                    st.markdown(
                        "<div style='border:2px dashed #ccc; border-radius:8px; "
                        "height:180px; display:flex; align-items:center; "
                        "justify-content:center; color:#aaa; font-size:0.9em;'>"
                        "No Image</div>",
                        unsafe_allow_html=True
                    )
                    if st.session_state.is_admin:
                        uploaded_horse = st.file_uploader("写真をアップロード",
                                                           type=["png","jpg","jpeg","webp"],
                                                           key="horse_img_upload")
                        photographer_input = st.text_input("撮影者名（任意）", key="photographer_new")
                        if uploaded_horse:
                            compressed, mime = compress_image(uploaded_horse.read())
                            img_b64 = base64.b64encode(compressed).decode('utf-8')
                            run_write(
                                "INSERT INTO horse_images (horse_id, image_data, mime_type, photographer) "
                                "VALUES (%s,%s,%s,%s)",
                                [horse_id, img_b64, mime,
                                 photographer_input or None]
                            )
                            st.rerun()

            with col_info:
                st.subheader("基本情報")
                st.markdown(f"""
<table style="width:100%; border-collapse:collapse;">
<tr><td style="padding:4px 8px; color:#888; width:40%;">生年月日</td><td style="padding:4px 8px;">{p['生年月日']}</td></tr>
<tr><td style="padding:4px 8px; color:#888;">性別</td><td style="padding:4px 8px;">{p['性別']}</td></tr>
<tr><td style="padding:4px 8px; color:#888;">毛色</td><td style="padding:4px 8px;">{p['毛色'] or '―'}</td></tr>
<tr><td style="padding:4px 8px; color:#888;">馬主</td><td style="padding:4px 8px;">{p['馬主'] or '―'}</td></tr>
<tr><td style="padding:4px 8px; color:#888;">生産者</td><td style="padding:4px 8px;">{p['生産者'] or '―'}</td></tr>
<tr><td style="padding:4px 8px; color:#888;">調教師</td><td style="padding:4px 8px;">{p['調教師'] or '―'}</td></tr>
<tr><td style="padding:4px 8px; color:#888;">所属</td><td style="padding:4px 8px;">{shozoku}</td></tr>
</table>
""", unsafe_allow_html=True)

            with col_blood:
                st.subheader("血統")
                st.markdown(f"""
<table style="width:100%; border-collapse:collapse;">
<tr><td style="padding:4px 8px; color:#888; width:40%;">父</td><td style="padding:4px 8px;">{p['父'] or '―'}</td></tr>
<tr><td style="padding:4px 8px; color:#888;">母</td><td style="padding:4px 8px;">{p['母'] or '―'}</td></tr>
<tr><td style="padding:4px 8px; color:#888;">母父</td><td style="padding:4px 8px;">{p['母父'] or '―'}</td></tr>
</table>
""", unsafe_allow_html=True)
    except Exception as e:
        st.error(f"基本情報の取得に失敗しました: {e}")

    st.markdown("---")
    st.subheader("通算成績")
    try:
        df_sum = run_query("""
            SELECT COUNT(*) AS 出走数,
                   COALESCE(SUM(CASE WHEN final_rank=1  THEN 1 ELSE 0 END),0) AS 一着,
                   COALESCE(SUM(CASE WHEN final_rank=2  THEN 1 ELSE 0 END),0) AS 二着,
                   COALESCE(SUM(CASE WHEN final_rank=3  THEN 1 ELSE 0 END),0) AS 三着,
                   COALESCE(SUM(CASE WHEN final_rank>=4 THEN 1 ELSE 0 END),0) AS 四着以下
            FROM raceentries WHERE horse_id=%s
        """, [horse_id])
        s = df_sum.iloc[0]
        total,w1,w2,w3,w4 = int(s['出走数']),int(s['一着']),int(s['二着']),int(s['三着']),int(s['四着以下'])
        if total == 0:
            st.markdown("### 0戦0勝　<span style='color:#888'>(未出走)</span>", unsafe_allow_html=True)
        else:
            st.markdown(f"### {total}戦{w1}勝　<span style='color:#555'>({w1}-{w2}-{w3}-{w4})</span>",
                        unsafe_allow_html=True)
            m1,m2,m3,m4 = st.columns(4)
            m1.metric("勝率",   f"{w1/total*100:.1f}%")
            m2.metric("連対率", f"{(w1+w2)/total*100:.1f}%")
            m3.metric("複勝率", f"{(w1+w2+w3)/total*100:.1f}%")
            m4.metric("3着内数",f"{w1+w2+w3}回")
    except Exception as e:
        st.error(f"通算成績の取得に失敗しました: {e}")

    st.markdown("---")
    st.subheader("出走履歴")
    try:
        df_entries = run_query("""
            SELECT r.race_date AS 開催日, r.race_name AS レース名, t.track_name AS 競馬場,
                   r.distance_meters AS 距離_m, r.surface_type AS 馬場種別,
                   r.track_condition AS 馬場状態, r.race_class AS クラス,
                   re.final_rank AS 着順,
                   CASE WHEN re.time_seconds IS NULL THEN '―'
                        ELSE CONCAT(
                            FLOOR((re.time_seconds + COALESCE(re.time_diff_seconds,0))/60),':',
                            LPAD(FLOOR((re.time_seconds + COALESCE(re.time_diff_seconds,0))%60),2,'0'),'.',
                            TRUNCATE(((re.time_seconds + COALESCE(re.time_diff_seconds,0))*10)%10,0))
                   END AS タイム,
                   IFNULL(FORMAT(re.last_3f_seconds,1),'―') AS 上がり3F,
                   re.running_style AS 脚質, re.race_pace AS ペース,
                   re.Weight AS 斤量, re.horse_weight AS 馬体重_kg,
                   re.weight_diff AS 体重増減,
                   j.jockey_name AS 騎手, tr.trainer_name AS 調教師
            FROM raceentries re
            JOIN races r ON re.race_id=r.race_id JOIN tracks t ON r.track_id=t.track_id
            LEFT JOIN jockeys j ON re.jockey_id=j.jockey_id
            LEFT JOIN trainers tr ON re.trainer_id=tr.trainer_id
            WHERE re.horse_id=%s ORDER BY r.race_date DESC
        """, [horse_id])
        if not df_entries.empty:
            def fmt_weight(row):
                w = row['馬体重_kg']
                d = row['体重増減']
                if pd.isna(w):
                    return '―'
                w = int(w)
                if pd.isna(d):
                    return str(w)
                d = int(d)
                sign = '+' if d >= 0 else ''
                return f"{w}({sign}{d})"
            df_entries.insert(
                df_entries.columns.get_loc('体重増減') + 1,
                '馬体重',
                df_entries.apply(fmt_weight, axis=1)
            )
            df_entries = df_entries.drop(columns=['馬体重_kg', '体重増減'])
        if df_entries.empty: st.info("出走履歴がありません。")
        else: st.dataframe(df_entries, hide_index=True)
    except Exception as e:
        st.error(f"出走履歴の取得に失敗しました: {e}")

# ══════════════════════════════════════════
# 一覧ページ
# ══════════════════════════════════════════
else:
    st.title("エフフォーリア産駒データベース")

    # サイドバー
    st.sidebar.markdown("---")
    st.sidebar.header("検索条件")
    horse_name_input = st.sidebar.text_input("馬名（一部でも可）", value="")
    selected_gender  = st.sidebar.radio("性別", ["すべて","牡","牝","騸"])
    st.sidebar.markdown("**産年（生年）**")
    yc1,yc2 = st.sidebar.columns(2)
    birth_year_from = yc1.number_input("From", min_value=2000, max_value=2040, value=2022, step=1)
    birth_year_to   = yc2.number_input("To",   min_value=2000, max_value=2040, value=2024, step=1)

    try:
        color_df = run_query(
            "SELECT DISTINCT color FROM horses WHERE sire_id=222 AND color IS NOT NULL ORDER BY color"
        )
        color_options = color_df['color'].tolist()
    except Exception:
        color_options = []
    color_sel  = st.sidebar.multiselect("毛色", color_options)
    region_sel = st.sidebar.multiselect("所属", ["美浦", "栗東", "地方"])

    try:
        loc_df = run_query(
            "SELECT DISTINCT b.location FROM horses h "
            "JOIN breeders b ON h.breeder_id=b.breeder_id "
            "WHERE h.sire_id=222 AND b.location IS NOT NULL ORDER BY b.location"
        )
        location_options = loc_df['location'].tolist()
    except Exception:
        location_options = []
    location_sel = st.sidebar.multiselect("生産地", location_options)

    try:
        bms_df = run_query(
            "SELECT DISTINCT hf.broodmare_sire_name FROM horses h "
            "JOIN horses_formatted hf ON h.horse_id=hf.horse_id "
            "WHERE h.sire_id=222 AND hf.broodmare_sire_name IS NOT NULL "
            "ORDER BY hf.broodmare_sire_name"
        )
        bms_options = bms_df['broodmare_sire_name'].tolist()
    except Exception:
        bms_options = []
    bms_sel = st.sidebar.multiselect("母父", bms_options)

    st.sidebar.markdown("---")
    st.sidebar.caption("※ 馬名は部分一致で検索します")

    # 管理者メニュー
    st.sidebar.markdown("---")
    with st.sidebar.expander("管理者メニュー"):
        if not st.session_state.is_admin:
            ap = st.text_input("管理者パスワード", type="password", key="admin_pass")
            if ap == st.secrets["ADMIN_PASSWORD"]:
                st.session_state.is_admin = True; st.rerun()
            elif ap != "":
                st.error("パスワードが間違っています。")
        else:
            st.success("管理者としてログイン中")
            if st.button("ログアウト", key="logout_btn"):
                st.session_state.is_admin = False; st.rerun()
            st.markdown("---")
            st.markdown("**新規記事の投稿**")
            st.caption("`{{image:ラベル}}` で画像挿入 / `{{graph:母父別}}` などでグラフ挿入")
            with st.form(key='post_article_form', clear_on_submit=True):
                at = st.text_input("記事のタイトル")
                ac = st.text_area("本文（Markdown対応）", height=200)
                if st.form_submit_button("記事を公開する"):
                    if at and ac:
                        try:
                            run_write("INSERT INTO articles (title,content) VALUES (%s,%s)", [at, ac])
                            st.success("記事を公開しました！"); st.rerun()
                        except Exception as e:
                            st.error(f"投稿に失敗しました: {e}")
                    else:
                        st.warning("タイトルと本文を入力してください。")

            st.markdown("---")
            st.markdown("**産駒データ一括登録**")

            # CSVテンプレートのダウンロード
            _template_cols = [
                "馬名", "生年月日", "性別", "毛色",
                "母名",
                "調教師名", "所属",
                "生産牧場名",
                "馬主名", "血統"
            ]
            _template_csv = ",".join(_template_cols) + "\n" \
                + ",".join(["サンプル花子", "2025-02-14", "牝", "鹿毛",
                             "サンプル母",
                             "田中調教師", "美浦",
                             "〇〇牧場",
                             "田中オーナー", ""]) + "\n"
            st.download_button(
                "CSVテンプレートをダウンロード",
                data=_template_csv.encode("utf-8-sig"),
                file_name="horse_import_template.csv",
                mime="text/csv",
                key="dl_template"
            )
            st.caption("Excelで編集後、文字コード UTF-8（BOM付き）で保存してアップロードしてください。")

            uploaded_csv = st.file_uploader(
                "CSVファイルをアップロード", type=["csv"], key="horse_csv_upload"
            )
            if uploaded_csv:
                try:
                    df_csv = pd.read_csv(uploaded_csv, encoding="utf-8-sig", dtype=str).fillna("")
                    st.write(f"読み込み: **{len(df_csv)} 頭**")
                    st.dataframe(df_csv, hide_index=True)

                    if st.button("上記データを一括登録する", type="primary", key="bulk_insert_btn"):
                        conn = get_connection()
                        cur  = conn.cursor()
                        ok = skip = err = 0
                        warnings_list = []
                        errors = []

                        for _, r in df_csv.iterrows():
                            horse_name = r.get("馬名", "").strip()
                            if not horse_name:
                                continue
                            try:
                                # 重複チェック
                                cur.execute(
                                    "SELECT horse_id FROM horses WHERE horse_name=%s LIMIT 1",
                                    [horse_name]
                                )
                                if cur.fetchone():
                                    skip += 1
                                    warnings_list.append(f"{horse_name}：すでに登録済みのためスキップ")
                                    continue

                                # trainer lookup / insert
                                trainer_id = None
                                t_name = r.get("調教師名", "").strip()
                                _reg_raw = r.get("所属", "").strip()
                                t_reg = {'西': '栗東', '東': '美浦'}.get(_reg_raw, _reg_raw) or None
                                if t_name:
                                    cur.execute(
                                        "SELECT trainer_id FROM trainers WHERE trainer_name=%s LIMIT 1",
                                        [t_name]
                                    )
                                    row = cur.fetchone()
                                    trainer_id = row[0] if row else None
                                    if not trainer_id:
                                        cur.execute(
                                            "INSERT INTO trainers (trainer_name, region) VALUES (%s,%s)",
                                            [t_name, t_reg]
                                        )
                                        trainer_id = cur.lastrowid

                                # breeder lookup / insert by name
                                # breeders テーブルの牧場名列名が判明したら下記を修正
                                breeder_id = None
                                b_name = r.get("生産牧場名", "").strip()
                                if b_name:
                                    cur.execute(
                                        "SELECT breeder_id FROM breeders WHERE breeder_name=%s LIMIT 1",
                                        [b_name]
                                    )
                                    row = cur.fetchone()
                                    breeder_id = row[0] if row else None
                                    if not breeder_id:
                                        cur.execute(
                                            "INSERT INTO breeders (breeder_name) VALUES (%s)",
                                            [b_name]
                                        )
                                        breeder_id = cur.lastrowid

                                # dam lookup (self-ref FK, nullable)
                                dam_id = None
                                dam_name = r.get("母名", "").strip()
                                if dam_name:
                                    cur.execute(
                                        "SELECT horse_id FROM horses WHERE horse_name=%s LIMIT 1",
                                        [dam_name]
                                    )
                                    row = cur.fetchone()
                                    if row:
                                        dam_id = row[0]
                                    else:
                                        warnings_list.append(
                                            f"{horse_name}：母「{dam_name}」がDBに未登録（dam_id=NULL）"
                                        )

                                dob = r.get("生年月日", "").strip() or None
                                cur.execute("""
                                    INSERT INTO horses
                                    (horse_name, date_of_birth, gender, color,
                                     sire_id, dam_id,
                                     trainer_id, breeder_id, Owner, bloodline)
                                    VALUES (%s,%s,%s,%s,222,%s,%s,%s,%s,%s)
                                """, [
                                    horse_name, dob,
                                    r.get("性別","").strip() or None,
                                    r.get("毛色","").strip() or None,
                                    dam_id,
                                    trainer_id, breeder_id,
                                    r.get("馬主名","").strip() or None,
                                    r.get("血統","").strip() or None
                                ])
                                ok += 1
                            except Exception as row_err:
                                err += 1
                                errors.append(f"{horse_name}：{row_err}")

                        conn.commit()
                        cur.close(); conn.close()
                        clear_query_cache()
                        if ok:
                            st.success(f"{ok} 頭を登録しました。")
                        if skip:
                            st.info(f"{skip} 頭はすでに登録済みのためスキップしました。")
                        if warnings_list:
                            with st.expander(f"注意事項（{len(warnings_list)} 件）"):
                                for msg in warnings_list:
                                    st.warning(msg)
                        if err:
                            st.error(f"{err} 件でエラーが発生しました。")
                            for msg in errors:
                                st.error(msg)
                except Exception as e:
                    st.error(f"CSV読み込みエラー: {e}")

            st.markdown("---")
            st.markdown("**繁殖馬・種牡馬登録**")
            st.caption("産駒CSVをインポートする前に、母・母父をここで先に登録してください。")

            # リポジトリ内のExcelから直接インポート
            _broodmare_path = Path(__file__).parent / "繁殖牝馬_2025_作業用.xlsx"
            if _broodmare_path.exists():
                st.markdown(f"📄 `繁殖牝馬_2025_作業用.xlsx` がリポジトリ内に見つかりました。")
                if st.button("このファイルからインポート", key="import_from_file_btn"):
                    try:
                        df_file = pd.read_excel(_broodmare_path, dtype=str, header=None)
                        header_idx = next(
                            (i for i, row in df_file.iterrows()
                             if any(str(v) == "馬名" for v in row)), 1
                        )
                        df_file.columns = df_file.iloc[header_idx]
                        df_file = df_file.iloc[header_idx + 1:].reset_index(drop=True)
                        df_file = df_file.fillna("").astype(str).replace("nan", "")

                        conn = get_connection()
                        cur  = conn.cursor()
                        ok = skip = err = 0
                        errors = []

                        for _, r in df_file.iterrows():
                            h_name = str(r.get("馬名", "")).strip()
                            if not h_name or h_name == "nan":
                                continue
                            try:
                                cur.execute(
                                    "SELECT horse_id FROM horses WHERE horse_name=%s LIMIT 1",
                                    [h_name]
                                )
                                if cur.fetchone():
                                    skip += 1
                                    continue

                                b_name = str(r.get("生産牧場名", "")).strip()
                                breeder_id = None
                                if b_name and b_name != "nan":
                                    cur.execute(
                                        "SELECT breeder_id FROM breeders WHERE breeder_name=%s LIMIT 1",
                                        [b_name]
                                    )
                                    row_b = cur.fetchone()
                                    breeder_id = row_b[0] if row_b else None
                                    if not breeder_id:
                                        cur.execute(
                                            "INSERT INTO breeders (breeder_name) VALUES (%s)",
                                            [b_name]
                                        )
                                        breeder_id = cur.lastrowid

                                sire_id = None
                                sire_name = str(r.get("父名", "")).strip()
                                if sire_name and sire_name != "nan":
                                    cur.execute(
                                        "SELECT horse_id FROM horses WHERE horse_name=%s LIMIT 1",
                                        [sire_name]
                                    )
                                    row_s = cur.fetchone()
                                    sire_id = row_s[0] if row_s else None

                                dob = str(r.get("生年月日", "")).strip()
                                dob = None if not dob or dob == "nan" else dob

                                cur.execute("""
                                    INSERT INTO horses
                                    (horse_name, date_of_birth, gender, color, sire_id, breeder_id)
                                    VALUES (%s,%s,%s,%s,%s,%s)
                                """, [
                                    h_name, dob,
                                    str(r.get("性別", "")).strip() or None,
                                    str(r.get("毛色", "")).strip() or None,
                                    sire_id, breeder_id
                                ])
                                ok += 1
                            except Exception as row_err:
                                err += 1
                                errors.append(f"{h_name}：{row_err}")

                        conn.commit()
                        cur.close(); conn.close()
                        clear_query_cache()
                        if ok:    st.success(f"{ok} 頭を登録しました。")
                        if skip:  st.info(f"{skip} 頭はすでに登録済みのためスキップしました。")
                        if err:
                            st.error(f"{err} 件でエラーが発生しました。")
                            for msg in errors: st.error(msg)
                    except Exception as e:
                        st.error(f"インポートに失敗しました: {e}")
            st.markdown("---")

            _pre_cols = ["馬名", "性別", "生年月日", "毛色", "生産牧場名", "父名"]
            _pre_csv  = ",".join(_pre_cols) + "\n" \
                + ",".join(["サンプル母", "牝", "2018-04-10", "鹿毛",
                             "〇〇牧場", "ディープインパクト"]) + "\n"
            st.download_button(
                "繁殖馬CSVテンプレートをダウンロード",
                data=_pre_csv.encode("utf-8-sig"),
                file_name="broodmare_import_template.csv",
                mime="text/csv",
                key="dl_pre_template"
            )
            st.caption("生産牧場名・父名は空欄でも登録できます。")

            uploaded_pre_csv = st.file_uploader(
                "繁殖馬ファイルをアップロード（CSV または Excel）",
                type=["csv", "xlsx"], key="pre_csv_upload"
            )
            if uploaded_pre_csv:
                try:
                    fname = uploaded_pre_csv.name
                    if fname.endswith(".xlsx"):
                        df_pre = pd.read_excel(uploaded_pre_csv, dtype=str, header=None)
                        # 「馬名」が含まれる行をヘッダーとして使用
                        header_idx = next(
                            (i for i, row in df_pre.iterrows()
                             if any(str(v) == "馬名" for v in row)),
                            1
                        )
                        df_pre.columns = df_pre.iloc[header_idx]
                        df_pre = df_pre.iloc[header_idx + 1:].reset_index(drop=True)
                        df_pre = df_pre.fillna("").astype(str).replace("nan", "")
                    else:
                        df_pre = pd.read_csv(uploaded_pre_csv, encoding="utf-8-sig", dtype=str).fillna("")
                    st.write(f"読み込み: **{len(df_pre)} 頭**")
                    st.dataframe(df_pre, hide_index=True)

                    if st.button("上記データを一括登録する", type="primary", key="pre_insert_btn"):
                        conn = get_connection()
                        cur  = conn.cursor()
                        ok = skip = err = 0
                        errors = []

                        for _, r in df_pre.iterrows():
                            h_name = r.get("馬名", "").strip()
                            if not h_name:
                                continue
                            try:
                                # 重複チェック
                                cur.execute(
                                    "SELECT horse_id FROM horses WHERE horse_name=%s LIMIT 1",
                                    [h_name]
                                )
                                if cur.fetchone():
                                    skip += 1
                                    continue

                                # breeder lookup / insert
                                b_name = r.get("生産牧場名", "").strip()
                                breeder_id = None
                                if b_name:
                                    cur.execute(
                                        "SELECT breeder_id FROM breeders WHERE breeder_name=%s LIMIT 1",
                                        [b_name]
                                    )
                                    row = cur.fetchone()
                                    breeder_id = row[0] if row else None
                                    if not breeder_id:
                                        cur.execute(
                                            "INSERT INTO breeders (breeder_name) VALUES (%s)",
                                            [b_name]
                                        )
                                        breeder_id = cur.lastrowid
                                # sire lookup (父、nullableなのでなければNULL)
                                sire_id = None
                                sire_name = r.get("父名", "").strip()
                                if sire_name:
                                    cur.execute(
                                        "SELECT horse_id FROM horses WHERE horse_name=%s LIMIT 1",
                                        [sire_name]
                                    )
                                    row = cur.fetchone()
                                    sire_id = row[0] if row else None

                                dob = r.get("生年月日", "").strip() or None
                                cur.execute("""
                                    INSERT INTO horses
                                    (horse_name, date_of_birth, gender, color,
                                     sire_id, breeder_id)
                                    VALUES (%s,%s,%s,%s,%s,%s)
                                """, [
                                    h_name, dob,
                                    r.get("性別", "").strip() or None,
                                    r.get("毛色", "").strip() or None,
                                    sire_id, breeder_id
                                ])
                                ok += 1
                            except Exception as row_err:
                                err += 1
                                errors.append(f"{h_name}：{row_err}")

                        conn.commit()
                        cur.close(); conn.close()
                        clear_query_cache()
                        if ok:
                            st.success(f"{ok} 頭を登録しました。")
                        if skip:
                            st.info(f"{skip} 頭はすでに登録済みのためスキップしました。")
                        if err:
                            st.error(f"{err} 件でエラーが発生しました。")
                            for msg in errors:
                                st.error(msg)
                except Exception as e:
                    st.error(f"CSV読み込みエラー: {e}")

    tab_search, tab_list, tab_race, tab_analysis, tab_custom, tab_stats, tab_auction = st.tabs([
        "条件検索", "産駒一覧", "レース成績検索", "産駒分析", "カスタム分析", "産駒統計", "セリ結果"
    ])

    # ── TAB: 馬一覧 ────────────────────────────────
    with tab_list:
        st.subheader("産駒一覧")
        st.caption("行を選択すると詳細ページに移動します（表の右上からCSVダウンロード・検索も可能です）")
        sc1,sc2 = st.columns([2,1])
        sort_key   = sc1.selectbox("並び替え", ["生年月日","馬名","出走数","勝利数"], key="sort_key")
        sort_order = sc2.selectbox("順序", ["昇順 ↑","降順 ↓"], key="sort_order")
        sort_asc   = sort_order == "昇順 ↑"

        sql = """
            SELECT h.horse_id, h.horse_name AS 馬名, h.date_of_birth AS 生年月日,
                   YEAR(h.date_of_birth) AS 産年, h.gender AS 性別, h.color AS 毛色,
                   hf.dam_name AS 母名, hf.breeder_name AS 生産牧場,
                   COUNT(re.entry_id) AS 出走数,
                   COALESCE(SUM(CASE WHEN re.final_rank=1 THEN 1 ELSE 0 END),0) AS 勝利数
            FROM horses h
            LEFT JOIN horses_formatted hf ON h.horse_id=hf.horse_id
            LEFT JOIN raceentries re ON h.horse_id=re.horse_id
            LEFT JOIN trainers tr ON h.trainer_id=tr.trainer_id
            LEFT JOIN breeders b ON h.breeder_id=b.breeder_id
            WHERE h.sire_id=222
        """
        params = []
        if horse_name_input:
            sql += " AND h.horse_name LIKE %s"; params.append(f"%{horse_name_input}%")
        if selected_gender != "すべて":
            sql += " AND h.gender=%s"; params.append(selected_gender)
        sql += " AND YEAR(h.date_of_birth) BETWEEN %s AND %s"
        params.extend([birth_year_from, birth_year_to])
        if color_sel:
            sql += f" AND h.color IN ({','.join(['%s']*len(color_sel))})"
            params.extend(color_sel)
        if region_sel:
            region_parts = []
            if '美浦' in region_sel:
                region_parts.append("tr.region = '美浦'")
            if '栗東' in region_sel:
                region_parts.append("tr.region = '栗東'")
            if '地方' in region_sel:
                region_parts.append(
                    "(tr.region IS NOT NULL AND tr.region NOT IN ('美浦', '栗東'))"
                )
            sql += f" AND ({' OR '.join(region_parts)})"
        if location_sel:
            sql += f" AND b.location IN ({','.join(['%s']*len(location_sel))})"
            params.extend(location_sel)
        if bms_sel:
            sql += f" AND hf.broodmare_sire_name IN ({','.join(['%s']*len(bms_sel))})"
            params.extend(bms_sel)
        sql += " GROUP BY h.horse_id,h.horse_name,h.date_of_birth,h.gender,h.color,hf.dam_name,hf.breeder_name"

        try:
            df_horses = run_query(sql, params)
            df_horses['戦績'] = (df_horses['出走数'].astype(int).astype(str) + '戦' +
                                 df_horses['勝利数'].astype(int).astype(str) + '勝')
            df_horses = df_horses.sort_values(by=sort_key, ascending=sort_asc)
            st.write(f"検索結果: **{len(df_horses)}** 頭")

            if not df_horses.empty:
                # 1行ごとにボタンを並べると数百ウィジェットになり描画が重いため、
                # 単一テーブル＋行選択で詳細ページへ遷移する
                df_horses = df_horses.reset_index(drop=True)
                df_view = df_horses[['馬名','生年月日','性別','毛色','母名','生産牧場','戦績']].fillna('―')
                event = st.dataframe(
                    df_view, hide_index=True,
                    on_select="rerun", selection_mode="single-row", key="horse_table"
                )
                selected_rows = event.selection.rows
                if selected_rows:
                    picked = df_horses.iloc[selected_rows[0]]
                    go_detail(int(picked['horse_id']), picked['馬名'])
                    st.rerun()

        except Exception as e:
            st.error(f"エラーが発生しました: {e}")

    # ── TAB: レース成績検索 ────────────────────────
    with tab_race:
        st.subheader("馬名でレース成績を検索")
        sn = st.text_input("馬名を入力（部分一致）", value=horse_name_input, key="tab2_name")
        if st.button("成績を検索", type="primary"):
            if not sn:
                st.warning("馬名を入力してください")
            else:
                try:
                    dfr = run_query("""
                        SELECT h.horse_name AS 馬名, r.race_date AS 開催日, r.race_name AS レース名,
                               t.track_name AS 競馬場, t.course_direction AS コース方向,
                               r.distance_meters AS 距離_m, r.surface_type AS 馬場種別,
                               r.track_condition AS 馬場状態, r.race_class AS クラス,
                               re.final_rank AS 着順,
                               CASE WHEN re.time_seconds IS NULL THEN '―'
                                    ELSE CONCAT(
                                        FLOOR((re.time_seconds + COALESCE(re.time_diff_seconds,0))/60),':',
                                        LPAD(FLOOR((re.time_seconds + COALESCE(re.time_diff_seconds,0))%60),2,'0'),'.',
                                        TRUNCATE(((re.time_seconds + COALESCE(re.time_diff_seconds,0))*10)%10,0))
                               END AS タイム,
                               IFNULL(FORMAT(re.last_3f_seconds,1),'―') AS 上がり3F,
                               re.running_style AS 脚質, re.race_pace AS レースペース,
                               re.Weight AS 斤量, re.harness AS 馬具,
                               j.jockey_name AS 騎手, tr.trainer_name AS 調教師, tr.region AS 調教師所属
                        FROM raceentries re
                        JOIN horses h ON re.horse_id=h.horse_id
                        JOIN races r ON re.race_id=r.race_id
                        JOIN tracks t ON r.track_id=t.track_id
                        LEFT JOIN jockeys j ON re.jockey_id=j.jockey_id
                        LEFT JOIN trainers tr ON re.trainer_id=tr.trainer_id
                        WHERE h.sire_id=222 AND h.horse_name LIKE %s ORDER BY r.race_date DESC
                    """, [f"%{sn}%"])
                    if dfr.empty:
                        st.info("該当する成績が見つかりませんでした。")
                    else:
                        rs = pd.to_numeric(dfr['着順'], errors='coerce'); total = len(dfr)
                        w1=int((rs==1).sum()); w2=int((rs==2).sum())
                        w3=int((rs==3).sum()); w4=int((rs>=4).sum())
                        st.markdown(f"### {total}戦{w1}勝　<span style='color:#555'>({w1}-{w2}-{w3}-{w4})</span>",
                                    unsafe_allow_html=True)
                        m1,m2,m3,m4 = st.columns(4)
                        m1.metric("勝率",   f"{w1/total*100:.1f}%")
                        m2.metric("連対率", f"{(w1+w2)/total*100:.1f}%")
                        m3.metric("複勝率", f"{(w1+w2+w3)/total*100:.1f}%")
                        m4.metric("3着内数",f"{w1+w2+w3}回")
                        st.dataframe(dfr, hide_index=True)
                        st.subheader("競馬場別 出走数")
                        st.bar_chart(dfr['競馬場'].value_counts())
                except Exception as e:
                    st.error(f"エラーが発生しました: {e}")

    # ── TAB: 産駒分析 ──────────────────────────────
    with tab_analysis:
        st.subheader("産駒分析")
        st.caption("分析軸を切り替えて、エフフォーリア産駒の傾向を探りましょう")
        analysis_category = st.radio(
            "開催区分", ["全て", "中央", "地方", "海外"],
            horizontal=True, key="analysis_category"
        )
        try:
            render_overall_summary(category=analysis_category)
        except Exception as e:
            st.warning(f"サマリーの取得に失敗しました: {e}")
        if analysis_category != "全て":
            st.caption(f"※ 登録頭数は全産駒、それ以外の指標は「{analysis_category}」の成績のみで集計しています。")
        st.markdown("---")

        fc1, fc2 = st.columns(2)
        with fc1:
            st.markdown("**開催年**")
            ay1, ay2 = st.columns(2)
            analysis_year_from = ay1.number_input("From", min_value=2020, max_value=2035,
                                                  value=2024, step=1, key="analysis_year_from")
            analysis_year_to   = ay2.number_input("To",   min_value=2020, max_value=2035,
                                                  value=2026, step=1, key="analysis_year_to")
        with fc2:
            st.markdown("**産年（世代）**")
            fy1, fy2 = st.columns(2)
            analysis_foal_from = fy1.number_input("From", min_value=2000, max_value=2040,
                                                  value=2022, step=1, key="analysis_foal_from")
            analysis_foal_to   = fy2.number_input("To",   min_value=2000, max_value=2040,
                                                  value=2024, step=1, key="analysis_foal_to")
        st.markdown("---")

        axis_tabs = st.tabs(["母父別", "生産者別", "騎手別", "馬主別"])
        axes      = ["母父別", "生産者別", "騎手別", "馬主別"]
        axis_desc = {
            "母父別":   "母父（ブルードメアサイアー）ごとの産駒傾向。どの血統との配合が成績に結びつきやすいかを確認できます。",
            "生産者別": "生産牧場ごとの産駒数・成績。どの牧場がエフフォーリア産駒を多く手がけているかを比較できます。",
            "騎手別":   "騎手ごとの騎乗成績。エフフォーリア産駒と相性の良い騎手を探しましょう。",
            "馬主別":   "馬主ごとの出走・勝利実績。各レースエントリー時点の馬主を基に集計します。",
        }
        for axis_tab, axis_key in zip(axis_tabs, axes):
            with axis_tab:
                st.markdown(f"<div class='analysis-card'>{axis_desc[axis_key]}</div>",
                            unsafe_allow_html=True)
                top_n = st.slider("表示件数", min_value=5, max_value=30, value=15, step=5,
                                  key=f"topn_{axis_key}")
                try:
                    render_analysis_section(axis_key, top_n=top_n,
                                            year_from=analysis_year_from,
                                            year_to=analysis_year_to,
                                            foal_year_from=analysis_foal_from,
                                            foal_year_to=analysis_foal_to,
                                            category=analysis_category)
                except Exception as e:
                    st.error(f"分析データの取得に失敗しました: {e}")

    # ── TAB: カスタム分析 ──────────────────────────
    with tab_custom:
        st.subheader("カスタム分析")
        st.caption("X軸・指標・絞り込み条件を自由に組み合わせてグラフと表を生成します")

        ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 2])
        axis_label   = ctrl1.selectbox(
            "X軸（集計軸）",
            ['生産者', '母父', '騎手', '競馬場', '馬場種別', '馬場状態', 'クラス', '距離帯'],
            key="custom_axis"
        )
        metric_label = ctrl2.selectbox(
            "指標",
            ['出走数', '勝利数', '連対数', '3着内数', '勝率(%)', '連対率(%)', '複勝率(%)'],
            key="custom_metric"
        )
        min_runs = ctrl3.number_input("最低出走数", min_value=1, max_value=200, value=5, step=1,
                                      key="custom_min_runs")

        with st.expander("絞り込み条件"):
            f1, f2 = st.columns(2)
            surface_filter   = f1.multiselect("馬場種別", ['芝', 'ダート'], default=[], key="custom_surface")
            condition_filter = f2.multiselect("馬場状態", ['良', '稍重', '重', '不良'], default=[], key="custom_condition")
            d1, d2 = st.columns(2)
            dist_from = d1.number_input("距離 From (m)", min_value=800, max_value=4300, value=800,
                                        step=100, key="custom_dist_from")
            dist_to   = d2.number_input("距離 To (m)",   min_value=800, max_value=4300, value=3600,
                                        step=100, key="custom_dist_to")
            y1, y2 = st.columns(2)
            year_from = y1.number_input("開催年 From", min_value=2024, max_value=2035, value=2024,
                                        step=1, key="custom_year_from")
            year_to   = y2.number_input("開催年 To",   min_value=2024, max_value=2035, value=2026,
                                        step=1, key="custom_year_to")

        AXIS_CONFIG = {
            '生産者': dict(
                select="COALESCE(b.breeder_name, '不明') AS 軸",
                extra_join="LEFT JOIN breeders b ON h.breeder_id = b.breeder_id",
                group="COALESCE(b.breeder_name, '不明')"
            ),
            '母父': dict(
                select="COALESCE(hf.broodmare_sire_name, '不明') AS 軸",
                extra_join="LEFT JOIN horses_formatted hf ON h.horse_id = hf.horse_id",
                group="COALESCE(hf.broodmare_sire_name, '不明')"
            ),
            '騎手': dict(
                select="COALESCE(j.jockey_name, '不明') AS 軸",
                extra_join="LEFT JOIN jockeys j ON re.jockey_id = j.jockey_id",
                group="COALESCE(j.jockey_name, '不明')"
            ),
            '競馬場': dict(select="t.track_name AS 軸",   extra_join="", group="t.track_name"),
            '馬場種別': dict(select="r.surface_type AS 軸",  extra_join="", group="r.surface_type"),
            '馬場状態': dict(select="r.track_condition AS 軸", extra_join="", group="r.track_condition"),
            'クラス':   dict(select="r.race_class AS 軸",    extra_join="", group="r.race_class"),
            '距離帯': dict(
                select="""CASE
                    WHEN r.distance_meters < 1301 THEN '短距離(~1300m)'
                    WHEN r.distance_meters < 1900 THEN 'マイル(1301~1899m)'
                    WHEN r.distance_meters < 2101 THEN '中距離(1900~2100m)'
                    ELSE '長距離(2101m~)' END AS 軸""",
                extra_join="",
                group="""CASE
                    WHEN r.distance_meters < 1301 THEN '短距離(~1300m)'
                    WHEN r.distance_meters < 1900 THEN 'マイル(1301~1899m)'
                    WHEN r.distance_meters < 2101 THEN '中距離(1900~2100m)'
                    ELSE '長距離(2101m~)' END"""
            ),
        }

        axis_cfg = AXIS_CONFIG[axis_label]
        sql_custom = f"""
            SELECT
                {axis_cfg['select']},
                COUNT(re.entry_id)                                   AS 出走数,
                SUM(CASE WHEN re.final_rank=1  THEN 1 ELSE 0 END)   AS 勝利数,
                SUM(CASE WHEN re.final_rank<=2 THEN 1 ELSE 0 END)   AS 連対数,
                SUM(CASE WHEN re.final_rank<=3 THEN 1 ELSE 0 END)   AS 複勝数
            FROM horses h
            JOIN raceentries re ON h.horse_id=re.horse_id
            JOIN races r        ON re.race_id=r.race_id
            JOIN tracks t       ON r.track_id=t.track_id
            {axis_cfg['extra_join']}
            WHERE h.sire_id=222
              AND r.distance_meters BETWEEN %s AND %s
              AND YEAR(r.race_date) BETWEEN %s AND %s
        """
        custom_params = [dist_from, dist_to, year_from, year_to]
        if surface_filter:
            sql_custom += " AND r.surface_type IN ({})".format(','.join(['%s']*len(surface_filter)))
            custom_params.extend(surface_filter)
        if condition_filter:
            sql_custom += " AND r.track_condition IN ({})".format(','.join(['%s']*len(condition_filter)))
            custom_params.extend(condition_filter)
        sql_custom += f" GROUP BY {axis_cfg['group']} HAVING COUNT(re.entry_id) >= %s"
        custom_params.append(min_runs)

        try:
            df_custom = run_query(sql_custom, custom_params)
            if df_custom.empty:
                st.info("条件に合うデータがありません。絞り込み条件を緩めてください。")
            else:
                df_custom['3着内数']   = df_custom['複勝数']
                df_custom['勝率(%)']  = (df_custom['勝利数'] / df_custom['出走数'] * 100).round(1)
                df_custom['連対率(%)'] = (df_custom['連対数'] / df_custom['出走数'] * 100).round(1)
                df_custom['複勝率(%)'] = (df_custom['複勝数'] / df_custom['出走数'] * 100).round(1)
                df_custom = df_custom.sort_values(by=metric_label, ascending=False)

                chart = alt.Chart(df_custom).mark_bar().encode(
                    x=alt.X('軸:N', sort=df_custom['軸'].tolist(), title=axis_label,
                            axis=alt.Axis(labelAngle=-45, labelOverlap=False)),
                    y=alt.Y(f'{metric_label}:Q', title=metric_label),
                    tooltip=['軸', '出走数', '勝率(%)', '連対率(%)', '複勝率(%)']
                ).properties(height=400)
                st.altair_chart(chart, width="stretch")

                st.dataframe(
                    df_custom[['軸','出走数','勝利数','連対数','3着内数','勝率(%)','連対率(%)','複勝率(%)']]
                    .rename(columns={'軸': axis_label}),
                    hide_index=True
                )
        except Exception as e:
            st.error(f"分析に失敗しました: {e}")

    # ── TAB: 条件検索 ──────────────────────────────
    with tab_search:
        st.subheader("条件を指定して戦績を検索")
        st.caption("複数の条件を組み合わせて出走履歴と統計を表示します。条件を指定しない項目は全て対象になります。")

        # URLに条件が載っていれば、ウィジェットを作る前に復元しておく
        if "cs_url_restored" not in st.session_state:
            if restore_search_from_url():
                st.info("URLの検索条件を復元しました。")
            st.session_state.cs_url_restored = True
        st.markdown("---")

        with st.expander("絞り込み条件", expanded=True):
            fc1, fc2, fc3 = st.columns(3)
            surface_sel   = fc1.multiselect("馬場種別", ["芝","ダート"], key="cs_surface")
            condition_sel = fc2.multiselect("馬場状態", ["良","稍重","重","不良"], key="cs_condition")
            gender_sel    = fc3.multiselect("性別", ["牡","牝","騸"], key="cs_gender")

            fd1, fd2, fd3 = st.columns(3)
            cs_dist_from = fd1.number_input("距離 From (m)", min_value=800, max_value=4300,
                                            value=800, step=100, key="cs_dist_from")
            cs_dist_to   = fd2.number_input("距離 To (m)",   min_value=800, max_value=4300,
                                            value=4300, step=100, key="cs_dist_to")
            class_sel    = fd3.multiselect("クラス",
                ["新馬","未勝利","1勝クラス","2勝クラス","3勝クラス","オープン","G3","G2","G1"],
                key="cs_class"
            )

            fy1, fy2, fy3 = st.columns(3)
            cs_year_from = fy1.number_input("開催年 From", min_value=2020, max_value=2030,
                                            value=2024, step=1, key="cs_year_from")
            cs_year_to   = fy2.number_input("開催年 To",   min_value=2020, max_value=2030,
                                            value=2026, step=1, key="cs_year_to")
            style_sel    = fy3.multiselect("脚質", ["逃げ","先行","差し","追込"], key="cs_style")

            try:
                df_tracks_opt = run_query(
                    "SELECT DISTINCT track_name, location FROM tracks ORDER BY track_name"
                )
                df_dir_opt    = run_query(
                    "SELECT DISTINCT course_direction FROM tracks WHERE course_direction IS NOT NULL ORDER BY course_direction"
                )
                track_options = df_tracks_opt['track_name'].tolist()
                dir_options   = df_dir_opt['course_direction'].tolist()
            except Exception:
                df_tracks_opt = pd.DataFrame(columns=['track_name','location'])
                track_options = []
                dir_options   = []

            ft0, ft1, ft2 = st.columns(3)
            category_sel = ft0.radio(
                "開催区分", ["全て", "中央", "地方", "海外"],
                horizontal=True, key="cs_category"
            )
            if category_sel == "全て":
                filtered_tracks = track_options
            else:
                filtered_tracks = df_tracks_opt[
                    df_tracks_opt['location'] == category_sel
                ]['track_name'].tolist()
            # 開催区分の切り替えやURL復元で、選択肢に無い値が残るとエラーになるため取り除く
            for _key, _valid in (("cs_track", filtered_tracks), ("cs_dir", dir_options)):
                _current = st.session_state.get(_key)
                if _current:
                    _kept = [v for v in _current if v in _valid]
                    if _kept != _current:
                        st.session_state[_key] = _kept
            track_sel = ft1.multiselect("競馬場", filtered_tracks, key="cs_track")
            dir_sel   = ft2.multiselect("形態（コース方向）", dir_options, key="cs_dir")

            fj1, fj2 = st.columns(2)
            jockey_input  = fj1.text_input("騎手名（部分一致）", key="cs_jockey")
            trainer_input = fj2.text_input("調教師名（部分一致）", key="cs_trainer")

            fw1, fw2, fw3 = st.columns(3)
            cs_weight_from = fw1.number_input("馬体重 From (kg)", min_value=0, max_value=700,
                                              value=0, step=2, key="cs_weight_from")
            cs_weight_to   = fw2.number_input("馬体重 To (kg)",   min_value=0, max_value=700,
                                              value=700, step=2, key="cs_weight_to")
            cs_weight_filter = fw3.checkbox("馬体重で絞り込む", value=False, key="cs_weight_filter")

        sync_search_to_url()
        share_col, reset_col = st.columns([5, 1])
        share_col.caption(
            "🔗 検索条件はURLに反映されます。ブラウザのアドレスバーをコピーすれば、"
            "同じ条件をそのまま共有・ブックマークできます。"
        )
        reset_col.button("条件をリセット", key="cs_reset", on_click=reset_search_conditions)

        sql_cs = """
            SELECT h.horse_name AS 馬名, h.gender AS 性別,
                   r.race_date AS 開催日, r.race_name AS レース名,
                   t.track_name AS 競馬場, t.course_direction AS コース方向,
                   r.distance_meters AS 距離_m, r.surface_type AS 馬場種別,
                   r.track_condition AS 馬場状態, r.race_class AS クラス,
                   re.final_rank AS 着順,
                   CASE WHEN re.time_seconds IS NULL THEN '―'
                        ELSE CONCAT(
                            FLOOR((re.time_seconds + COALESCE(re.time_diff_seconds,0))/60),':',
                            LPAD(FLOOR((re.time_seconds + COALESCE(re.time_diff_seconds,0))%60),2,'0'),'.',
                            TRUNCATE(((re.time_seconds + COALESCE(re.time_diff_seconds,0))*10)%10,0))
                   END AS タイム,
                   IFNULL(FORMAT(re.last_3f_seconds,1),'―') AS 上がり3F,
                   re.running_style AS 脚質, re.race_pace AS ペース,
                   re.Weight AS 斤量, re.horse_weight AS 馬体重_kg,
                   re.weight_diff AS 体重増減,
                   j.jockey_name AS 騎手, tr.trainer_name AS 調教師
            FROM raceentries re
            JOIN horses  h  ON re.horse_id=h.horse_id
            JOIN races   r  ON re.race_id=r.race_id
            JOIN tracks  t  ON r.track_id=t.track_id
            LEFT JOIN jockeys  j  ON re.jockey_id=j.jockey_id
            LEFT JOIN trainers tr ON re.trainer_id=tr.trainer_id
            WHERE h.sire_id=222
              AND r.distance_meters BETWEEN %s AND %s
              AND YEAR(r.race_date) BETWEEN %s AND %s
        """
        cs_params = [cs_dist_from, cs_dist_to, cs_year_from, cs_year_to]

        if surface_sel:
            sql_cs += f" AND r.surface_type IN ({','.join(['%s']*len(surface_sel))})"
            cs_params.extend(surface_sel)
        if condition_sel:
            sql_cs += f" AND r.track_condition IN ({','.join(['%s']*len(condition_sel))})"
            cs_params.extend(condition_sel)
        if gender_sel:
            sql_cs += f" AND h.gender IN ({','.join(['%s']*len(gender_sel))})"
            cs_params.extend(gender_sel)
        if class_sel:
            sql_cs += f" AND r.race_class IN ({','.join(['%s']*len(class_sel))})"
            cs_params.extend(class_sel)
        if style_sel:
            sql_cs += f" AND re.running_style IN ({','.join(['%s']*len(style_sel))})"
            cs_params.extend(style_sel)
        if track_sel:
            sql_cs += f" AND t.track_name IN ({','.join(['%s']*len(track_sel))})"
            cs_params.extend(track_sel)
        elif category_sel != "全て":
            sql_cs += " AND t.location = %s"
            cs_params.append(category_sel)
        if dir_sel:
            sql_cs += f" AND t.course_direction IN ({','.join(['%s']*len(dir_sel))})"
            cs_params.extend(dir_sel)
        if jockey_input:
            sql_cs += " AND j.jockey_name LIKE %s"; cs_params.append(f"%{jockey_input}%")
        if trainer_input:
            sql_cs += " AND tr.trainer_name LIKE %s"; cs_params.append(f"%{trainer_input}%")
        if cs_weight_filter:
            sql_cs += " AND re.horse_weight BETWEEN %s AND %s"
            cs_params.extend([cs_weight_from, cs_weight_to])
        sql_cs += " ORDER BY r.race_date DESC"

        try:
            df_cs = run_query(sql_cs, cs_params)
            if not df_cs.empty:
                def fmt_cs_weight(row):
                    w = row['馬体重_kg']
                    d = row['体重増減']
                    if pd.isna(w):
                        return '―'
                    w = int(w)
                    if pd.isna(d):
                        return str(w)
                    d = int(d)
                    sign = '+' if d >= 0 else ''
                    return f"{w}({sign}{d})"
                df_cs.insert(
                    df_cs.columns.get_loc('体重増減') + 1,
                    '馬体重',
                    df_cs.apply(fmt_cs_weight, axis=1)
                )
                df_cs = df_cs.drop(columns=['馬体重_kg', '体重増減'])
            if df_cs.empty:
                st.info("条件に合う出走記録が見つかりませんでした。")
            else:
                rank_series = pd.to_numeric(df_cs['着順'], errors='coerce')
                total = len(df_cs)
                w1=int((rank_series==1).sum()); w2=int((rank_series==2).sum())
                w3=int((rank_series==3).sum()); w4=int((rank_series>=4).sum())
                starters = df_cs['馬名'].nunique()
                winners  = df_cs.loc[rank_series==1, '馬名'].nunique()
                st.markdown(
                    f"### {total}戦{w1}勝　"
                    f"<span style='font-size:1.1em; color:#555;'>({w1}-{w2}-{w3}-{w4})</span>",
                    unsafe_allow_html=True
                )
                m1,m2,m3,m4,m5,m6 = st.columns(6)
                m1.metric("出走数",   f"{total}回")
                m2.metric("勝ち上がり率", f"{winners/starters*100:.1f}%" if starters else "―",
                          help=f"この条件で1勝以上した頭数 {winners}頭 ÷ この条件で出走した頭数 {starters}頭")
                m3.metric("勝率",    f"{w1/total*100:.1f}%")
                m4.metric("連対率",  f"{(w1+w2)/total*100:.1f}%")
                m5.metric("複勝率",  f"{(w1+w2+w3)/total*100:.1f}%")
                m6.metric("3着内数", f"{w1+w2+w3}回")
                st.markdown("---")
                st.subheader(f"出走履歴（{total}件）")
                st.dataframe(df_cs, hide_index=True)
        except Exception as e:
            st.error(f"検索に失敗しました: {e}")

    # ── TAB: 産駒統計 ───────────────────────────────
    with tab_stats:
        st.subheader("産駒統計")
        st.caption("産年ごとの母父・生産地・毛色の分布を確認できます。")

        try:
            df_years = run_query("""
                SELECT DISTINCT YEAR(date_of_birth) AS yr
                FROM horses
                WHERE sire_id = 222 AND date_of_birth IS NOT NULL
                ORDER BY yr DESC
            """)
            year_list = [str(int(y)) for y in df_years['yr'].tolist()]
        except Exception:
            year_list = []

        sy_col, _ = st.columns([1, 3])
        stat_year = sy_col.selectbox("産年", ["全て"] + year_list, key="stat_year")

        year_cond  = "" if stat_year == "全て" else "AND YEAR(h.date_of_birth) = %s"
        year_param = [] if stat_year == "全て" else [int(stat_year)]

        st.markdown("---")
        sc1, sc2, sc3 = st.columns(3)

        with sc1:
            st.markdown("**母父**")
            try:
                df_bms = run_query(f"""
                    SELECT
                        COALESCE(bms.horse_name, '不明') AS 母父,
                        COUNT(*) AS 頭数,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS 割合
                    FROM horses h
                    LEFT JOIN horses dam ON h.dam_id  = dam.horse_id
                    LEFT JOIN horses bms ON dam.sire_id = bms.horse_id
                    WHERE h.sire_id = 222
                      AND h.date_of_birth IS NOT NULL
                      {year_cond}
                    GROUP BY bms.horse_name
                    ORDER BY 頭数 DESC
                """, year_param)
                st.dataframe(df_bms, hide_index=True)
            except Exception as e:
                st.error(f"母父データの取得に失敗: {e}")

        with sc2:
            st.markdown("**生産地**")
            try:
                df_loc = run_query(f"""
                    SELECT
                        COALESCE(b.location, '不明') AS 生産地,
                        COUNT(*) AS 頭数,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS 割合
                    FROM horses h
                    LEFT JOIN breeders b ON h.breeder_id = b.breeder_id
                    WHERE h.sire_id = 222
                      AND h.date_of_birth IS NOT NULL
                      {year_cond}
                    GROUP BY b.location
                    ORDER BY 頭数 DESC
                """, year_param)
                st.dataframe(df_loc, hide_index=True)
            except Exception as e:
                st.error(f"生産地データの取得に失敗: {e}")

        with sc3:
            st.markdown("**毛色**")
            try:
                df_color = run_query(f"""
                    SELECT
                        COALESCE(h.color, '不明') AS 毛色,
                        COUNT(*) AS 頭数,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS 割合
                    FROM horses h
                    WHERE h.sire_id = 222
                      AND h.date_of_birth IS NOT NULL
                      {year_cond}
                    GROUP BY h.color
                    ORDER BY 頭数 DESC
                """, year_param)
                st.dataframe(df_color, hide_index=True)
            except Exception as e:
                st.error(f"毛色データの取得に失敗: {e}")

    # ── TAB: セリ結果 ───────────────────────────────
    with tab_auction:
        st.subheader("セリ結果")
        st.caption("※ 価格は税抜き（万円）")

        EXCEL_PATH = Path(__file__).parent / "エフフォーリア産駒 セリ結果.xlsx"

        def load_seri_data(path):
            wb = openpyxl.load_workbook(path)
            ws = wb.active
            grid = [[cell.value for cell in row] for row in ws.iter_rows()]
            num_cols = len(grid[0]) if grid else 0

            records = []
            for base in range(1, num_cols, 3):
                price_col = base + 1
                current_sale = None

                for row in grid:
                    if base >= len(row):
                        continue
                    cell = str(row[base]).strip() if row[base] is not None else ""

                    if re.search(r'セール\d{4}', cell):
                        current_sale = cell
                        continue

                    if not cell or cell in ("馬名", "落札額(万円)", "平均", "※価格は税抜き"):
                        continue
                    if current_sale is None:
                        continue

                    price_raw = row[price_col] if price_col < len(row) else None
                    if isinstance(price_raw, (int, float)):
                        price = int(price_raw)
                        display = f"{price:,}"
                    elif price_raw in ("主取り", "欠場"):
                        price = None
                        display = str(price_raw)
                    else:
                        price = None
                        display = "―"

                    year_match = re.search(r'\d{4}', current_sale)
                    sale_year = int(year_match.group()) if year_match else None

                    records.append({
                        "セール名":     current_sale,
                        "セール年":     sale_year,
                        "馬名":         cell,
                        "落札額(万円)":  price,
                        "表示額":       display,
                    })

            return pd.DataFrame(records)

        try:
            df_seri = load_seri_data(EXCEL_PATH)

            years = sorted(df_seri["セール年"].dropna().unique().astype(int))
            sel_year = st.selectbox("セール年度", years, index=len(years) - 1, key="seri_year")

            df_year = df_seri[df_seri["セール年"] == sel_year]

            for sale_name in df_year["セール名"].unique():
                df_sale = df_year[df_year["セール名"] == sale_name].copy()
                prices = df_sale["落札額(万円)"].dropna()

                n    = len(df_sale)
                avg  = f"{int(prices.mean()):,}" if len(prices) > 0 else "―"
                high = f"{int(prices.max()):,}"  if len(prices) > 0 else "―"

                st.markdown(
                    f"**{sale_name}**　{n}頭　"
                    f"平均 **{avg}** 万円　最高 **{high}** 万円"
                )

                disp = df_sale[["馬名", "表示額"]].rename(columns={"表示額": "落札額(万円)"})
                st.dataframe(disp, hide_index=True)
                st.markdown("---")

        except Exception as e:
            st.error(f"セリ結果の読み込みに失敗しました: {e}")
