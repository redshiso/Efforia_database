import re
import base64
import streamlit as st
import mysql.connector
import pandas as pd
import altair as alt

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

def run_query(sql, params=None):
    conn = get_connection()
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

def run_write(sql, params=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params or [])
    conn.commit(); cursor.close(); conn.close()

# ─────────────────────────────────────────
# 訪問カウント（セッション開始時に1回だけDB更新）
# ─────────────────────────────────────────
def get_views():
    df = run_query("SELECT views FROM page_views WHERE id=1")
    return int(df.iloc[0]['views']) if not df.empty else 0

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
def render_overall_summary():
    df = run_query("""
        SELECT
            COUNT(DISTINCT h.horse_id)                                            AS 登録頭数,
            COUNT(re.entry_id)                                                    AS 総出走数,
            COALESCE(SUM(CASE WHEN re.final_rank=1  THEN 1 ELSE 0 END),0)         AS 総勝利数,
            COALESCE(SUM(CASE WHEN re.final_rank<=3 THEN 1 ELSE 0 END),0)         AS 総複勝数,
            COUNT(DISTINCT CASE WHEN re.entry_id IS NOT NULL THEN h.horse_id END) AS 出走経験頭数
        FROM horses h
        LEFT JOIN raceentries re ON h.horse_id=re.horse_id
        WHERE h.sire_id=222
    """)
    s = df.iloc[0]
    total_starts = int(s['総出走数'])
    wins         = int(s['総勝利数'])
    placed       = int(s['総複勝数'])
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("登録頭数",     f"{int(s['登録頭数'])}頭")
    c2.metric("出走経験頭数", f"{int(s['出走経験頭数'])}頭")
    c3.metric("総出走数",     f"{total_starts}回")
    c4.metric("勝率",         f"{wins/total_starts*100:.1f}%" if total_starts else "―")
    c5.metric("複勝率",       f"{placed/total_starts*100:.1f}%" if total_starts else "―")

# ─────────────────────────────────────────
# 産駒分析グラフ＋考察＋画像
# ─────────────────────────────────────────
def render_analysis_section(axis_key, top_n=15):
    axis_map = {
        '母父別':   ('hf.broodmare_sire_name', '母父'),
        '生産者別': ('hf.breeder_name',         '生産者'),
        '騎手別':   ('j.jockey_name',            '騎手'),
        '馬主別':   ('re.owner',                 '馬主'),
    }
    col_expr, col_alias = axis_map[axis_key]
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
        LEFT JOIN jockeys  j           ON re.jockey_id=j.jockey_id
        LEFT JOIN trainers tr          ON re.trainer_id=tr.trainer_id
        WHERE h.sire_id=222 AND {col_expr} IS NOT NULL
        GROUP BY {col_expr}
        HAVING 出走数 > 0
        ORDER BY 出走数 DESC
        LIMIT {top_n}
    """
    df = run_query(sql)
    if df.empty:
        st.info("データがありません。"); return

    df['勝率(%)']  = (df['勝利数'] / df['出走数'] * 100).round(1)
    df['複勝率(%)'] = (df['複勝数'] / df['出走数'] * 100).round(1)
    sort_order = df[col_alias].tolist()

    metric = st.radio("表示指標", ["出走数","勝利数","勝率(%)","複勝率(%)"],
                      horizontal=True, key=f"metric_{axis_key}")
    chart = alt.Chart(df).mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
        x=alt.X(f'{col_alias}:N', sort=sort_order, title=None,
                axis=alt.Axis(labelAngle=-40, labelOverlap=False)),
        y=alt.Y(f'{metric}:Q', title=metric),
        color=alt.Color(f'{metric}:Q', scale=alt.Scale(scheme='blues'), legend=None),
        tooltip=[col_alias,'頭数','出走数','勝利数','勝率(%)','複勝率(%)']
    ).properties(height=360)
    st.altair_chart(chart, use_container_width=True)

    with st.expander("詳細テーブル"):
        st.dataframe(df[[col_alias,'頭数','出走数','勝利数','複勝数','勝率(%)','複勝率(%)']],
                     use_container_width=True, hide_index=True)

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
                    use_container_width=True
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
                    st.image(img_bytes, caption=img['caption'] or None, use_container_width=True)
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

# セッション開始時に1回だけカウントアップ
if 'visited' not in st.session_state:
    run_write("UPDATE page_views SET views=views+1 WHERE id=1")
    st.session_state.visited = True

def go_detail(horse_id, horse_name):
    st.session_state.selected_horse_id   = horse_id
    st.session_state.selected_horse_name = horse_name
    st.session_state.page = 'detail'

def go_list():
    st.session_state.page = 'list'

def go_article(article_id):
    st.session_state.selected_article_id = article_id
    st.session_state.page = 'article'

def go_list_article_tab():
    st.session_state.page = 'list'
    st.session_state.selected_article_id = None

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
                "SELECT image_data, mime_type FROM horse_images WHERE horse_id=%s", [horse_id]
            )
            col_photo, col_info, col_blood = st.columns([2, 3, 3])

            with col_photo:
                st.subheader("写真")
                if not df_horse_img.empty:
                    img_bytes = base64.b64decode(df_horse_img.iloc[0]['image_data'])
                    st.image(img_bytes, use_container_width=True)
                    if st.session_state.is_admin:
                        if st.button("写真を削除", key="del_horse_img"):
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
                        if uploaded_horse:
                            img_b64 = base64.b64encode(uploaded_horse.read()).decode('utf-8')
                            run_write(
                                "INSERT INTO horse_images (horse_id, image_data, mime_type) VALUES (%s,%s,%s)",
                                [horse_id, img_b64, uploaded_horse.type]
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
                   re.final_rank AS 着順, re.time_seconds AS タイム_秒,
                   re.running_style AS 脚質, re.race_pace AS ペース,
                   re.Weight AS 斤量, j.jockey_name AS 騎手, tr.trainer_name AS 調教師
            FROM raceentries re
            JOIN races r ON re.race_id=r.race_id JOIN tracks t ON r.track_id=t.track_id
            LEFT JOIN jockeys j ON re.jockey_id=j.jockey_id
            LEFT JOIN trainers tr ON re.trainer_id=tr.trainer_id
            WHERE re.horse_id=%s ORDER BY r.race_date DESC
        """, [horse_id])
        if df_entries.empty: st.info("出走履歴がありません。")
        else: st.dataframe(df_entries, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"出走履歴の取得に失敗しました: {e}")

# ══════════════════════════════════════════
# 一覧ページ
# ══════════════════════════════════════════
else:
    st.title("エフフォーリア産駒データベース")

    # サイドバー
    st.sidebar.markdown("---")
    total_views = get_views()
    st.sidebar.caption(f"あなたは: {total_views} 人目の武史です。")
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

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "産駒一覧", "レース成績検索", "産駒分析", "カスタム分析", "条件検索", "記事・コラム"
    ])

    # ── TAB 1: 馬一覧 ──────────────────────────────
    with tab1:
        st.subheader("産駒一覧")
        st.caption("馬名をクリックすると詳細ページに移動します")
        sc1,sc2 = st.columns([2,1])
        sort_key   = sc1.selectbox("並び替え", ["生年月日","馬名"], key="sort_key")
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
                h0,h1,h2,h3,h4,h5 = st.columns([3,2,1,1,2,1])
                h0.markdown("**馬名**"); h1.markdown("**生年月日**"); h2.markdown("**性別**")
                h3.markdown("**毛色**"); h4.markdown("**母名**"); h5.markdown("**戦績**")
                st.markdown("---")

                for _, row in df_horses.iterrows():
                    c0,c1,c2,c3,c4,c5 = st.columns([3,2,1,1,2,1])
                    c0.button(row['馬名'], key=f"btn_{row['horse_id']}",
                              on_click=go_detail, args=(row['horse_id'], row['馬名']))
                    c1.write(str(row['生年月日'])); c2.write(row['性別'])
                    c3.write(row['毛色'] or '―'); c4.write(row['母名'] or '―'); c5.write(row['戦績'])

        except Exception as e:
            st.error(f"エラーが発生しました: {e}")

    # ── TAB 2: レース成績検索 ──────────────────────
    with tab2:
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
                               re.final_rank AS 着順, re.time_seconds AS タイム_秒,
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
                        st.dataframe(dfr, use_container_width=True, hide_index=True)
                        st.subheader("競馬場別 出走数")
                        st.bar_chart(dfr['競馬場'].value_counts())
                except Exception as e:
                    st.error(f"エラーが発生しました: {e}")

    # ── TAB 3: 産駒分析 ────────────────────────────
    with tab3:
        st.subheader("産駒分析")
        st.caption("分析軸を切り替えて、エフフォーリア産駒の傾向を探りましょう")
        try:
            render_overall_summary()
        except Exception as e:
            st.warning(f"サマリーの取得に失敗しました: {e}")
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
                    render_analysis_section(axis_key, top_n=top_n)
                except Exception as e:
                    st.error(f"分析データの取得に失敗しました: {e}")

    # ── TAB 4: カスタム分析 ────────────────────────
    with tab4:
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
                st.altair_chart(chart, use_container_width=True)

                st.dataframe(
                    df_custom[['軸','出走数','勝利数','連対数','3着内数','勝率(%)','連対率(%)','複勝率(%)']]
                    .rename(columns={'軸': axis_label}),
                    use_container_width=True, hide_index=True
                )
        except Exception as e:
            st.error(f"分析に失敗しました: {e}")

    # ── TAB 5: 条件検索 ────────────────────────────
    with tab5:
        st.subheader("条件を指定して戦績を検索")
        st.caption("複数の条件を組み合わせて出走履歴と統計を表示します。条件を指定しない項目は全て対象になります。")
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
                df_tracks_opt = run_query("SELECT DISTINCT track_name FROM tracks ORDER BY track_name")
                df_dir_opt    = run_query(
                    "SELECT DISTINCT course_direction FROM tracks WHERE course_direction IS NOT NULL ORDER BY course_direction"
                )
                track_options = df_tracks_opt['track_name'].tolist()
                dir_options   = df_dir_opt['course_direction'].tolist()
            except Exception:
                track_options = []
                dir_options   = []

            ft1, ft2 = st.columns(2)
            track_sel = ft1.multiselect("競馬場", track_options, key="cs_track")
            dir_sel   = ft2.multiselect("形態（コース方向）", dir_options, key="cs_dir")

            fj1, fj2 = st.columns(2)
            jockey_input  = fj1.text_input("騎手名（部分一致）", key="cs_jockey")
            trainer_input = fj2.text_input("調教師名（部分一致）", key="cs_trainer")

        sql_cs = """
            SELECT h.horse_name AS 馬名, h.gender AS 性別,
                   r.race_date AS 開催日, r.race_name AS レース名,
                   t.track_name AS 競馬場, t.course_direction AS コース方向,
                   r.distance_meters AS 距離_m, r.surface_type AS 馬場種別,
                   r.track_condition AS 馬場状態, r.race_class AS クラス,
                   re.final_rank AS 着順, re.time_seconds AS タイム_秒,
                   re.running_style AS 脚質, re.race_pace AS ペース,
                   re.Weight AS 斤量, j.jockey_name AS 騎手, tr.trainer_name AS 調教師
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
        if dir_sel:
            sql_cs += f" AND t.course_direction IN ({','.join(['%s']*len(dir_sel))})"
            cs_params.extend(dir_sel)
        if jockey_input:
            sql_cs += " AND j.jockey_name LIKE %s"; cs_params.append(f"%{jockey_input}%")
        if trainer_input:
            sql_cs += " AND tr.trainer_name LIKE %s"; cs_params.append(f"%{trainer_input}%")
        sql_cs += " ORDER BY r.race_date DESC"

        try:
            df_cs = run_query(sql_cs, cs_params)
            if df_cs.empty:
                st.info("条件に合う出走記録が見つかりませんでした。")
            else:
                rank_series = pd.to_numeric(df_cs['着順'], errors='coerce')
                total = len(df_cs)
                w1=int((rank_series==1).sum()); w2=int((rank_series==2).sum())
                w3=int((rank_series==3).sum()); w4=int((rank_series>=4).sum())
                st.markdown(
                    f"### {total}戦{w1}勝　"
                    f"<span style='font-size:1.1em; color:#555;'>({w1}-{w2}-{w3}-{w4})</span>",
                    unsafe_allow_html=True
                )
                m1,m2,m3,m4,m5 = st.columns(5)
                m1.metric("出走数",   f"{total}回")
                m2.metric("勝率",    f"{w1/total*100:.1f}%")
                m3.metric("連対率",  f"{(w1+w2)/total*100:.1f}%")
                m4.metric("複勝率",  f"{(w1+w2+w3)/total*100:.1f}%")
                m5.metric("3着内数", f"{w1+w2+w3}回")
                st.markdown("---")
                st.subheader(f"出走履歴（{total}件）")
                st.dataframe(df_cs, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"検索に失敗しました: {e}")

    # ── TAB 6: 記事・コラム ─────────────────────────
    with tab6:
        st.subheader("エフフォーリア産駒に関する考察・コラム")
        if st.session_state.is_admin:
            st.caption("`{{image:ラベル}}` で画像挿入 / `{{graph:母父別}}` などでグラフ挿入")
        st.markdown("---")
        try:
            dfa = run_query("""
                SELECT article_id, title,
                       DATE_FORMAT(created_at,'%Y-%m-%d %H:%i') AS post_date
                FROM articles ORDER BY created_at DESC
            """)
            if dfa.empty:
                st.info("現在、掲載されている記事はありません。")
            else:
                for _, row in dfa.iterrows():
                    aid = row['article_id']
                    ct, cd, cb = st.columns([5, 2, 1])
                    ct.button(row['title'], key=f"article_btn_{aid}",
                              on_click=go_article, args=(aid,), use_container_width=True)
                    cd.markdown(
                        f"<div style='padding-top:8px;color:#888;font-size:0.85em'>{row['post_date']}</div>",
                        unsafe_allow_html=True
                    )
                    if st.session_state.is_admin:
                        if cb.button("削除", key=f"del_{aid}"):
                            run_write("DELETE FROM article_images WHERE article_id=%s", [aid])
                            run_write("DELETE FROM articles WHERE article_id=%s", [aid])
                            st.rerun()
                    st.markdown("---")
        except Exception as e:
            st.error(f"記事の読み込みに失敗しました: {e}")
