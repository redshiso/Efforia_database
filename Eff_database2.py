import streamlit as st
import mysql.connector
import pandas as pd

st.set_page_config(page_title="エフフォーリア産駒データベース", layout="wide")

# ボタンの馬名が折り返されないようにするCSS
st.markdown("""
<style>
div[data-testid="column"]:first-child button {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    width: 100%;
    min-width: 120px;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# DB接続設定
# ─────────────────────────────────────────
db_config = {
    'user': st.secrets["DB_USER"],
    'password': st.secrets["DB_PASSWORD"],
    'host': st.secrets["DB_HOST"],
    'port': st.secrets["DB_PORT"],
    'database': st.secrets["DB_NAME"],
    'ssl_disabled': False,
    'ssl_verify_cert': False,
    'ssl_verify_identity': False
}

def get_connection():
    return mysql.connector.connect(**db_config)

def run_query(sql, params=None):
    conn = get_connection()
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

# ─────────────────────────────────────────
# アクセスカウンター関数
# ─────────────────────────────────────────
def update_and_get_views():
    conn = get_connection()
    cursor = conn.cursor()
    if 'visited' not in st.session_state:
        cursor.execute("UPDATE page_views SET views = views + 1 WHERE id = 1")
        conn.commit()
        st.session_state.visited = True
    cursor.execute("SELECT views FROM page_views WHERE id = 1")
    views = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return views

# ─────────────────────────────────────────
# session_state 初期化（ページ管理）
# ─────────────────────────────────────────
if 'page' not in st.session_state:
    st.session_state.page = 'list'
if 'selected_horse_id' not in st.session_state:
    st.session_state.selected_horse_id = None
if 'selected_horse_name' not in st.session_state:
    st.session_state.selected_horse_name = ""
if 'selected_article_id' not in st.session_state:
    st.session_state.selected_article_id = None
if 'edit_article_id' not in st.session_state:
    st.session_state.edit_article_id = None
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False

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

    sql_article = """
        SELECT
            article_id,
            title,
            content,
            DATE_FORMAT(created_at, '%Y-%m-%d %H:%i') AS post_date
        FROM articles
        WHERE article_id = %s
    """
    try:
        df_article = run_query(sql_article, [article_id])
        if df_article.empty:
            st.warning("記事が見つかりませんでした。")
        else:
            row = df_article.iloc[0]
            aid = row['article_id']

            # 編集モード
            if st.session_state.is_admin and st.session_state.edit_article_id == aid:
                st.markdown(f"**「{row['title']}」を編集中...**")
                with st.form(key=f"edit_form_{aid}"):
                    edit_title   = st.text_input("タイトル", value=row['title'])
                    edit_content = st.text_area("本文", value=row['content'], height=300)
                    col_submit, col_cancel = st.columns([1, 1])
                    if col_submit.form_submit_button("更新する"):
                        conn = get_connection()
                        cursor = conn.cursor()
                        cursor.execute(
                            "UPDATE articles SET title = %s, content = %s WHERE article_id = %s",
                            (edit_title, edit_content, aid)
                        )
                        conn.commit()
                        cursor.close()
                        conn.close()
                        st.session_state.edit_article_id = None
                        st.rerun()
                    if col_cancel.form_submit_button("キャンセル"):
                        st.session_state.edit_article_id = None
                        st.rerun()
            # 通常表示
            else:
                st.title(row['title'])
                st.caption(f"公開日時: {row['post_date']}")
                st.markdown("---")
                st.markdown(row['content'])

                # 管理者ボタン
                if st.session_state.is_admin:
                    st.markdown("---")
                    col1, col2, _ = st.columns([1, 1, 8])
                    if col1.button("✏️ 編集"):
                        st.session_state.edit_article_id = aid
                        st.rerun()
                    if col2.button("🗑️ 削除"):
                        conn = get_connection()
                        cursor = conn.cursor()
                        cursor.execute("DELETE FROM articles WHERE article_id = %s", (aid,))
                        conn.commit()
                        cursor.close()
                        conn.close()
                        go_list_article_tab()
                        st.rerun()
    except Exception as e:
        st.error(f"記事の読み込みに失敗しました: {e}")

# ══════════════════════════════════════════
# 馬詳細ページ
# ══════════════════════════════════════════
elif st.session_state.page == 'detail':
    horse_id   = st.session_state.selected_horse_id
    horse_name = st.session_state.selected_horse_name

    st.button("← 一覧に戻る", on_click=go_list)
    st.title(f"🐴 {horse_name}")
    st.markdown("---")

    sql_profile = """
        SELECT
            hf.date_of_birth       AS 生年月日,
            hf.gender              AS 性別,
            hf.color               AS 毛色,
            hf.bloodline           AS 血統,
            h.Owner                AS 馬主,
            hf.breeder_name        AS 生産者,
            hf.sire_name           AS 父,
            hf.dam_name            AS 母,
            hf.broodmare_sire_name AS 母父,
            hf.trainer_name        AS 調教師
        FROM horses_formatted hf
        JOIN horses h ON hf.horse_id = h.horse_id
        WHERE hf.horse_id = %s
    """
    try:
        df_profile = run_query(sql_profile, [horse_id])
        if not df_profile.empty:
            p = df_profile.iloc[0]
            col_info, col_blood = st.columns(2)
            with col_info:
                st.subheader("基本情報")
                st.markdown(f"""
| 項目 | 内容 |
|------|------|
| 生年月日 | {p['生年月日']} |
| 性別 | {p['性別']} |
| 毛色 | {p['毛色'] or '―'} |
| 馬主 | {p['馬主'] or '―'} |
| 生産者 | {p['生産者'] or '―'} |
| 調教師 | {p['調教師'] or '―'} |
""")
            with col_blood:
                st.subheader("血統")
                st.markdown(f"""
| 項目 | 内容 |
|------|------|
| 父 | {p['父'] or '―'} |
| 母 | {p['母'] or '―'} |
| 母父 | {p['母父'] or '―'} |
""")
    except Exception as e:
        st.error(f"基本情報の取得に失敗しました: {e}")

    st.markdown("---")

    st.subheader("通算成績")
    sql_summary = """
        SELECT
            COUNT(*)                                                          AS 出走数,
            COALESCE(SUM(CASE WHEN re.final_rank = 1  THEN 1 ELSE 0 END), 0) AS 一着,
            COALESCE(SUM(CASE WHEN re.final_rank = 2  THEN 1 ELSE 0 END), 0) AS 二着,
            COALESCE(SUM(CASE WHEN re.final_rank = 3  THEN 1 ELSE 0 END), 0) AS 三着,
            COALESCE(SUM(CASE WHEN re.final_rank >= 4 THEN 1 ELSE 0 END), 0) AS 四着以下
        FROM raceentries re
        WHERE re.horse_id = %s
    """
    try:
        df_sum = run_query(sql_summary, [horse_id])
        if not df_sum.empty:
            s     = df_sum.iloc[0]
            total = int(s['出走数'])
            w1    = int(s['一着'])
            w2    = int(s['二着'])
            w3    = int(s['三着'])
            w4    = int(s['四着以下'])
            if total == 0:
                st.markdown("### 0戦0勝　<span style='font-size:1.2em; color:#555;'>(未出走)</span>", unsafe_allow_html=True)
            else:
                st.markdown(
                    f"### {total}戦{w1}勝　"
                    f"<span style='font-size:1.2em; color:#555;'>({w1}-{w2}-{w3}-{w4})</span>",
                    unsafe_allow_html=True
                )
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("勝率",    f"{w1/total*100:.1f}%")
                m2.metric("連対率",  f"{(w1+w2)/total*100:.1f}%")
                m3.metric("複勝率",  f"{(w1+w2+w3)/total*100:.1f}%")
                m4.metric("3着内数", f"{w1+w2+w3}回")
    except Exception as e:
        st.error(f"通算成績の取得に失敗しました: {e}")

    st.markdown("---")

    st.subheader("出走履歴")
    sql_entries = """
        SELECT
            r.race_date        AS 開催日,
            r.race_name        AS レース名,
            t.track_name       AS 競馬場,
            r.distance_meters  AS 距離_m,
            r.surface_type     AS 馬場種別,
            r.track_condition  AS 馬場状態,
            r.race_class       AS クラス,
            re.final_rank      AS 着順,
            re.time_seconds    AS タイム_秒,
            re.running_style   AS 脚質,
            re.race_pace       AS ペース,
            re.Weight          AS 斤量,
            j.jockey_name      AS 騎手,
            tr.trainer_name    AS 調教師
        FROM raceentries re
        JOIN races   r  ON re.race_id    = r.race_id
        JOIN tracks  t  ON r.track_id    = t.track_id
        LEFT JOIN jockeys  j  ON re.jockey_id  = j.jockey_id
        LEFT JOIN trainers tr ON re.trainer_id = tr.trainer_id
        WHERE re.horse_id = %s
        ORDER BY r.race_date DESC
    """
    try:
        df_entries = run_query(sql_entries, [horse_id])
        if df_entries.empty:
            st.info("出走履歴がありません。")
        else:
            st.dataframe(df_entries, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"出走履歴の取得に失敗しました: {e}")

# ══════════════════════════════════════════
# 一覧ページ
# ══════════════════════════════════════════
else:
    st.title("エフフォーリア産駒データベース")

    # ── サイドバー ──────────────────────────────────
    st.sidebar.markdown("---")
    total_views = update_and_get_views()
    st.sidebar.caption(f"あなたは: {total_views} 人目の訪問者です。")

    st.sidebar.header("検索条件")
    horse_name_input = st.sidebar.text_input("馬名（一部でも可）", value="")
    selected_gender  = st.sidebar.radio("性別", ["すべて", "牡", "牝", "騸"])
    st.sidebar.markdown("**産年（生年）**")
    year_col1, year_col2 = st.sidebar.columns(2)
    birth_year_from = year_col1.number_input("From", min_value=2000, max_value=2040, value=2022, step=1)
    birth_year_to   = year_col2.number_input("To",   min_value=2000, max_value=2040, value=2024, step=1)
    st.sidebar.markdown("---")
    st.sidebar.caption("※ 馬名は部分一致で検索します")

    # ── 管理者メニュー ──────────────────────────────
    st.sidebar.markdown("---")
    with st.sidebar.expander("🛠 管理者メニュー"):
        if not st.session_state.is_admin:
            admin_password  = st.text_input("管理者パスワード", type="password", key="admin_pass")
            SECRET_PASSWORD = st.secrets["ADMIN_PASSWORD"]
            if admin_password == SECRET_PASSWORD:
                st.session_state.is_admin = True
                st.rerun()
            elif admin_password != "":
                st.error("パスワードが間違っています。")
        else:
            st.success("管理者としてログイン中")
            if st.button("ログアウト", key="logout_btn"):
                st.session_state.is_admin = False
                st.rerun()
            st.markdown("---")
            st.markdown("**新規記事の投稿**")
            with st.form(key='post_article_form', clear_on_submit=True):
                article_title   = st.text_input("記事のタイトル")
                article_content = st.text_area("本文（Markdown対応）", height=200)
                if st.form_submit_button("記事を公開する"):
                    if article_title and article_content:
                        try:
                            conn = get_connection()
                            cursor = conn.cursor()
                            cursor.execute(
                                "INSERT INTO articles (title, content) VALUES (%s, %s)",
                                (article_title, article_content)
                            )
                            conn.commit()
                            cursor.close()
                            conn.close()
                            st.success("記事を公開しました！")
                            st.rerun()
                        except Exception as e:
                            st.error(f"投稿に失敗しました: {e}")
                    else:
                        st.warning("タイトルと本文を入力してください。")

    tab1, tab2, tab3 = st.tabs(["産駒一覧", "レース成績検索", "記事・コラム"])

    # ── TAB 1: 馬一覧 ──────────────────────────────
    with tab1:
        st.subheader("産駒一覧")
        st.caption("馬名をクリックすると詳細ページに移動します")

        sort_col1, sort_col2 = st.columns([2, 1])
        sort_key   = sort_col1.selectbox("並び替え", ["生年月日", "馬名"], key="sort_key")
        sort_order = sort_col2.selectbox("順序", ["昇順 ↑", "降順 ↓"], key="sort_order")
        sort_asc   = sort_order == "昇順 ↑"

        sql = """
            SELECT
                h.horse_id,
                h.horse_name          AS 馬名,
                h.date_of_birth       AS 生年月日,
                YEAR(h.date_of_birth) AS 産年,
                h.gender              AS 性別,
                h.color               AS 毛色,
                hf.dam_name           AS 母名,
                hf.breeder_name       AS 生産牧場,
                COUNT(re.entry_id)    AS 出走数,
                COALESCE(SUM(CASE WHEN re.final_rank = 1 THEN 1 ELSE 0 END), 0) AS 勝利数
            FROM horses h
            LEFT JOIN horses_formatted hf ON h.horse_id = hf.horse_id
            LEFT JOIN raceentries re ON h.horse_id = re.horse_id
            WHERE h.sire_id = 222
        """
        params = []
        if horse_name_input:
            sql += " AND h.horse_name LIKE %s"
            params.append(f"%{horse_name_input}%")
        if selected_gender != "すべて":
            sql += " AND h.gender = %s"
            params.append(selected_gender)
        sql += " AND YEAR(h.date_of_birth) BETWEEN %s AND %s"
        params.extend([birth_year_from, birth_year_to])
        sql += " GROUP BY h.horse_id, h.horse_name, h.date_of_birth, h.gender, h.color, hf.dam_name, hf.breeder_name"

        try:
            df_horses = run_query(sql, params)
            df_horses['戦績'] = df_horses['出走数'].astype(int).astype(str) + '戦' + df_horses['勝利数'].astype(int).astype(str) + '勝'
            df_horses = df_horses.sort_values(by=sort_key, ascending=sort_asc)

            st.write(f"検索結果: **{len(df_horses)}** 頭")

            if not df_horses.empty:
                h0, h1, h2, h3, h4, h5 = st.columns([2, 2, 1, 1, 2, 1.5])
                h0.markdown("**馬名**")
                h1.markdown("**生年月日**")
                h2.markdown("**性別**")
                h3.markdown("**毛色**")
                h4.markdown("**母名**")
                h5.markdown("**戦績**")
                st.markdown("---")

                for _, row in df_horses.iterrows():
                    c0, c1, c2, c3, c4, c5 = st.columns([2, 2, 1, 1, 2, 1.5])
                    c0.button(
                        row['馬名'],
                        key=f"btn_{row['horse_id']}",
                        on_click=go_detail,
                        args=(row['horse_id'], row['馬名'])
                    )
                    c1.write(str(row['生年月日']))
                    c2.write(row['性別'])
                    c3.write(row['毛色'] or '―')
                    c4.write(row['母名'] or '―')
                    c5.write(row['戦績'])

                st.markdown("---")
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("毛色の内訳")
                    import altair as alt
                    color_counts   = df_horses['毛色'].fillna('不明').value_counts()
                    df_color       = color_counts.reset_index()
                    df_color.columns = ['毛色', '頭数']
                    chart_color = alt.Chart(df_color).mark_bar().encode(
                        x=alt.X('毛色:N', sort=df_color['毛色'].tolist(), title=None,
                                axis=alt.Axis(labelAngle=-45, labelOverlap=False)),
                        y=alt.Y('頭数:Q', title='頭数'),
                        tooltip=['毛色', '頭数']
                    ).properties(height=400)
                    st.altair_chart(chart_color, use_container_width=True)

                with col2:
                    st.subheader("生産地別 頭数")
                    import altair as alt
                    sql_location = """
                        SELECT b.location AS 生産地
                        FROM horses h
                        LEFT JOIN breeders b ON h.breeder_id = b.breeder_id
                        WHERE h.sire_id = 222
                    """
                    loc_params = []
                    if horse_name_input:
                        sql_location += " AND h.horse_name LIKE %s"
                        loc_params.append(f"%{horse_name_input}%")
                    if selected_gender != "すべて":
                        sql_location += " AND h.gender = %s"
                        loc_params.append(selected_gender)
                    sql_location += " AND YEAR(h.date_of_birth) BETWEEN %s AND %s"
                    loc_params.extend([birth_year_from, birth_year_to])

                    df_loc     = run_query(sql_location, loc_params)
                    loc_counts = df_loc['生産地'].fillna('不明').value_counts()
                    top9_loc   = loc_counts.head(9)
                    others_loc = loc_counts.iloc[9:].sum()
                    df_chart   = top9_loc.reset_index()
                    df_chart.columns = ['生産地', '頭数']
                    df_chart   = pd.concat(
                        [df_chart, pd.DataFrame([{'生産地': 'その他', '頭数': others_loc}])],
                        ignore_index=True
                    )
                    chart_loc = alt.Chart(df_chart).mark_bar().encode(
                        x=alt.X('生産地:N', sort=df_chart['生産地'].tolist(), title=None,
                                axis=alt.Axis(labelAngle=-45, labelOverlap=False)),
                        y=alt.Y('頭数:Q', title='頭数'),
                        tooltip=['生産地', '頭数']
                    ).properties(height=400)
                    st.altair_chart(chart_loc, use_container_width=True)

        except Exception as e:
            st.error(f"エラーが発生しました: {e}")

    # ── TAB 2: レース成績検索 ───────────────────────
    with tab2:
        st.subheader("馬名でレース成績を検索")
        search_name = st.text_input("馬名を入力（部分一致）", value=horse_name_input, key="tab2_name")

        if st.button("成績を検索", type="primary"):
            if not search_name:
                st.warning("馬名を入力してください")
            else:
                sql_results = """
                    SELECT
                        h.horse_name       AS 馬名,
                        r.race_date        AS 開催日,
                        r.race_name        AS レース名,
                        t.track_name       AS 競馬場,
                        t.course_direction AS コース方向,
                        r.distance_meters  AS 距離_m,
                        r.surface_type     AS 馬場種別,
                        r.track_condition  AS 馬場状態,
                        r.race_class       AS クラス,
                        re.final_rank      AS 着順,
                        re.time_seconds    AS タイム_秒,
                        re.running_style   AS 脚質,
                        re.race_pace       AS レースペース,
                        re.Weight          AS 斤量,
                        re.harness         AS 馬具,
                        j.jockey_name      AS 騎手,
                        tr.trainer_name    AS 調教師,
                        tr.region          AS 調教師所属
                    FROM raceentries re
                    JOIN horses  h  ON re.horse_id   = h.horse_id
                    JOIN races   r  ON re.race_id    = r.race_id
                    JOIN tracks  t  ON r.track_id    = t.track_id
                    LEFT JOIN jockeys  j  ON re.jockey_id  = j.jockey_id
                    LEFT JOIN trainers tr ON re.trainer_id = tr.trainer_id
                    WHERE h.sire_id = 222
                      AND h.horse_name LIKE %s
                    ORDER BY r.race_date DESC
                """
                try:
                    df_results = run_query(sql_results, [f"%{search_name}%"])
                    if df_results.empty:
                        st.info("該当する成績が見つかりませんでした。")
                    else:
                        rank_series = pd.to_numeric(df_results['着順'], errors='coerce')
                        total = len(df_results)
                        w1 = int((rank_series == 1).sum())
                        w2 = int((rank_series == 2).sum())
                        w3 = int((rank_series == 3).sum())
                        w4 = int((rank_series >= 4).sum())
                        st.markdown(
                            f"### {total}戦{w1}勝　"
                            f"<span style='font-size:1.1em; color:#555;'>({w1}-{w2}-{w3}-{w4})</span>",
                            unsafe_allow_html=True
                        )
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("勝率",    f"{w1/total*100:.1f}%")
                        m2.metric("連対率",  f"{(w1+w2)/total*100:.1f}%")
                        m3.metric("複勝率",  f"{(w1+w2+w3)/total*100:.1f}%")
                        m4.metric("3着内数", f"{w1+w2+w3}回")
                        st.dataframe(df_results, use_container_width=True, hide_index=True)
                        st.subheader("競馬場別 出走数")
                        st.bar_chart(df_results['競馬場'].value_counts())
                except Exception as e:
                    st.error(f"エラーが発生しました: {e}")

    # ── TAB 3: コラム・記事 ─────────────────────────
    with tab3:
        st.subheader("📝 エフフォーリア産駒に関する考察・コラム")
        st.markdown("---")

        sql_articles = """
            SELECT
                article_id,
                title
            FROM articles
            ORDER BY created_at DESC
        """
        try:
            df_articles = run_query(sql_articles)
            if df_articles.empty:
                st.info("現在、掲載されている記事はありません。")
            else:
                for _, row in df_articles.iterrows():
                    aid = row['article_id']

                    if st.session_state.is_admin:
                        col_title, col_btn = st.columns([9, 1])
                        col_title.button(
                            f"📄 {row['title']}",
                            key=f"article_btn_{aid}",
                            on_click=go_article,
                            args=(aid,),
                            use_container_width=True
                        )
                        if col_btn.button("🗑️", key=f"del_{aid}"):
                            conn = get_connection()
                            cursor = conn.cursor()
                            cursor.execute("DELETE FROM articles WHERE article_id = %s", (aid,))
                            conn.commit()
                            cursor.close()
                            conn.close()
                            st.rerun()
                    else:
                        st.button(
                            f"📄 {row['title']}",
                            key=f"article_btn_{aid}",
                            on_click=go_article,
                            args=(aid,),
                            use_container_width=True
                        )

                    st.markdown("---")

        except Exception as e:
            st.error(f"記事の読み込みに失敗しました: {e}")
