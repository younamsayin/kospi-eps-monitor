import sqlite3
import os
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from db.models import init_db, add_favorite_company, list_favorite_companies, remove_favorite_company

load_dotenv()

DB_PATH = os.environ.get("DB_PATH", "kospi_eps.db")

init_db()

st.set_page_config(page_title="KOSPI EPS Monitor", layout="wide")

st.markdown(
    """
    <style>
        .block-container {
            max-width: 100%;
            padding-top: 1.25rem;
            padding-left: clamp(1rem, 3vw, 2.5rem);
            padding-right: clamp(1rem, 3vw, 2.5rem);
            padding-bottom: 2rem;
        }

        [data-testid="stSidebar"] {
            min-width: min(22rem, 85vw);
            max-width: min(22rem, 85vw);
        }

        [data-testid="stHorizontalBlock"] {
            gap: 0.75rem;
            flex-wrap: wrap;
        }

        [data-testid="column"] {
            min-width: min(100%, 240px);
        }

        .stTabs [data-baseweb="tab-list"] {
            flex-wrap: wrap;
            gap: 0.5rem;
        }

        .stTabs [data-baseweb="tab"] {
            white-space: nowrap;
        }

        @media (max-width: 900px) {
            .block-container {
                padding-left: 1rem;
                padding-right: 1rem;
            }

            [data-testid="stSidebar"] {
                min-width: 100vw;
                max-width: 100vw;
            }
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("KOSPI 200 — Forward EPS Monitor")


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def q(sql, params=()):
    with get_conn() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_favorites_map():
    with get_conn() as conn:
        rows = list_favorite_companies(conn)
    return {row["ticker"]: row["company"] for row in rows}


def toggle_favorite_company(company_label: str):
    if company_label == "All":
        return
    ticker = company_label.split("(")[-1].rstrip(")")
    company = company_label.rsplit("(", 1)[0].strip()
    with get_conn() as conn:
        favorites = {row["ticker"] for row in list_favorite_companies(conn)}
        if ticker in favorites:
            remove_favorite_company(conn, ticker)
        else:
            add_favorite_company(conn, ticker, company)
        conn.commit()


def select_company(company_label: str):
    st.session_state["selected_company"] = company_label


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filters")

    companies_df = q("""
        SELECT DISTINCT r.ticker, r.company
        FROM analyst_reports r
        WHERE r.ticker != ''
        ORDER BY r.company
    """)
    company_options = ["All"] + [
        f"{row['company']} ({row['ticker']})" for _, row in companies_df.iterrows()
    ]
    if "selected_company" not in st.session_state or st.session_state["selected_company"] not in company_options:
        st.session_state["selected_company"] = "All"

    favorites_map = get_favorites_map()

    company_col, star_col = st.columns([6, 1])
    with company_col:
        selected_company = st.selectbox("Company", company_options, key="selected_company")
    with star_col:
        is_favorite = False
        if selected_company != "All":
            selected_ticker_preview = selected_company.split("(")[-1].rstrip(")")
            is_favorite = selected_ticker_preview in favorites_map
        star_label = "★" if is_favorite else "☆"
        if st.button(
            star_label,
            key="favorite_toggle",
            disabled=(selected_company == "All"),
            help="Add/remove this company from favorites",
            use_container_width=True,
        ):
            toggle_favorite_company(selected_company)
            st.rerun()

    if favorites_map:
        st.caption("Favorites")
        favorite_labels = [
            f"{company} ({ticker})"
            for ticker, company in sorted(favorites_map.items(), key=lambda item: item[1])
            if f"{company} ({ticker})" in company_options
        ]
        fav_cols = st.columns(2)
        for idx, label in enumerate(favorite_labels):
            with fav_cols[idx % 2]:
                st.button(
                    label,
                    key=f"favorite_{label}",
                    use_container_width=True,
                    on_click=select_company,
                    args=(label,),
                )

    if selected_company != "All":
        st.button(
            "← All Companies",
            use_container_width=True,
            on_click=select_company,
            args=("All",),
        )

    years_df = q("SELECT DISTINCT fiscal_year FROM eps_estimates ORDER BY fiscal_year")
    available_years = years_df["fiscal_year"].tolist()
    if available_years:
        # Default to current or next year
        import datetime
        current_year = datetime.date.today().year
        default_idx = next(
            (i for i, y in enumerate(available_years) if y >= current_year), len(available_years) - 1
        )
        selected_year = st.selectbox("Fiscal Year", available_years, index=default_idx)
    else:
        selected_year = 2026

    st.divider()
    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Parse filter ─────────────────────────────────────────────────────────────

selected_ticker = None
if selected_company != "All":
    selected_ticker = selected_company.split("(")[-1].rstrip(")")


# ── Top metrics ───────────────────────────────────────────────────────────────

col1, col2, col3, col4 = st.columns(4)

total_reports = q("SELECT COUNT(*) AS n FROM analyst_reports").iloc[0]["n"]
total_companies = q("SELECT COUNT(DISTINCT ticker) FROM analyst_reports WHERE ticker != ''").iloc[0]["COUNT(DISTINCT ticker)"]
today_reports = q("SELECT COUNT(*) AS n FROM analyst_reports WHERE report_date = date('now')").iloc[0]["n"]
total_revisions = q("""
    SELECT COUNT(*) AS n FROM (
        SELECT e.fwd_eps,
               LAG(e.fwd_eps) OVER (PARTITION BY e.ticker, e.fiscal_year, e.broker ORDER BY e.extracted_at) AS prev_eps
        FROM eps_estimates e
    ) WHERE prev_eps IS NOT NULL AND abs(fwd_eps - prev_eps) / abs(prev_eps) > 0.02
""").iloc[0]["n"]

col1.metric("Total Reports", f"{total_reports:,}")
col2.metric("Companies Covered", total_companies)
col3.metric("Reports Today", today_reports)
col4.metric("EPS Revisions (>2%)", total_revisions)

st.divider()


# ── Main tabs ─────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs([
    "Consensus", "Recent Reports", "Revisions"
])


# ── Tab 1: Consensus (average across brokers) ────────────────────────────────

with tab1:
    ticker_filter = "AND e.ticker = ?" if selected_ticker else ""
    params = (selected_ticker, selected_ticker) if selected_ticker else ()

    consensus_df = q(f"""
        WITH latest_per_broker AS (
            SELECT e.ticker, r.company, e.broker, e.fiscal_year, e.fwd_eps, e.target_price,
                   ROW_NUMBER() OVER (PARTITION BY e.ticker, e.broker, e.fiscal_year ORDER BY e.extracted_at DESC) AS rn
            FROM eps_estimates e
            JOIN analyst_reports r ON e.report_id = r.id
            WHERE e.fiscal_year = {selected_year} AND e.fwd_eps IS NOT NULL {ticker_filter}
        ),
        week_old_per_broker AS (
            SELECT e.ticker, r.company, e.broker, e.fiscal_year, e.fwd_eps, e.target_price,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.ticker, e.broker, e.fiscal_year
                       ORDER BY e.extracted_at DESC, e.id DESC
                   ) AS rn
            FROM eps_estimates e
            JOIN analyst_reports r ON e.report_id = r.id
            WHERE e.fiscal_year = {selected_year}
              AND e.fwd_eps IS NOT NULL
              AND DATE(e.extracted_at) <= DATE('now', '-7 day')
              {ticker_filter}
        ),
        latest_consensus AS (
            SELECT
                ticker, company,
                COUNT(DISTINCT broker) AS broker_count,
                ROUND(AVG(fwd_eps), 0) AS consensus_eps,
                ROUND(MIN(fwd_eps), 0) AS min_eps,
                ROUND(MAX(fwd_eps), 0) AS max_eps,
                ROUND(AVG(target_price), 0) AS consensus_tp,
                ROUND(MIN(target_price), 0) AS min_tp,
                ROUND(MAX(target_price), 0) AS max_tp
            FROM latest_per_broker
            WHERE rn = 1
            GROUP BY ticker, company
        ),
        week_old_consensus AS (
            SELECT
                ticker,
                ROUND(AVG(fwd_eps), 0) AS wow_base_eps,
                ROUND(AVG(target_price), 0) AS wow_base_tp
            FROM week_old_per_broker
            WHERE rn = 1
            GROUP BY ticker
        )
        SELECT
            l.ticker, l.company, l.broker_count,
            l.consensus_eps, l.min_eps, l.max_eps,
            l.consensus_tp, l.min_tp, l.max_tp,
            w.wow_base_eps, w.wow_base_tp,
            CASE
                WHEN w.wow_base_eps IS NOT NULL AND w.wow_base_eps != 0
                THEN ROUND((l.consensus_eps - w.wow_base_eps) * 100.0 / ABS(w.wow_base_eps), 2)
                ELSE NULL
            END AS wow_eps_change_pct,
            CASE
                WHEN w.wow_base_tp IS NOT NULL AND w.wow_base_tp != 0
                THEN ROUND((l.consensus_tp - w.wow_base_tp) * 100.0 / ABS(w.wow_base_tp), 2)
                ELSE NULL
            END AS wow_tp_change_pct
        FROM latest_consensus l
        LEFT JOIN week_old_consensus w ON l.ticker = w.ticker
        WHERE l.broker_count >= 1
        ORDER BY l.consensus_eps DESC
    """, params)

    if consensus_df.empty:
        st.info("No consensus data yet.")
    else:
        st.subheader(f"Consensus FWD EPS {selected_year}E")

        if selected_ticker:
            latest = consensus_df.iloc[0]
            metric_cols = st.columns(3)
            metric_cols[0].metric(
                "Consensus EPS",
                f"{latest['consensus_eps']:,.0f}" if pd.notna(latest["consensus_eps"]) else "-",
                f"{latest['wow_eps_change_pct']:+.1f}% WoW" if pd.notna(latest["wow_eps_change_pct"]) else None,
            )
            metric_cols[1].metric(
                "Consensus TP",
                f"{latest['consensus_tp']:,.0f}" if pd.notna(latest["consensus_tp"]) else "-",
                f"{latest['wow_tp_change_pct']:+.1f}% WoW" if pd.notna(latest["wow_tp_change_pct"]) else None,
            )
            metric_cols[2].metric(
                "Brokers",
                int(latest["broker_count"]) if pd.notna(latest["broker_count"]) else 0,
            )
        else:
            aggregate = consensus_df.agg({
                "company": "count",
                "consensus_eps": "mean",
                "consensus_tp": "mean",
                "broker_count": "sum",
            })
            metric_cols = st.columns(4)
            metric_cols[0].metric("Companies", int(aggregate["company"]))
            metric_cols[1].metric(
                "Avg Consensus EPS",
                f"{aggregate['consensus_eps']:,.0f}" if pd.notna(aggregate["consensus_eps"]) else "-",
            )
            metric_cols[2].metric(
                "Avg Consensus TP",
                f"{aggregate['consensus_tp']:,.0f}" if pd.notna(aggregate["consensus_tp"]) else "-",
            )
            metric_cols[3].metric("Broker Observations", int(aggregate["broker_count"]) if pd.notna(aggregate["broker_count"]) else 0)

        trend_title = (
            f"Weekly Consensus Trend — {selected_company}"
            if selected_ticker
            else "Weekly Aggregate Consensus Trend — All Companies"
        )
        trend_caption = (
            f"Weekly average consensus EPS and target price for the last 52 weeks ({selected_year}E)."
            if selected_ticker
            else f"Weekly average of company-level consensus EPS and target price for the last 52 weeks ({selected_year}E)."
        )
        st.subheader(trend_title)
        st.caption(trend_caption)

        trend_filter = "AND e.ticker = ?" if selected_ticker else ""
        trend_params = (selected_ticker,) if selected_ticker else ()
        trend_df = q(f"""
            WITH broker_daily_latest AS (
                SELECT
                    e.ticker,
                    e.broker,
                    e.fiscal_year,
                    e.fwd_eps,
                    e.target_price,
                    COALESCE(r.report_date, DATE(e.extracted_at)) AS report_day,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.ticker, e.broker, e.fiscal_year, COALESCE(r.report_date, DATE(e.extracted_at))
                        ORDER BY r.report_date DESC, e.extracted_at DESC, e.id DESC
                    ) AS rn
                FROM eps_estimates e
                JOIN analyst_reports r ON e.report_id = r.id
                WHERE e.fiscal_year = {selected_year}
                  AND (e.fwd_eps IS NOT NULL OR e.target_price IS NOT NULL)
                  AND DATE(COALESCE(r.report_date, DATE(e.extracted_at))) >= DATE('now', '-364 day')
                  {trend_filter}
            ),
            daily_consensus AS (
                SELECT
                    ticker,
                    report_day,
                    AVG(fwd_eps) AS consensus_eps,
                    AVG(target_price) AS consensus_tp
                FROM broker_daily_latest
                WHERE rn = 1
                GROUP BY ticker, report_day
            ),
            weekly_consensus AS (
                SELECT
                    DATE(report_day, '-' || ((CAST(strftime('%w', report_day) AS INTEGER) + 6) % 7) || ' days') AS week_start,
                    AVG(consensus_eps) AS avg_consensus_eps,
                    AVG(consensus_tp) AS avg_consensus_tp,
                    COUNT(DISTINCT ticker) AS company_count
                FROM daily_consensus
                GROUP BY week_start
            )
            SELECT
                week_start,
                avg_consensus_eps,
                avg_consensus_tp,
                company_count
            FROM weekly_consensus
            ORDER BY week_start DESC
            LIMIT 52
        """, trend_params)

        if trend_df.empty:
            st.info("Not enough history yet for a weekly consensus trend.")
        else:
            trend_df["week_start"] = pd.to_datetime(trend_df["week_start"])
            trend_df = trend_df.sort_values("week_start")
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trend_df["week_start"],
                y=trend_df["avg_consensus_eps"],
                mode="lines+markers",
                name="Avg EPS",
                yaxis="y1",
                hovertemplate="%{x}<br>Avg EPS: %{y:,.0f}<br>Companies: %{customdata}<extra></extra>",
                customdata=trend_df["company_count"],
            ))
            layout_kwargs = {
                "xaxis_title": "Week",
                "yaxis": dict(title="Average EPS (KRW/share)"),
                "height": 420,
                "margin": dict(l=10, r=10, t=20, b=10),
                "legend": dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                "plot_bgcolor": "rgba(0,0,0,0)",
                "paper_bgcolor": "rgba(0,0,0,0)",
            }
            if selected_ticker:
                fig.add_trace(go.Scatter(
                    x=trend_df["week_start"],
                    y=trend_df["avg_consensus_tp"],
                    mode="lines+markers",
                    name="Avg TP",
                    yaxis="y2",
                    hovertemplate="%{x}<br>Avg TP: %{y:,.0f}<br>Companies: %{customdata}<extra></extra>",
                    customdata=trend_df["company_count"],
                ))
                layout_kwargs["yaxis2"] = dict(title="Average TP (KRW)", overlaying="y", side="right")
            fig.update_layout(**layout_kwargs)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Latest Broker Reports")
        latest_report_filter = "AND r.ticker = ?" if selected_ticker else ""
        latest_report_params = (selected_ticker,) if selected_ticker else ()
        latest_reports_df = q(f"""
            WITH raw_report_rows AS (
                SELECT
                    r.report_date,
                    r.ticker,
                    r.company,
                    e.broker,
                    MAX(r.id) AS report_id,
                    MAX(e.extracted_at) AS extracted_at,
                    MAX(CASE WHEN e.fiscal_year = {selected_year} THEN e.fwd_eps END) AS eps_this_year,
                    MAX(CASE WHEN e.fiscal_year = {selected_year + 1} THEN e.fwd_eps END) AS eps_next_year,
                    MAX(e.target_price) AS target_price
                FROM analyst_reports r
                JOIN eps_estimates e ON e.report_id = r.id
                WHERE r.ticker != '' {latest_report_filter}
                GROUP BY r.report_date, r.ticker, r.company, e.broker
            ),
            report_rows AS (
                SELECT
                    *,
                    LAG(eps_this_year) OVER (
                        PARTITION BY ticker, broker
                        ORDER BY report_date, extracted_at, report_id
                    ) AS prev_eps_this_year,
                    LAG(eps_next_year) OVER (
                        PARTITION BY ticker, broker
                        ORDER BY report_date, extracted_at, report_id
                    ) AS prev_eps_next_year,
                    LAG(target_price) OVER (
                        PARTITION BY ticker, broker
                        ORDER BY report_date, extracted_at, report_id
                    ) AS prev_target_price
                FROM raw_report_rows
            ),
            latest_per_broker AS (
                SELECT
                    rr.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY rr.ticker, rr.broker
                        ORDER BY rr.report_date DESC, rr.extracted_at DESC, rr.report_id DESC
                    ) AS rn
                FROM report_rows rr
            )
            SELECT
                report_date,
                ticker,
                company,
                broker,
                eps_this_year,
                CASE
                    WHEN prev_eps_this_year IS NOT NULL AND prev_eps_this_year != 0 AND eps_this_year IS NOT NULL
                    THEN ROUND((eps_this_year - prev_eps_this_year) * 100.0 / ABS(prev_eps_this_year), 2)
                    ELSE NULL
                END AS eps_this_year_change_pct,
                eps_next_year,
                CASE
                    WHEN prev_eps_next_year IS NOT NULL AND prev_eps_next_year != 0 AND eps_next_year IS NOT NULL
                    THEN ROUND((eps_next_year - prev_eps_next_year) * 100.0 / ABS(prev_eps_next_year), 2)
                    ELSE NULL
                END AS eps_next_year_change_pct,
                target_price,
                CASE
                    WHEN prev_target_price IS NOT NULL AND prev_target_price != 0 AND target_price IS NOT NULL
                    THEN ROUND((target_price - prev_target_price) * 100.0 / ABS(prev_target_price), 2)
                    ELSE NULL
                END AS target_price_change_pct
            FROM latest_per_broker
            WHERE rn = 1
            ORDER BY report_date DESC, ticker, broker
        """, latest_report_params)

        if latest_reports_df.empty:
            st.info("No latest broker report rows available yet.")
        else:
            display_df = latest_reports_df.copy()
            display_df.columns = [
                "Date",
                "Ticker",
                "Company",
                "Broker",
                f"EPS {selected_year}E",
                f"EPS {selected_year}E %",
                f"EPS {selected_year + 1}E",
                f"EPS {selected_year + 1}E %",
                "TP",
                "TP %",
            ]
            for col in [f"EPS {selected_year}E", f"EPS {selected_year + 1}E", "TP"]:
                display_df[col] = display_df[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) and x else "-")
            for col in [f"EPS {selected_year}E %", f"EPS {selected_year + 1}E %", "TP %"]:
                display_df[col] = display_df[col].apply(lambda x: f"{x:+.2f}%" if pd.notna(x) else "-")
            st.dataframe(display_df, use_container_width=True, hide_index=True)


# ── Tab 2: Recent Reports ───────────────────────────────────────────────────

with tab2:
    ticker_filter = "AND ticker = ?" if selected_ticker else ""
    params = (selected_ticker,) if selected_ticker else ()

    reports_df = q(f"""
        SELECT company, ticker, broker, source, title, report_date, report_url
        FROM analyst_reports
        WHERE ticker != '' {ticker_filter}
        ORDER BY report_date DESC, fetched_at DESC
        LIMIT 100
    """, params)

    if reports_df.empty:
        st.info("No reports found.")
    else:
        for _, row in reports_df.iterrows():
            with st.container():
                c1, c2, c3, c4 = st.columns([3, 2, 1.2, 1])
                with c1:
                    st.markdown(f"**{row['company']}** ({row['ticker']})")
                    st.caption(row["title"])
                with c2:
                    st.write(row["broker"])
                    st.caption(row["report_date"])
                with c3:
                    source = row["source"] if pd.notna(row["source"]) and row["source"] else "-"
                    st.write(str(source).title())
                    st.caption("Source")
                with c4:
                    st.link_button("PDF", row["report_url"])
            st.divider()


# ── Tab 3: EPS & Target Price Revisions ──────────────────────────────────────

with tab3:
    ticker_filter = "AND e.ticker = ?" if selected_ticker else ""
    params = (selected_ticker,) if selected_ticker else ()

    revisions_df = q(f"""
        SELECT
            r.company, e.ticker, e.broker, e.fiscal_year,
            e.fwd_eps,
            r.report_date,
            LAG(e.fwd_eps) OVER (
                PARTITION BY e.ticker, e.fiscal_year, e.broker
                ORDER BY r.report_date, e.extracted_at, e.id
            ) AS prev_eps,
            LAG(r.report_date) OVER (
                PARTITION BY e.ticker, e.fiscal_year, e.broker
                ORDER BY r.report_date, e.extracted_at, e.id
            ) AS prev_report_date,
            e.target_price,
            e.extracted_at, r.report_url
        FROM eps_estimates e
        JOIN analyst_reports r ON e.report_id = r.id
        WHERE 1=1 {ticker_filter}
        ORDER BY r.report_date DESC, e.extracted_at DESC
    """, params)

    # --- EPS Revisions ---
    st.subheader("EPS Revisions")
    eps_rev = revisions_df.dropna(subset=["prev_eps"]).copy()
    eps_rev = eps_rev[eps_rev["prev_eps"] != 0]
    if eps_rev.empty:
        st.info("No EPS revisions yet — need at least two reports for the same company/broker/year.")
    else:
        eps_rev["change_pct"] = (eps_rev["fwd_eps"] - eps_rev["prev_eps"]) / eps_rev["prev_eps"].abs() * 100

        col_u, col_d = st.columns(2)

        upgrades = eps_rev[eps_rev["change_pct"] > 2].sort_values("change_pct", ascending=False)
        downgrades = eps_rev[eps_rev["change_pct"] < -2].sort_values("change_pct")

        with col_u:
            st.markdown("**▲ Upgrades (>2%)**")
            if upgrades.empty:
                st.info("No upgrades yet.")
            else:
                for _, row in upgrades.head(20).iterrows():
                    st.markdown(
                        f"**{row['company']}** ({row['ticker']}) {int(row['fiscal_year'])}E  \n"
                        f"{row['prev_eps']:,.0f} → **{row['fwd_eps']:,.0f}** ({row['change_pct']:+.1f}%)  \n"
                        f"{row['prev_report_date']} → {row['report_date']}  \n"
                        f"_{row['broker']}_"
                    )
                    st.divider()

        with col_d:
            st.markdown("**▼ Downgrades (>2%)**")
            if downgrades.empty:
                st.info("No downgrades yet.")
            else:
                for _, row in downgrades.head(20).iterrows():
                    st.markdown(
                        f"**{row['company']}** ({row['ticker']}) {int(row['fiscal_year'])}E  \n"
                        f"{row['prev_eps']:,.0f} → **{row['fwd_eps']:,.0f}** ({row['change_pct']:+.1f}%)  \n"
                        f"{row['prev_report_date']} → {row['report_date']}  \n"
                        f"_{row['broker']}_"
                    )
                    st.divider()

    # --- Target Price Revisions ---
    st.subheader("Target Price Revisions")
    tp_filter = "AND e.ticker = ?" if selected_ticker else ""
    tp_rev = q(f"""
        WITH report_tps AS (
            SELECT
                e.report_id,
                e.ticker,
                r.company,
                e.broker,
                e.target_price,
                MIN(e.extracted_at) AS extracted_at,
                r.report_date,
                r.report_url
            FROM eps_estimates e
            JOIN analyst_reports r ON e.report_id = r.id
            WHERE e.target_price IS NOT NULL {tp_filter}
            GROUP BY e.report_id, e.ticker, r.company, e.broker, e.target_price, r.report_date, r.report_url
        )
        SELECT
            company,
            ticker,
            broker,
            target_price,
            report_date,
            LAG(target_price) OVER (
                PARTITION BY ticker, broker
                ORDER BY report_date, extracted_at, report_id
            ) AS prev_tp,
            LAG(report_date) OVER (
                PARTITION BY ticker, broker
                ORDER BY report_date, extracted_at, report_id
            ) AS prev_report_date,
            extracted_at,
            report_url
        FROM report_tps
        ORDER BY report_date DESC, extracted_at DESC, report_id DESC
    """, params)
    tp_rev = tp_rev.dropna(subset=["prev_tp", "target_price"]).copy()
    tp_rev = tp_rev[tp_rev["prev_tp"] != 0]
    if tp_rev.empty:
        st.info("No target price revisions yet.")
    else:
        tp_rev["tp_change_pct"] = (tp_rev["target_price"] - tp_rev["prev_tp"]) / tp_rev["prev_tp"].abs() * 100

        col_r, col_c = st.columns(2)
        raised = tp_rev[tp_rev["tp_change_pct"] > 2].sort_values("tp_change_pct", ascending=False)
        cut = tp_rev[tp_rev["tp_change_pct"] < -2].sort_values("tp_change_pct")

        with col_r:
            st.markdown("**▲ TP Raised (>2%)**")
            if raised.empty:
                st.info("No target price raises yet.")
            else:
                for _, row in raised.head(20).iterrows():
                    st.markdown(
                        f"**{row['company']}** ({row['ticker']})  \n"
                        f"{row['prev_tp']:,.0f} → **{row['target_price']:,.0f}**원 ({row['tp_change_pct']:+.1f}%)  \n"
                        f"{row['prev_report_date']} → {row['report_date']}  \n"
                        f"_{row['broker']}_"
                    )
                    st.divider()

        with col_c:
            st.markdown("**▼ TP Cut (>2%)**")
            if cut.empty:
                st.info("No target price cuts yet.")
            else:
                for _, row in cut.head(20).iterrows():
                    st.markdown(
                        f"**{row['company']}** ({row['ticker']})  \n"
                        f"{row['prev_tp']:,.0f} → **{row['target_price']:,.0f}**원 ({row['tp_change_pct']:+.1f}%)  \n"
                        f"{row['prev_report_date']} → {row['report_date']}  \n"
                        f"_{row['broker']}_"
                    )
                    st.divider()

    # EPS trend chart for a specific company
    if selected_ticker:
        st.subheader(f"EPS History — {selected_company}")
        history_df = q("""
            SELECT e.fiscal_year, e.fwd_eps, e.broker, e.extracted_at
            FROM eps_estimates e
            WHERE e.ticker = ?
            ORDER BY e.extracted_at
        """, (selected_ticker,))

        if not history_df.empty:
            fig = go.Figure()
            for year in sorted(history_df["fiscal_year"].unique()):
                yr_df = history_df[history_df["fiscal_year"] == year]
                fig.add_trace(go.Scatter(
                    x=yr_df["extracted_at"],
                    y=yr_df["fwd_eps"],
                    mode="lines+markers",
                    name=f"{year}E",
                ))
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="EPS (KRW/share)",
                height=350,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)
