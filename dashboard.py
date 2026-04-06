import sqlite3
import os
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.environ.get("DB_PATH", "kospi_eps.db")

st.set_page_config(page_title="KOSPI EPS Monitor", layout="wide")
st.title("KOSPI 200 — Forward EPS Monitor")


# ── DB helpers ────────────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def q(sql, params=()):
    return pd.read_sql_query(sql, get_conn(), params=params)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filters")

    companies_df = q("""
        SELECT DISTINCT r.ticker, r.company
        FROM analyst_reports r
        ORDER BY r.company
    """)
    company_options = ["All"] + [
        f"{row['company']} ({row['ticker']})" for _, row in companies_df.iterrows()
    ]
    selected_company = st.selectbox("Company", company_options)

    years_df = q("SELECT DISTINCT fiscal_year FROM eps_estimates ORDER BY fiscal_year")
    available_years = years_df["fiscal_year"].tolist()
    selected_year = st.selectbox("Fiscal Year", available_years, index=len(available_years) - 1)

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
total_companies = q("SELECT COUNT(DISTINCT ticker) FROM analyst_reports").iloc[0]["COUNT(DISTINCT ticker)"]
today_reports = q("SELECT COUNT(*) AS n FROM analyst_reports WHERE report_date = date('now')").iloc[0]["n"]
upgrades = q("""
    SELECT COUNT(*) AS n FROM (
        SELECT e.ticker, e.fiscal_year, e.broker, e.fwd_eps,
               LAG(e.fwd_eps) OVER (PARTITION BY e.ticker, e.fiscal_year, e.broker ORDER BY e.extracted_at) AS prev_eps
        FROM eps_estimates e
    ) WHERE prev_eps IS NOT NULL AND fwd_eps > prev_eps * 1.02
""").iloc[0]["n"]

col1.metric("Total Reports", f"{total_reports:,}")
col2.metric("Companies Covered", total_companies)
col3.metric("Reports Today", today_reports)
col4.metric("EPS Upgrades Detected", upgrades)

st.divider()


# ── Main tabs ─────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs(["EPS Estimates", "Recent Reports", "EPS Revisions"])


# ── Tab 1: EPS Estimates table ────────────────────────────────────────────────

with tab1:
    ticker_filter = "AND e.ticker = ?" if selected_ticker else ""
    params = (selected_ticker,) if selected_ticker else ()

    eps_df = q(f"""
        SELECT
            r.company,
            e.ticker,
            e.broker,
            e.fiscal_year,
            e.fwd_eps,
            e.target_price,
            e.recommendation,
            r.report_date,
            r.report_url
        FROM eps_estimates e
        JOIN analyst_reports r ON e.report_id = r.id
        WHERE e.fiscal_year = {selected_year}
        {ticker_filter}
        ORDER BY r.report_date DESC, r.company
    """, params)

    if eps_df.empty:
        st.info("No EPS estimates found for the selected filters.")
    else:
        # Summary pivot: latest EPS per company for selected year
        latest_eps = eps_df.sort_values("report_date", ascending=False).drop_duplicates(
            subset=["ticker", "broker"]
        )

        # Chart: EPS by company
        if not selected_ticker:
            fig = go.Figure()
            chart_data = latest_eps.sort_values("fwd_eps", ascending=True).head(30)
            colors = ["#ef4444" if v < 0 else "#22c55e" for v in chart_data["fwd_eps"]]
            fig.add_trace(go.Bar(
                x=chart_data["fwd_eps"],
                y=chart_data["company"],
                orientation="h",
                marker_color=colors,
                text=[f"{v:,.0f}" for v in chart_data["fwd_eps"]],
                textposition="outside",
            ))
            fig.update_layout(
                title=f"FWD EPS {selected_year}E — Latest Estimates",
                xaxis_title="EPS (KRW/share)",
                yaxis_title="",
                height=max(400, len(chart_data) * 28),
                margin=dict(l=10, r=60, t=40, b=10),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)

        # Table
        display_df = latest_eps[["company", "ticker", "fiscal_year", "fwd_eps", "target_price", "recommendation", "broker", "report_date"]].copy()
        display_df.columns = ["Company", "Ticker", "Year", "FWD EPS", "Target Price", "Rec.", "Broker", "Report Date"]
        display_df["FWD EPS"] = display_df["FWD EPS"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "-")
        display_df["Target Price"] = display_df["Target Price"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "-")

        st.dataframe(display_df, use_container_width=True, hide_index=True)


# ── Tab 2: Recent Reports ─────────────────────────────────────────────────────

with tab2:
    ticker_filter = "AND ticker = ?" if selected_ticker else ""
    params = (selected_ticker,) if selected_ticker else ()

    reports_df = q(f"""
        SELECT company, ticker, broker, title, report_date, report_url
        FROM analyst_reports
        WHERE 1=1 {ticker_filter}
        ORDER BY fetched_at DESC
        LIMIT 100
    """, params)

    if reports_df.empty:
        st.info("No reports found.")
    else:
        for _, row in reports_df.iterrows():
            with st.container():
                c1, c2, c3 = st.columns([3, 2, 1])
                with c1:
                    st.markdown(f"**{row['company']}** ({row['ticker']})")
                    st.caption(row["title"])
                with c2:
                    st.write(row["broker"])
                    st.caption(row["report_date"])
                with c3:
                    st.link_button("PDF", row["report_url"])
            st.divider()


# ── Tab 3: EPS Revisions ──────────────────────────────────────────────────────

with tab3:
    ticker_filter = "AND e.ticker = ?" if selected_ticker else ""
    params = (selected_ticker,) if selected_ticker else ()

    revisions_df = q(f"""
        SELECT
            r.company,
            e.ticker,
            e.broker,
            e.fiscal_year,
            e.fwd_eps,
            LAG(e.fwd_eps) OVER (
                PARTITION BY e.ticker, e.fiscal_year, e.broker
                ORDER BY e.extracted_at
            ) AS prev_eps,
            e.extracted_at,
            r.report_url
        FROM eps_estimates e
        JOIN analyst_reports r ON e.report_id = r.id
        WHERE 1=1 {ticker_filter}
        ORDER BY e.extracted_at DESC
    """, params)

    revisions_df = revisions_df.dropna(subset=["prev_eps"])
    if revisions_df.empty:
        st.info("No revisions yet — need at least two reports for the same company/broker/year.")
    else:
        revisions_df["change"] = revisions_df["fwd_eps"] - revisions_df["prev_eps"]
        revisions_df["change_pct"] = revisions_df["change"] / revisions_df["prev_eps"].abs() * 100
        revisions_df["direction"] = revisions_df["change"].apply(lambda x: "▲ Upgrade" if x > 0 else "▼ Downgrade")

        upgrades_df = revisions_df[revisions_df["change"] > 0].sort_values("change_pct", ascending=False)
        downgrades_df = revisions_df[revisions_df["change"] < 0].sort_values("change_pct")

        col_u, col_d = st.columns(2)

        with col_u:
            st.subheader("▲ Upgrades")
            if upgrades_df.empty:
                st.info("No upgrades yet.")
            else:
                for _, row in upgrades_df.iterrows():
                    st.markdown(
                        f"**{row['company']}** ({row['ticker']}) {row['fiscal_year']}E  \n"
                        f"{row['prev_eps']:,.0f} → **{row['fwd_eps']:,.0f}** "
                        f"({row['change_pct']:+.1f}%)  \n"
                        f"_{row['broker']}_"
                    )
                    st.divider()

        with col_d:
            st.subheader("▼ Downgrades")
            if downgrades_df.empty:
                st.info("No downgrades yet.")
            else:
                for _, row in downgrades_df.iterrows():
                    st.markdown(
                        f"**{row['company']}** ({row['ticker']}) {row['fiscal_year']}E  \n"
                        f"{row['prev_eps']:,.0f} → **{row['fwd_eps']:,.0f}** "
                        f"({row['change_pct']:+.1f}%)  \n"
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
