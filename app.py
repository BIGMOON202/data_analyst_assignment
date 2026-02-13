"""
Cross-Channel Marketing Analytics Dashboard
Supports BigQuery or Supabase (PostgreSQL). Set DATABASE_URL for Supabase, or
BQ_PROJECT + BQ_DATASET for BigQuery.
"""
import os
from pathlib import Path
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Page config
st.set_page_config(
    page_title="Cross-Channel Ads Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ----- Data source: Supabase (Postgres) or BigQuery -----
def get_database_url():
    return os.environ.get("DATABASE_URL") or st.secrets.get("DATABASE_URL", "")


def get_bq_config():
    project = os.environ.get("BQ_PROJECT") or st.secrets.get("BQ_PROJECT", "")
    dataset = os.environ.get("BQ_DATASET") or st.secrets.get("BQ_DATASET", "")
    return project, dataset


@st.cache_data(ttl=300)
def load_unified_from_postgres(url: str) -> pd.DataFrame:
    if not url or not url.strip().startswith("postgresql"):
        return None
    from sqlalchemy import create_engine
    engine = create_engine(url)
    query = """
    SELECT date, platform, campaign_id, campaign_name, ad_group_id, ad_group_name,
           impressions, clicks, spend, conversions
    FROM unified_ads
    ORDER BY date, platform
    """
    return pd.read_sql(query, engine)


@st.cache_data(ttl=300)
def load_daily_from_postgres(url: str) -> pd.DataFrame:
    if not url or not url.strip().startswith("postgresql"):
        return None
    try:
        from sqlalchemy import create_engine
        engine = create_engine(url)
        query = """
        SELECT date, platform, impressions, clicks, spend, conversions, ctr, cost_per_conversion
        FROM unified_ads_daily_summary
        ORDER BY date, platform
        """
        return pd.read_sql(query, engine)
    except Exception:
        return None


@st.cache_data(ttl=300)
def load_unified_from_bigquery(project: str, dataset: str) -> pd.DataFrame:
    if not project or not dataset:
        return None
    from google.cloud import bigquery
    client = bigquery.Client(project=project)
    query = f"""
    SELECT date, platform, campaign_id, campaign_name, ad_group_id, ad_group_name,
           impressions, clicks, spend, conversions
    FROM `{project}.{dataset}.unified_ads`
    ORDER BY date, platform
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_daily_from_bigquery(project: str, dataset: str) -> pd.DataFrame:
    if not project or not dataset:
        return None
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project)
        query = f"""
        SELECT date, platform, impressions, clicks, spend, conversions, ctr, cost_per_conversion
        FROM `{project}.{dataset}.unified_ads_daily_summary`
        ORDER BY date, platform
        """
        return client.query(query).to_dataframe()
    except Exception:
        return None


def main():
    st.title("📊 Cross-Channel Advertising Performance")
    st.caption("Unified view: Facebook, Google, TikTok")

    database_url = get_database_url()
    project, dataset = get_bq_config()

    # Sidebar: data source and connection
    with st.sidebar:
        st.header("Data source")
        use_supabase = st.radio(
            "Database",
            ["Supabase (PostgreSQL)", "BigQuery"],
            index=0 if database_url else 1,
        )
        if use_supabase == "Supabase (PostgreSQL)":
            if database_url:
                st.success("DATABASE_URL is set (env or secrets)")
            else:
                database_url = st.text_input(
                    "Database URL (PostgreSQL)",
                    placeholder="postgresql://postgres.xxx:password@...",
                    type="password",
                )
        else:
            st.header("BigQuery")
            if not project:
                project = st.text_input("GCP Project ID", placeholder="my-project")
            else:
                st.text(f"Project: {project}")
            if not dataset:
                dataset = st.text_input("Dataset ID", placeholder="marketing_analyst")
            else:
                st.text(f"Dataset: {dataset}")

    # Load data
    df = None
    daily = None
    data_source_label = ""

    if use_supabase == "Supabase (PostgreSQL)" and database_url:
        data_source_label = "Supabase (unified_ads)"
        try:
            df = load_unified_from_postgres(database_url)
            daily = load_daily_from_postgres(database_url)
        except Exception as e:
            st.error(f"Could not connect to Supabase: {e}")
            st.info("Check DATABASE_URL. Get it from Supabase: Project Settings → Database → Connection string (URI). Replace [YOUR-PASSWORD] with your DB password.")
            return
    elif use_supabase != "Supabase (PostgreSQL)" and project and dataset:
        data_source_label = "BigQuery"
        try:
            df = load_unified_from_bigquery(project, dataset)
            daily = load_daily_from_bigquery(project, dataset)
        except Exception as e:
            st.error(f"Could not load from BigQuery: {e}")
            st.info("Ensure the unified table exists and credentials are set (GOOGLE_APPLICATION_CREDENTIALS or gcloud auth).")
            return
    else:
        if use_supabase == "Supabase (PostgreSQL)":
            st.info("Set **DATABASE_URL** in the sidebar (or in env/secrets). Get it from Supabase: Project Settings → Database → Connection string (URI).")
        else:
            st.info("Enter your BigQuery **Project ID** and **Dataset ID** in the sidebar, then run the SQL script to create the unified table.")
        st.markdown("""
        **Supabase:** See `SUPABASE_GUIDE.md` for loading CSVs and running `sql/supabase/02_unified_model.sql`.
        **BigQuery:** See `ASSIGNMENT_GUIDE.md` for loading CSVs and running `sql/02_unified_model.sql`.
        """)
        return

    if df is None or df.empty:
        st.warning("No data returned. Check that the unified table exists and is populated.")
        return

    # ----- KPIs -----
    st.subheader("Key metrics (all time)")
    tot_spend = df["spend"].sum()
    tot_impressions = df["impressions"].sum()
    tot_clicks = df["clicks"].sum()
    tot_conversions = df["conversions"].sum()
    overall_cpc = tot_spend / tot_clicks if tot_clicks else 0
    overall_cpa = tot_spend / tot_conversions if tot_conversions else 0
    overall_ctr = (tot_clicks / tot_impressions * 100) if tot_impressions else 0

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total spend", f"${tot_spend:,.0f}")
    k2.metric("Impressions", f"{tot_impressions:,.0f}")
    k3.metric("Clicks", f"{tot_clicks:,.0f}")
    k4.metric("Conversions", f"{tot_conversions:,.0f}")
    k5.metric("CTR %", f"{overall_ctr:.2f}%")
    k6.metric("Cost per conversion", f"${overall_cpa:,.2f}")

    st.divider()

    # ----- By platform -----
    st.subheader("Performance by platform")
    by_platform = (
        df.groupby("platform", as_index=False)
        .agg(
            spend=("spend", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            conversions=("conversions", "sum"),
        )
    )
    by_platform["ctr_pct"] = (by_platform["clicks"] / by_platform["impressions"] * 100).round(2)
    by_platform["cost_per_conv"] = (by_platform["spend"] / by_platform["conversions"].replace(0, float("nan"))).round(2)
    by_platform["share_of_spend"] = (by_platform["spend"] / by_platform["spend"].sum() * 100).round(1)

    col1, col2 = st.columns(2)

    with col1:
        fig_spend = px.bar(
            by_platform,
            x="platform",
            y="spend",
            title="Spend by platform",
            color="platform",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_spend.update_layout(showlegend=False, xaxis_title="", yaxis_title="Spend ($)")
        st.plotly_chart(fig_spend, use_container_width=True)

    with col2:
        fig_conv = px.bar(
            by_platform,
            x="platform",
            y="conversions",
            title="Conversions by platform",
            color="platform",
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig_conv.update_layout(showlegend=False, xaxis_title="", yaxis_title="Conversions")
        st.plotly_chart(fig_conv, use_container_width=True)

    display_df = by_platform[
        ["platform", "spend", "impressions", "clicks", "conversions", "ctr_pct", "cost_per_conv", "share_of_spend"]
    ].copy()
    display_df["spend"] = display_df["spend"].apply(lambda x: f"${x:,.0f}")
    display_df["impressions"] = display_df["impressions"].apply(lambda x: f"{x:,.0f}")
    display_df["clicks"] = display_df["clicks"].apply(lambda x: f"{x:,.0f}")
    display_df["conversions"] = display_df["conversions"].apply(lambda x: f"{x:,.0f}")
    display_df["ctr_pct"] = display_df["ctr_pct"].apply(lambda x: f"{x:.2f}%")
    display_df["cost_per_conv"] = display_df["cost_per_conv"].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "—")
    display_df["share_of_spend"] = display_df["share_of_spend"].apply(lambda x: f"{x:.1f}%")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.divider()

    # ----- Over time -----
    st.subheader("Spend and conversions over time")
    if daily is not None and not daily.empty:
        fig_time = go.Figure()
        for platform in daily["platform"].unique():
            sub = daily[daily["platform"] == platform]
            fig_time.add_trace(
                go.Scatter(
                    x=sub["date"],
                    y=sub["spend"],
                    name=platform,
                    mode="lines+markers",
                )
            )
        fig_time.update_layout(
            title="Daily spend by platform",
            xaxis_title="Date",
            yaxis_title="Spend ($)",
            legend_title="Platform",
            hovermode="x unified",
        )
        st.plotly_chart(fig_time, use_container_width=True)

        fig_conv_time = go.Figure()
        for platform in daily["platform"].unique():
            sub = daily[daily["platform"] == platform]
            fig_conv_time.add_trace(
                go.Scatter(
                    x=sub["date"],
                    y=sub["conversions"],
                    name=platform,
                    mode="lines+markers",
                )
            )
        fig_conv_time.update_layout(
            title="Daily conversions by platform",
            xaxis_title="Date",
            yaxis_title="Conversions",
            legend_title="Platform",
            hovermode="x unified",
        )
        st.plotly_chart(fig_conv_time, use_container_width=True)
    else:
        daily_alt = df.groupby(["date", "platform"]).agg({"spend": "sum", "conversions": "sum"}).reset_index()
        fig_time = px.line(daily_alt, x="date", y="spend", color="platform", title="Daily spend by platform", markers=True)
        st.plotly_chart(fig_time, use_container_width=True)

    st.divider()

    # ----- Efficiency -----
    st.subheader("Efficiency: cost per conversion by platform")
    eff = by_platform[["platform", "spend", "conversions", "cost_per_conv"]].dropna(subset=["cost_per_conv"])
    if not eff.empty:
        fig_cpa = px.bar(
            eff,
            x="platform",
            y="cost_per_conv",
            title="Cost per conversion ($)",
            color="platform",
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig_cpa.update_layout(showlegend=False, xaxis_title="", yaxis_title="Cost per conversion ($)")
        st.plotly_chart(fig_cpa, use_container_width=True)

    st.divider()
    st.caption(f"Data source: {data_source_label} · Marketing Analyst Assignment")


if __name__ == "__main__":
    main()
