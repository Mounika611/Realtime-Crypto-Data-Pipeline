import sqlite3
import time
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Advanced Real-Time Data Pipeline", layout="wide")
st.title("⚡ Advanced Real-Time Market Data Pipeline")
st.markdown(
    "Multi-asset streaming pipeline featuring thread-safe queues, SQLite"
    " persistence, and Plotly analytics."
)

USD_TO_INR = 95.65


def load_data(asset, limit):
  conn = sqlite3.connect("market_data.db")
  query = """
        SELECT timestamp, price, rolling_avg FROM asset_prices 
        WHERE asset = ? ORDER BY id DESC LIMIT ?
    """
  df = pd.read_sql(query, conn, params=(asset, limit))
  conn.close()
  return df.iloc[::-1]


def load_alerts(asset):
  conn = sqlite3.connect("market_data.db")
  query = """
        SELECT timestamp, message FROM alerts 
        WHERE asset = ? ORDER BY id DESC LIMIT 10
    """
  df = pd.read_sql(query, conn, params=(asset,))
  conn.close()
  return df


# Sidebar Controls moved OUTSIDE the fragment
st.sidebar.header("Pipeline Controls")
selected_asset = st.sidebar.selectbox(
    "Select Asset", ["BTC-USD", "ETH-USD"], key="asset_selectbox"
)
window_limit = st.sidebar.slider(
    "Sliding Window Depth (Ticks)", 20, 100, 30, key="window_slider"
)


@st.fragment(run_every=2)
def render_dashboard(asset, limit):
  df = load_data(asset, limit)
  alerts_df = load_alerts(asset)

  if not df.empty:
    latest_price_usd = df["price"].iloc[-1]
    latest_avg_usd = df["rolling_avg"].iloc[-1]
    session_high_usd = df["price"].max()
    session_low_usd = df["price"].min()

    latest_price_inr = latest_price_usd * USD_TO_INR
    latest_avg_inr = latest_avg_usd * USD_TO_INR
    session_high_inr = session_high_usd * USD_TO_INR
    session_low_inr = session_low_usd * USD_TO_INR

    # Top Metric Cards (USD & INR + Session High/Low)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Latest Price (USD)", f"${latest_price_usd:,.2f}")
    col2.metric("Latest Price (INR)", f"₹{latest_price_inr:,.2f}")
    col3.metric("Session High (USD)", f"${session_high_usd:,.2f}")
    col4.metric("Session Low (USD)", f"${session_low_usd:,.2f}")

    # Advanced Plotly Interactive Chart
    st.subheader(f"📈 {asset} Professional Analytics Stream")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["price"],
            mode="lines+markers",
            name="Live Price",
            line=dict(color="#00FFA3", width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["rolling_avg"],
            mode="lines",
            name="Rolling Average",
            line=dict(color="#FF007F", width=2, dash="dash"),
        )
    )
    fig.update_layout(
        template="plotly_dark",
        margin=dict(l=20, r=20, t=20, b=20),
        height=400,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
        ),
    )
    st.plotly_chart(
        fig, use_container_width=True, key=f"plotly_chart_{asset}"
    )

    # Volatility Alert History Log Table
    st.subheader("🚨 Volatility Alert Log History (SQLite Persistence)")
    if not alerts_df.empty:
      st.dataframe(alerts_df, use_container_width=True)
    else:
      st.info("No volatility alerts recorded for this asset yet.")

    # CSV Data Export Component
    st.markdown("### 📊 Pipeline Data Management")
    csv_data = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"Download {asset} Buffer as CSV (ETL Export)",
        data=csv_data,
        file_name=f"{asset}_stream_export.csv",
        mime="text/csv",
        key=f"download_{asset}",
    )

  else:
    st.warning(
        f"Waiting for incoming data stream for {asset} from backend"
        " ingestion engine..."
    )


render_dashboard(selected_asset, window_limit)