import sqlite3
import pandas as pd
import plotly.express as px
import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Real-Time Crypto Pipeline", page_icon="⚡", layout="wide"
)

# Sidebar controls
st.sidebar.header("Pipeline Controls")
asset = st.sidebar.selectbox("Select Asset", ["BTC-USD", "ETH-USD", "SOL-USD"])
limit = st.sidebar.slider("Sliding Window Depth (Ticks)", 10, 200, 30)

st.title("⚡ Advanced Real-Time Market Data Pipeline")
st.markdown(
    "Multi-asset streaming pipeline featuring thread-safe queues, SQLite persistence, and Plotly analytics."
)


@st.cache_data(ttl=2)
def load_data(asset, limit):
  try:
    conn = sqlite3.connect("crypto_data.db")
    query = (
        "SELECT * FROM trades WHERE asset = ? ORDER BY timestamp DESC LIMIT ?"
    )
    df = pd.read_sql(query, conn, params=(asset, limit))
    conn.close()
    return df
  except Exception:
    # Returns an empty DataFrame if the database or table doesn't exist yet
    return pd.DataFrame(columns=["timestamp", "price", "volume", "asset"])


def render_dashboard():
  df = load_data(asset, limit)

  if df.empty:
    st.warning(
        f"⚠️ No data found for {asset} yet. Make sure your ingestion engine is"
        " running and writing to the database!"
    )
    return

  # Sort chronologically for proper time-series charting
  df = df.sort_values("timestamp")

  # Metrics Row
  latest_price = df["iloc"][-1] if not df.empty else 0
  col1, col2, col3 = st.columns(3)

  with col1:
    st.metric(label=f"{asset} Latest Price", value=f"${df['price'].iloc[-1]:,.2f}")
  with col2:
    st.metric(label="Total Volume (Window)", value=f"{df['volume'].sum():,.4f}")
  with col3:
    st.metric(label="Data Points Loaded", value=len(df))

  # Price Trend Chart
  st.subheader(f"Live Price Action ({asset})")
  fig = px.line(
      df,
      x="timestamp",
      y="price",
      markers=True,
      title=f"Real-Time Tick Prices for {asset}",
  )
  fig.update_layout(xaxis_title="Timestamp", yaxis_title="Price (USD)")
  st.plotly_chart(fig, use_container_width=True)

  # Raw Data View
  with st.expander("View Raw Database Ticks"):
    st.dataframe(df)


render_dashboard()

# Auto-refresh the dashboard every 2 seconds to fetch live updates
st.rerun()