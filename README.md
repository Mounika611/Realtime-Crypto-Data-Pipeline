# ⚡ Real-Time Multi-Asset Crypto Data Pipeline

An enterprise-grade, high-throughput real-time data streaming pipeline and analytical dashboard built with Python, WebSockets, SQLite, and Streamlit.

This system ingests sub-second market ticker data from Coinbase WebSockets, routes incoming streams through an in-memory sliding window, safely writes to a SQLite database using a multi-threaded producer-consumer pattern, and visualizes live pricing analytics using interactive Plotly charts.

---

## 🏛 Architecture Diagram & Data Flow