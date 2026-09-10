import json
import queue
import sqlite3
import threading
from collections import defaultdict, deque
from datetime import datetime
import websocket

DB_NAME = "market_data.db"
write_queue = queue.Queue()


def init_db():
  conn = sqlite3.connect(DB_NAME)
  cursor = conn.cursor()
  # Price tracking table
  cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            price REAL NOT NULL,
            rolling_avg REAL NOT NULL
        )
    """)
  # Alert history log table
  cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            message TEXT NOT NULL
        )
    """)
  conn.commit()
  conn.close()


# Background worker thread to handle SQLite writes safely without locking
def database_worker():
  conn = sqlite3.connect(DB_NAME)
  cursor = conn.cursor()
  while True:
    task = write_queue.get()
    if task is None:
      break
    task_type, data = task
    if task_type == "price":
      asset, timestamp, price, rolling_avg = data
      cursor.execute(
          "INSERT INTO asset_prices (asset, timestamp, price, rolling_avg)"
          " VALUES (?, ?, ?, ?)",
          (asset, timestamp, price, rolling_avg),
      )
    elif task_type == "alert":
      asset, timestamp, message = data
      cursor.execute(
          "INSERT INTO alerts (asset, timestamp, message) VALUES (?, ?, ?)",
          (asset, timestamp, message),
      )
    conn.commit()
    write_queue.task_done()


# Sliding window dictionaries for multiple assets
price_windows = defaultdict(lambda: deque(maxlen=50))


def on_message(ws, message):
  data = json.loads(message)
  if "price" in data and "product_id" in data:
    asset = data["product_id"]
    price = float(data["price"])
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    window = price_windows[asset]
    window.append(price)
    rolling_avg = sum(window) / len(window)

    print(
        f"[{asset}] Price: ${price:,.2f} | Rolling Avg: ${rolling_avg:,.2f}"
    )

    # Push to thread-safe queue instead of direct writing
    write_queue.put(("price", (asset, timestamp, price, rolling_avg)))

    # Check for volatility threshold cross (0.5% deviation)
    if abs(price - rolling_avg) > (0.005 * rolling_avg):
      alert_msg = (
          f"Volatility spike detected for {asset}! Price: ${price:,.2f}"
      )
      write_queue.put(("alert", (asset, timestamp, alert_msg)))


def on_error(ws, error):
  print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
  print("WebSocket closed. Reconnecting...")


def on_open(ws):
  print("Connected to Coinbase WebSocket. Subscribing to multiple assets...")
  subscribe_msg = {
      "type": "subscribe",
      "channels": [
          {"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD"]}
      ],
  }
  ws.send(json.dumps(subscribe_msg))


def run_websocket():
  init_db()
  # Start database worker thread
  db_thread = threading.Thread(target=database_worker, daemon=True)
  db_thread.start()

  while True:
    try:
      ws_url = "wss://ws-feed.exchange.coinbase.com"
      ws = websocket.WebSocketApp(
          ws_url,
          on_open=on_open,
          on_message=on_message,
          on_error=on_error,
          on_close=on_close,
      )
      ws.run_forever()
    except Exception as e:
      print(f"Connection dropped: {e}. Retrying in 5 seconds...")


if __name__ == "__main__":
  t = threading.Thread(target=run_websocket, daemon=True)
  t.start()
  import time

  while True:
    time.sleep(1)