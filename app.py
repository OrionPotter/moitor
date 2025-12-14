# app.py
import os
import datetime
import threading
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS

from data_fetcher import get_portfolio_data

# ================= 初始化 =================

app = Flask(__name__)
CORS(app)

# ================= K线自动更新逻辑 =================

def should_auto_update():
    try:
        from kline_manager import get_latest_kline_date
        from db import get_enabled_monitor_stocks
        from datetime import datetime, timedelta

        monitor_stocks = get_enabled_monitor_stocks()
        if not monitor_stocks:
            return False, "没有配置监控股票"

        latest_dates = []
        for stock in monitor_stocks:
            latest = get_latest_kline_date(stock[1])
            if latest:
                latest_dates.append(latest)

        if not latest_dates:
            return True, "未发现K线数据，需初始化"

        latest_dt = datetime.strptime(max(latest_dates), "%Y-%m-%d")
        now = datetime.now()
        hours = (now - latest_dt).total_seconds() / 3600

        if hours >= 24:
            return True, f"距离上次更新 {hours:.1f} 小时"
        if 9 <= now.hour <= 14 and latest_dt.date() < now.date():
            return True, "交易时段需更新今日数据"

        return False, f"{hours:.1f} 小时内已更新"

    except Exception as e:
        print(f"[{datetime.datetime.now():%H:%M:%S}] ❌ 更新判断失败: {e}")
        return False, "判断失败"


def auto_update_kline_data():
    try:
        print(f"[{datetime.datetime.now():%H:%M:%S}] 🔍 检查K线更新条件")
        need, reason = should_auto_update()

        if not need:
            print(f"[{datetime.datetime.now():%H:%M:%S}] ⏭ {reason}")
            return

        print(f"[{datetime.datetime.now():%H:%M:%S}] 🚀 {reason}，开始更新")
        from kline_manager import batch_update_kline_data
        batch_update_kline_data(force_update=False, max_workers=2)
        print(f"[{datetime.datetime.now():%H:%M:%S}] ✅ K线更新完成")

    except Exception as e:
        print(f"[{datetime.datetime.now():%H:%M:%S}] ❌ 自动更新异常: {e}")


def start_kline_update_thread():
    if os.getenv("AUTO_UPDATE_KLINE", "true").lower() != "true":
        print("⚠️ 已禁用自动K线更新")
        return

    t = threading.Thread(target=auto_update_kline_data, daemon=True)
    t.start()
    print("🧵 K线更新后台线程已启动")

# ================= 页面路由 =================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/admin")
def admin():
    return render_template("admin.html")


@app.route("/monitor")
def monitor():
    return render_template("monitor.html")

# ================= API =================

@app.route("/api/portfolio")
def api_portfolio():
    rows, summary = get_portfolio_data()
    return jsonify({
        "status": "success",
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows": rows,
        "summary": summary
    })

# ---------- 监控数据 ----------

@app.route("/api/monitor")
def api_monitor():
    try:
        from data_fetcher import get_monitor_data
        from db import get_monitor_stock_by_code

        stocks = get_monitor_data()
        result = []

        for stock in stocks:
            conf = get_monitor_stock_by_code(stock["code"])
            pe_min = conf[4] if conf and conf[4] else 15
            pe_max = conf[5] if conf and conf[5] else 20

            eps = stock.get("eps_forecast")
            stock["reasonable_pe_min"] = pe_min
            stock["reasonable_pe_max"] = pe_max
            stock["reasonable_price"] = round(eps * pe_min, 2) if eps else None

            result.append(stock)

        return jsonify({
            "status": "success",
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "stocks": result
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": str(e),
            "stocks": []
        })

# ---------- 手动K线更新 ----------

@app.route("/api/update-kline", methods=["POST"])
def api_update_kline():
    from kline_manager import batch_update_kline_data
    force = (request.get_json() or {}).get("force_update", False)

    def task():
        batch_update_kline_data(force_update=force, max_workers=3)

    threading.Thread(target=task, daemon=True).start()
    return jsonify({"status": "success", "message": "K线更新任务已启动"})

# ================= 启动 =================

if __name__ == "__main__":
    print("🚀 Flask 启动中：http://localhost:5000")

    # 避免 debug 模式下线程启动两次
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        start_kline_update_thread()

    app.run(host="0.0.0.0", port=5000, debug=True)
