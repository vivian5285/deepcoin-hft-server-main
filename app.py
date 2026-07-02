#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, threading, json, logging
from flask import Flask, request, jsonify
from position_supervisor_deepcoin import position_supervisor

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] Flask: %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json() if request.is_json else (json.loads(request.get_data(as_text=True)) if request.get_data(as_text=True) else {})
    except Exception as e:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    if not data: return jsonify({"status": "error", "message": "Empty payload"}), 400
    if str(data.get("secret", "")).strip() != os.getenv("WEBHOOK_SECRET", "528586"): return jsonify({"status": "error", "message": "Invalid secret"}), 403

    raw_action = data.get("action", "UNKNOWN")
    reason = data.get("reason", "策略安全换防")
    
    if "CLOSE_PROTECT" in raw_action:
        logger.info(f"[Webhook] 📥 收到信号 → 【保护性全平】 | 原因: {reason} | Regime: {data.get('regime', 'N/A')}")
    else:
        logger.info(f"[Webhook] 📥 收到信号 → 【{raw_action}】 | Regime: {data.get('regime', 'N/A')}")

    threading.Thread(target=position_supervisor.handle_signal, args=(data,), daemon=True).start()
    return jsonify({"status": "success", "message": "Signal processing started", "action": raw_action}), 200

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "service": "deepcoin_webhook", "version": "v13.4.8-tp-radar-dingtalk"}), 200

if __name__ == '__main__':
    host_ip = os.getenv("FLASK_HOST", "0.0.0.0")
    port_num = int(os.getenv("FLASK_PORT", 5004))
    logger.info(f"🚀 深币 Webhook 服务启动 -> {host_ip}:{port_num}")
    app.run(host=host_ip, port=port_num, debug=False, threaded=True)
