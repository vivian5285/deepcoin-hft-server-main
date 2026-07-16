#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, threading, logging
from flask import Flask, request, jsonify
from position_supervisor_deepcoin import (
    get_supervisor_for_payload,
    SUPERVISORS,
    bootstrap_supervisors,
    DEEPCOIN_SUPERVISOR_VERSION,
)
from webhook_parser import parse_webhook_request, normalize_tv_payload, format_webhook_log, TV_STRATEGY_VERSION, EXCHANGE_LEVERAGE
from symbol_config import active_deepcoin_symbols

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] Flask: %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route('/webhook', methods=['POST'])
@app.route('/webhook/<path:ticker>', methods=['POST'])
def webhook(ticker=None):
    try:
        _, data = parse_webhook_request(
            request.get_data(),
            request.content_type or "",
            as_json=request.get_json(silent=True),
        )
        data = normalize_tv_payload(data)
    except ValueError as e:
        logger.warning(f"[Webhook] 解析失败: {e}")
        return jsonify({"status": "error", "message": str(e)}), 400

    if not data:
        return jsonify({"status": "error", "message": "Empty payload"}), 400
    if str(data.get("secret", "")).strip() != os.getenv("WEBHOOK_SECRET", "528586"):
        return jsonify({"status": "error", "message": "Invalid secret"}), 403
    if not data.get("_parse_ok"):
        return jsonify({"status": "error", "message": "Missing or invalid action"}), 400

    if ticker:
        data["ticker"] = ticker
        data["symbol"] = ticker

    raw_action = data.get("action", "UNKNOWN")
    if raw_action == "PING":
        return jsonify({
            "status": "success",
            "message": "pong",
            "action": "PING",
            "schema": TV_STRATEGY_VERSION,
            "symbols": active_deepcoin_symbols(),
        }), 200

    supervisor, sym = get_supervisor_for_payload(data)
    if supervisor is None:
        logger.warning(f"[Webhook] 不支持的品种: {sym}")
        return jsonify({
            "status": "error",
            "message": f"Unsupported symbol: {sym}",
            "allowed": active_deepcoin_symbols(),
        }), 400

    logger.info(f"[Webhook] [{sym}] {format_webhook_log(data)}")

    threading.Thread(
        target=supervisor.handle_signal, args=(data,), daemon=True,
        name=f"tv-{sym}",
    ).start()
    return jsonify({
        "status": "success",
        "message": "Signal processing started",
        "action": raw_action,
        "symbol": sym,
        "schema": TV_STRATEGY_VERSION,
    }), 200


@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok",
        "service": "deepcoin_webhook",
        "version": DEEPCOIN_SUPERVISOR_VERSION,
        "tv_strategy": TV_STRATEGY_VERSION,
        "leverage": EXCHANGE_LEVERAGE,
        "symbols": list(SUPERVISORS.keys()) or active_deepcoin_symbols(),
        "monitoring": {
            s: bool(getattr(sup, "monitoring", False))
            for s, sup in SUPERVISORS.items()
        },
    }), 200


if __name__ == '__main__':
    bootstrap_supervisors()
    host_ip = os.getenv("FLASK_HOST", "0.0.0.0")
    port_num = int(os.getenv("FLASK_PORT", 5004))
    logger.info(f"🚀 深币 Webhook 服务启动 -> {host_ip}:{port_num}")
    app.run(host=host_ip, port=port_num, debug=False, threaded=True)
