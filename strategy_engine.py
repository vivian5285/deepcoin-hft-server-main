# strategy_engine.py — 策略下单示例（对齐官方 REST 接口）
import requests
from deepcoin_client import deepcoin_client

DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN"

def send_dingtalk(msg):
    requests.post(DINGTALK_WEBHOOK, json={"msgtype": "text", "text": {"content": f"【战神监控】{msg}"}})

def execute_order(symbol, side, price, amount):
    pos_side = "long" if side.upper() in ("LONG", "BUY") else "short"
    order_side = "buy" if pos_side == "long" else "sell"

    res = deepcoin_client.get_position_info(symbol)
    if res and res.get("data"):
        for p in res["data"]:
            if int(p.get("pos", 0)) > 0 and pos_side == p.get("posSide", "").lower():
                return "已有持仓，忽略重复开单"

    res = deepcoin_client.place_limit_order(symbol, order_side, pos_side, price, amount)
    send_dingtalk(f"执行下单: {side} {amount} 张, 价格: {price}")
    return res
