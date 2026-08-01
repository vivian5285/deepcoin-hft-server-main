#!/usr/bin/env python3
import sys
sys.path.insert(0, '.')
import json
import os

os.chdir('/home/deepcoin/deepcoin-hft-server')

from deepcoin_client import DeepcoinClient
dc = DeepcoinClient()

print("=" * 60)
for sym in ["ETH-USDT-SWAP", "BNB-USDT-SWAP"]:
    pos = dc.get_all_positions(sym)
    print(f"\n### {sym}")
    print(json.dumps(pos, indent=2, ensure_ascii=False))
    orders = dc.get_open_orders(sym)
    print(f"  Open orders ({len(orders)}):")
    for o in orders:
        print(f"    {o.get('side','?')} {o.get('sz','?')}张 @{o.get('px','?')} {o.get('ordType','?')} reduceOnly={o.get('reduceOnly',False)} ordId={o.get('ordId','?')}")
    px = dc.get_current_price(sym)
    print(f"  Current price: {px}")

print("\n" + "=" * 60)

# Check state file
for sym in ["ETH-USDT-SWAP", "BNB-USDT-SWAP"]:
    state_file = f"logs/deepcoin_state_{sym.replace('-','_')}.json"
    if os.path.exists(state_file):
        with open(state_file) as f:
            s = json.load(f)
        tv_sl = s.get("tv_sl", 0)
        tv_tps = s.get("tv_tps", [])
        tv_price = s.get("tv_price", 0)
        current_sl = s.get("current_sl", 0)
        monitoring = s.get("monitoring", False)
        print(f"\n{sym} state: tv_sl={tv_sl}, tv_tps={tv_tps}, tv_price={tv_price}, current_sl={current_sl}, monitoring={monitoring}")
