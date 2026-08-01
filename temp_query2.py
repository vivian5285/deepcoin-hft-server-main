import sys, os, json
sys.path.insert(0, '/home/deepcoin/deepcoin-hft-server')
os.chdir('/home/deepcoin/deepcoin-hft-server')
from deepcoin_client import DeepcoinClient
dc = DeepcoinClient()
print("=" * 60)
for sym in ['ETH-USDT-SWAP', 'BNB-USDT-SWAP']:
    pos = dc.get_position_info(sym)
    data = pos.get('data', []) if isinstance(pos, dict) else []
    if data:
        p = data[0]
        print(f"\n{sym}: {p.get('posSide','?').upper()} {p.get('pos','0')}张 @ {p.get('avgPx','?')}")
    else:
        print(f"\n{sym}: 无持仓")
    orders = dc.get_pending_orders(sym)
    print(f"  Pending orders ({len(orders)}):")
    for o in orders:
        print(f"    {o.get('side','?')} {o.get('sz','?')}张 @{o.get('px','?')} {o.get('ordType','?')} reduceOnly={o.get('reduceOnly','?')} oid={o.get('ordId','?')}")
    px = dc.get_current_price(sym)
    print(f"  Price: {px}")
print("\n" + "=" * 60)
# Check BNB state file
sf = '/home/deepcoin/deepcoin-hft-server/deepcoin_vps_state_BNB_USDT_SWAP.json'
if os.path.exists(sf):
    with open(sf) as f:
        s = json.load(f)
    print(f"BNB state: monitoring={s.get('monitoring')} tv_sl={s.get('tv_sl')} tv_tps={s.get('tv_tps')} shield_active={s.get('shield_active')}")
else:
    print("BNB state file not found")
