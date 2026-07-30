#!/usr/bin/env python3
import sys
sys.path.insert(0, '/home/deepcoin/deepcoin-hft-server')
from deepcoin_client import deepcoin_client

orders = deepcoin_client.get_pending_orders('ETH-USDT-SWAP')
print(f'订单数量: {len(orders)}')
for o in orders[:5]:
    print(f'ordId: {o.get("ordId")}, px: {o.get("px")}, price: {o.get("price")}, sz: {o.get("sz")}')
