import sys
sys.path.insert(0, ".")
from deepcoin_client import DeepcoinClient
c = DeepcoinClient()
s = c.get_account_summary("USDT")
print("Account Summary:", s)
print("Principal Wallet Balance:", c.get_principal_wallet_balance("USDT"))
