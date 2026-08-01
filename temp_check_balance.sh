#!/bin/bash
cd ~/deepcoin-hft-server
source venv/bin/activate
python3 -c "
from deepcoin_client import deepcoin_client
import json
print(json.dumps(deepcoin_client.get_account_summary(), indent=2))
"
