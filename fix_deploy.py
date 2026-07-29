#!/usr/bin/env python3
import re

files = [
    '/home/deepcoin/deepcoin-hft-server/deploy_deepcoin.sh',
    '/home/deepcoin-b/deepcoin-hft-server/deploy_deepcoin.sh'
]

for path in files:
    with open(path, 'r') as f:
        content = f.read()
    
    # Update version string
    content = content.replace('v13.26-deploy-tp-radar-realign', 'v16.10-deploy-version-fix')
    
    # Fix MIN_SUPERVISOR_VERSION_RE to support v16.x
    old_pattern = "MIN_SUPERVISOR_VERSION_RE='v13\\.(4\\.[6-9]|(?:[5-9]|[1-9][0-9]+)\\.)'"
    new_pattern = "MIN_SUPERVISOR_VERSION_RE='v(13\\.(4\\.[6-9]|(?:[5-9]|[1-9][0-9]+)\\.)|16\\.)'"
    content = content.replace(old_pattern, new_pattern)
    
    with open(path, 'w') as f:
        f.write(content)
    
    print(f'Fixed {path}')
