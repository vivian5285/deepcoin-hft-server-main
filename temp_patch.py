#!/usr/bin/env python3
"""Patch position_supervisor_deepcoin.py to add None guards."""
import re

filepath = "/home/deepcoin/deepcoin-hft-server/position_supervisor_deepcoin.py"

with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

# Patch 1: _enforce_tv_direction_or_flat - add None guard at start
# Find the function and add None check after the docstring
old_enforce = '''    def _enforce_tv_direction_or_flat(self, pos, source="", expected_side=None,
                                verify_note=""):
        """
        Returns True if the position was closed (flat), False otherwise.
        "tv_opposite" means TV says CLOSE → close and return True.
        If TV says OPEN and direction aligns with live position → skip.
        '''
        if pos is None:
            return False'''

new_enforce = '''    def _enforce_tv_direction_or_flat(self, pos, source="", expected_side=None,
                                verify_note=""):
        """
        Returns True if the position was closed (flat), False otherwise.
        "tv_opposite" means TV says CLOSE → close and return True.
        If TV says OPEN and direction aligns with live position → skip.
        '''
        # v16.23: prevent NoneType crash if pos passed as None
        if pos is None:
            return False'''

if old_enforce in content:
    content = content.replace(old_enforce, new_enforce)
    print("Patch 1 (_enforce_tv_direction_or_flat None guard): APPLIED")
else:
    print("Patch 1: NOT FOUND - checking for existing guard...")
    if "if pos is None:" in content and "_enforce_tv_direction_or_flat" in content:
        print("Patch 1: None guard already exists")
    else:
        print("Patch 1: WARNING - could not verify")

# Patch 2: _perform_live_takeover - ensure explicit None guard before any pos.get()
old_takeover_start = '''    def _perform_live_takeover(self, pos, source="巡检", manual_open=False, qty_change=None):
        """
        实盘有仓但 VPS 未监控 / 防线缺失 → 补挂 TP123+硬止损，启动雷达哨兵。
        """
        # v16.22 修复：防止 pos=None 导致崩溃（_recover_missed_flat_on_startup 可能在持仓已平时仍调用）
        if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
            logger.warning(f"⚠️ _perform_live_takeover 跳过：无实盘持仓 | source={source}")
            return False'''

new_takeover_start = '''    def _perform_live_takeover(self, pos, source="巡检", manual_open=False, qty_change=None):
        """
        实盘有仓但 VPS 未监控 / 防线缺失 → 补挂 TP123+硬止损，启动雷达哨兵。
        """
        # v16.22+ 修复：双重 None 保护，防止任何形式的 pos=None 导致崩溃
        if pos is None:
            logger.warning(f"⚠️ _perform_live_takeover 跳过：pos=None | source={source}")
            return False
        if not pos or self._safe_qty(pos.get("size", 0)) <= 0:
            logger.warning(f"⚠️ _perform_live_takeover 跳过：无实盘持仓 | source={source}")
            return False'''

if old_takeover_start in content:
    content = content.replace(old_takeover_start, new_takeover_start)
    print("Patch 2 (_perform_live_takeover explicit None guard): APPLIED")
else:
    print("Patch 2: NOT FOUND - checking for existing double guard...")
    if "if pos is None:" in content and "_perform_live_takeover" in content:
        count = content.count("if pos is None:")
        print(f"Patch 2: Found {count} 'if pos is None:' guards")

with open(filepath, "w", encoding="utf-8") as f:
    f.write(content)

print("Patching complete.")
