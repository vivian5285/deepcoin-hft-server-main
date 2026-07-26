#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""规格 §5.1：TP1/TP2 绝对价激活（首次中点 / 重入 TP2）+ 最多 1 次重入。"""
from __future__ import annotations

import time
import unittest

from breath_profiles import BREATH_ETH, BREATH_XAU, get_breath_profile
from reentry_profiles import (
    ACTIVATION_MODE_FIRST,
    ACTIVATION_MODE_REENTRY,
    ARM_SL_ATR,
    HARD_SL_BUFFER_MULT,
    MAX_REENTRIES,
    activation_frac_for_attempt,
    activation_mode_for_attempt,
    activation_price,
    adx_to_tier,
    apply_tier_to_breath_profile,
    arm_stop_price,
    buffer_for_tier,
    can_smart_reenter,
    compute_reentry_limit_px,
    exit_in_reentry_zone,
    get_reentry_profile,
    is_better_than_tv,
    looser_tier,
    make_reentry_client_order_id,
    next_activation_frac,
    pick_dual_insurance,
    radar_gate_label,
    radar_gate_price_from_tps,
    reentry_enabled,
    reentry_window_sec,
    tier_coeffs,
    tier_label,
)
from smart_reentry_engine import (
    blank_reentry_state,
    bump_after_reentry_fill,
    init_cycle_on_open,
    plan_reentry_limit,
)


class TestAdxTiers(unittest.TestCase):
    def test_adx_bounds(self):
        self.assertEqual(adx_to_tier(19.9), 0)
        self.assertEqual(adx_to_tier(20.0), 1)
        self.assertEqual(adx_to_tier(30.0), 1)
        self.assertEqual(adx_to_tier(30.1), 2)

    def test_buffer_unified_115(self):
        self.assertAlmostEqual(HARD_SL_BUFFER_MULT, 1.15)
        self.assertAlmostEqual(buffer_for_tier(0), 1.15)

    def test_looser_tier(self):
        self.assertEqual(looser_tier(0), 1)
        self.assertEqual(looser_tier(1), 2)
        self.assertEqual(looser_tier(2), 2)


class TestActivationTp12Absolute(unittest.TestCase):
    """规格算例：tp1=1925.65 tp2=1955.00 → 首次中点 1940.325；重入=1955。"""

    TP1 = 1925.65
    TP2 = 1955.00
    MID = (1925.65 + 1955.00) / 2.0  # 1940.325

    def test_modes(self):
        self.assertEqual(activation_mode_for_attempt(0), ACTIVATION_MODE_FIRST)
        self.assertEqual(activation_mode_for_attempt(1), ACTIVATION_MODE_REENTRY)
        self.assertEqual(MAX_REENTRIES, 1)
        self.assertAlmostEqual(activation_frac_for_attempt(0), 0.0)
        self.assertAlmostEqual(activation_frac_for_attempt(1), 1.0)
        self.assertEqual(radar_gate_label(0), "TP1-TP2中点")
        self.assertEqual(radar_gate_label(1), "TP2绝对价")
        self.assertEqual(tier_label(2), "强趋势")

    def test_spec_numeric_first_mid(self):
        gate = radar_gate_price_from_tps(self.TP1, self.TP2, reentry_attempt=0)
        self.assertAlmostEqual(gate, 1940.325, places=3)

    def test_spec_numeric_reentry_tp2(self):
        gate = radar_gate_price_from_tps(self.TP1, self.TP2, reentry_attempt=1)
        self.assertAlmostEqual(gate, self.TP2, places=2)

    def test_same_tps_different_gates(self):
        first = radar_gate_price_from_tps(self.TP1, self.TP2, 0)
        reent = radar_gate_price_from_tps(self.TP1, self.TP2, 1)
        self.assertLess(first, reent)
        self.assertAlmostEqual(first, self.MID, places=3)
        self.assertAlmostEqual(reent, self.TP2, places=2)

    def test_reentry_past_mid_before_tp2_must_stay_dormant(self):
        """
        【关键边界】重入单：价格已过中点、尚未到 TP2 → 雷达不得介入。
        易漏写成「沿用首次中点门槛」。
        """
        mid = self.MID
        tp2 = self.TP2
        px = round((mid + tp2) / 2.0, 2)  # ~1947.66
        self.assertGreater(px, mid)
        self.assertLess(px, tp2)

        first_gate = radar_gate_price_from_tps(self.TP1, self.TP2, 0)
        reentry_gate = radar_gate_price_from_tps(self.TP1, self.TP2, 1)

        wrongly_armed_if_first_gate = px >= first_gate
        correctly_armed = px >= reentry_gate
        self.assertTrue(wrongly_armed_if_first_gate)
        self.assertFalse(correctly_armed)

        class _S:
            current_side = "LONG"
            watched_entry = 1890.0
            best_price = 0.0
            tv_tps = [
                TestActivationTp12Absolute.TP1,
                TestActivationTp12Absolute.TP2,
                1980.0,
            ]
            reentry_attempt = 1
            radar_activation_frac = 1.0

            def _radar_activation_price(self):
                return radar_gate_price_from_tps(
                    self.tv_tps[0], self.tv_tps[1], self.reentry_attempt,
                )

            def _price_reached_radar_activation(self, curr_px, live_only=True):
                act = self._radar_activation_price()
                if self.current_side == "LONG":
                    return float(curr_px) >= act
                return float(curr_px) <= act

        s = _S()
        self.assertFalse(
            s._price_reached_radar_activation(px),
            msg="重入过中点未到TP2时不得激活",
        )
        self.assertTrue(s._price_reached_radar_activation(tp2))
        s.reentry_attempt = 0
        s.radar_activation_frac = 0.0
        self.assertTrue(
            s._price_reached_radar_activation(px),
            msg="同一价位首次开仓应已过中点门槛",
        )

    def test_short_gate_uses_tps(self):
        tp1, tp2 = 1955.0, 1925.65
        mid = (tp1 + tp2) / 2.0
        self.assertAlmostEqual(radar_gate_price_from_tps(tp1, tp2, 0), mid, places=3)
        self.assertAlmostEqual(radar_gate_price_from_tps(tp1, tp2, 1), tp2, places=2)

    def test_legacy_atr_fallback_still_callable(self):
        atr, entry = 20.0, 3000.0
        self.assertAlmostEqual(
            activation_price("LONG", entry, atr, 0.85),
            entry + atr * 1.1475, places=2,
        )

    def test_arm_stop_half_atr(self):
        self.assertAlmostEqual(ARM_SL_ATR, 0.5)
        self.assertAlmostEqual(arm_stop_price("LONG", 3000, 20), 2990.0)
        self.assertAlmostEqual(arm_stop_price("SHORT", 3000, 20), 3010.0)


class TestTierCoeffs(unittest.TestCase):
    def test_eth_three_tiers(self):
        eth = get_reentry_profile("ETHUSDT")
        self.assertEqual(len(eth["tiers"]), 3)
        t0 = tier_coeffs(0, eth)
        t2 = tier_coeffs(2, eth)
        self.assertAlmostEqual(t0["step_trigger_atr"], 0.40)
        self.assertAlmostEqual(t2["step_trigger_atr"], 0.60)

    def test_xau_three_tiers(self):
        xau = get_reentry_profile("XAUUSDT")
        t1 = tier_coeffs(1, xau)
        self.assertAlmostEqual(t1["step_trigger_atr"], 0.40)

    def test_overlay_disables_early_be(self):
        out = apply_tier_to_breath_profile(
            dict(BREATH_ETH), 1, get_reentry_profile("ETHUSDT"),
        )
        self.assertAlmostEqual(out["early_be_atr"], 0.0)
        self.assertAlmostEqual(out["initial_sl_atr"], 0.5)

    def test_breath_baseline(self):
        eth = get_breath_profile("ETHUSDT")
        self.assertAlmostEqual(eth["initial_sl_atr"], 0.5)
        self.assertAlmostEqual(eth["early_be_atr"], 0.0)


class TestReentryGate(unittest.TestCase):
    def test_hard_sl_blocked(self):
        ok, why = can_smart_reenter(
            exit_source="vps_hard_sl", side="LONG", entry=3000,
            exit_px=3005, initial_atr=20, reentry_attempt=0,
        )
        self.assertFalse(ok)
        self.assertEqual(why, "hard_sl_no_reentry")

    def test_max_one_reentry(self):
        ok, why = can_smart_reenter(
            exit_source="radar_be", side="LONG", entry=3000,
            exit_px=3005, initial_atr=20, reentry_attempt=1,
        )
        self.assertFalse(ok)
        self.assertEqual(why, "max_reentries")

    def test_window_expired(self):
        ok, why = can_smart_reenter(
            exit_source="radar_be", side="LONG", entry=3000,
            exit_px=3005, initial_atr=20, reentry_attempt=0,
            window_deadline_ts=time.time() - 10,
        )
        self.assertFalse(ok)
        self.assertEqual(why, "window_expired")

    def test_zone_ok(self):
        self.assertTrue(exit_in_reentry_zone("LONG", 3000, 3005, 20, 0.5))
        ok, why = can_smart_reenter(
            exit_source="radar_be", side="LONG", entry=3000,
            exit_px=3005, initial_atr=20, reentry_attempt=0,
            window_deadline_ts=time.time() + 3600,
            tp1_ever_filled=False,
            adx_tier=2,
        )
        self.assertTrue(ok)

    def test_tp1_already_filled_blocks(self):
        ok, why = can_smart_reenter(
            exit_source="radar_be", side="LONG", entry=3000,
            exit_px=3005, initial_atr=20, reentry_attempt=0,
            window_deadline_ts=time.time() + 3600,
            tp1_ever_filled=True,
            adx_tier=2,
        )
        self.assertFalse(ok)
        self.assertEqual(why, "tp1_already_filled")

    def test_tier_not_strong_blocks(self):
        for tier in (0, 1):
            ok, why = can_smart_reenter(
                exit_source="radar_be", side="LONG", entry=3000,
                exit_px=3005, initial_atr=20, reentry_attempt=0,
                window_deadline_ts=time.time() + 3600,
                tp1_ever_filled=False,
                adx_tier=tier,
            )
            self.assertFalse(ok, msg=f"tier={tier}")
            self.assertEqual(why, "tier_not_strong")

    def test_window_bars(self):
        self.assertAlmostEqual(reentry_window_sec("ETHUSDT"), 10800)
        self.assertAlmostEqual(reentry_window_sec("XAUUSDT"), 8100)


class TestDualInsurance(unittest.TestCase):
    def test_pick_and_better(self):
        lim, src = pick_dual_insurance("LONG", 2980, 2991)
        self.assertAlmostEqual(lim, 2980)
        self.assertTrue(is_better_than_tv("LONG", lim, 3000))

    def test_must_beat_entry(self):
        lim, why = compute_reentry_limit_px(
            side="LONG", tv_price=3000, low5=3001, high5=3010,
            prev_entry=2990,
        )
        self.assertEqual(lim, 0)
        self.assertIn(why, ("not_better_than_tv", "not_better_than_entry"))


class TestEngine(unittest.TestCase):
    def test_bump_loosens_tier_and_reentry_mode(self):
        b = bump_after_reentry_fill(0, 0.0, "ETHUSDT", adx_tier=0)
        self.assertEqual(b["reentry_attempt"], 1)
        self.assertEqual(b["radar_tier"], 1)
        self.assertAlmostEqual(b["radar_activation_frac"], 1.0)

    def test_init_cycle_first_mode(self):
        st = init_cycle_on_open(
            side="LONG", tv_price=3000, entry=3001, open_atr=20,
            symbol="ETHUSDT", adx_tier=2,
        )
        self.assertEqual(st["adx_tier"], 2)
        self.assertAlmostEqual(st["radar_activation_frac"], 0.0)
        self.assertTrue(st["radar_pending_arm"])

    def test_next_frac(self):
        self.assertAlmostEqual(next_activation_frac(0.0, 1), 1.0)

    def test_blank_and_tag(self):
        blank = blank_reentry_state()
        self.assertIn("adx_tier", blank)
        tag = make_reentry_client_order_id("ETHUSDT", "LONG", 2990.5)
        self.assertTrue(tag.startswith("RE"))

    def test_plan_limit(self):
        plan, why = plan_reentry_limit(
            side="LONG", tv_price=3000, symbol="ETHUSDT",
            klines_5m=[[0, 0, 3010, 2979, 0]],
            prev_entry=3000,
        )
        self.assertEqual(why, "ok")
        self.assertIsNotNone(plan)
        self.assertLess(plan["limit_px"], 3000)

    def test_enabled(self):
        self.assertTrue(reentry_enabled("ETHUSDT"))
        self.assertTrue(reentry_enabled("XAUUSDT"))


if __name__ == "__main__":
    unittest.main()
