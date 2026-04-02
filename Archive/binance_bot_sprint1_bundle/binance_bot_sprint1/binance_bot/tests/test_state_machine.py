from __future__ import annotations

import unittest

from binance_bot.domain.case_registry import CaseRegistry
from binance_bot.domain.state_machine import CaseEvent, CaseStateMachine, InvalidTransitionError


class CaseStateMachineTests(unittest.TestCase):
    def setUp(self):
        self.registry = CaseRegistry()
        self.case = self.registry.create_case(
            case_id='case-1',
            case_day='2026-03-28',
            symbol='BTCUSDT',
            side='LONG',
            strategy='legacy_5m_retest',
            created_time_local='2026-03-28T10:00:00+07:00',
        )
        self.sm = CaseStateMachine()

    def test_confirm_then_send(self):
        self.sm.apply(self.case, CaseEvent('pending_created', '2026-03-28T10:00:00+07:00'))
        self.sm.apply(self.case, CaseEvent('pending_confirmed', '2026-03-28T10:05:00+07:00'))
        self.sm.apply(self.case, CaseEvent('signal_sent', '2026-03-28T10:06:00+07:00'))
        self.assertTrue(self.case.is_confirmed)
        self.assertTrue(self.case.is_sent_signal)

    def test_send_before_confirm_rejected(self):
        self.sm.apply(self.case, CaseEvent('pending_created', '2026-03-28T10:00:00+07:00'))
        with self.assertRaises(InvalidTransitionError):
            self.sm.apply(self.case, CaseEvent('signal_sent', '2026-03-28T10:01:00+07:00'))


if __name__ == '__main__':
    unittest.main()
