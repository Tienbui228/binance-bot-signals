from __future__ import annotations

import unittest

from binance_bot.domain.case_registry import CaseRegistry
from binance_bot.domain.enums import CaseCloseType, EvidenceReadiness, StageContentType, StageName, StageStatus


class ReviewSemanticArchetypeTests(unittest.TestCase):
    def test_pending_not_due_is_partial_not_lifecycle_complete(self):
        registry = CaseRegistry()
        case = registry.create_case(
            case_id='case-1',
            case_day='2026-03-28',
            symbol='BTCUSDT',
            side='LONG',
            strategy='legacy_5m_retest',
        )
        registry.upsert_stage('case-1', StageName.PRE_PENDING, StageStatus.CAPTURED, StageContentType.CHART_SNAPSHOT)
        registry.upsert_stage('case-1', StageName.PENDING_OPEN, StageStatus.CAPTURED, StageContentType.CHART_SNAPSHOT)
        case.case_close_type = CaseCloseType.NOT_DUE_YET
        registry.recompute(case)
        self.assertEqual(case.evidence_ready_for_review, EvidenceReadiness.PARTIAL)
        self.assertFalse(case.lifecycle_complete)


if __name__ == '__main__':
    unittest.main()
