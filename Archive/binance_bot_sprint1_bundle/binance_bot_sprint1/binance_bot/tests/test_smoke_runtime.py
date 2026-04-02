from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
SCANNER_PATH = ROOT / 'oi_scanner_sprint1_hotfix.py'


class ScannerSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, str(ROOT))
        spec = importlib.util.spec_from_file_location('oi_scanner_sprint1_hotfix', SCANNER_PATH)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        cls.BinanceScanner = module.BinanceScanner

    def test_required_methods_exist(self):
        required = (
            'save_signal',
            'save_pending',
            'sync_pending_send_decision',
            'close_pending',
            'scan_once',
            'process_pending_setups',
            'build_daily_review_pack',
        )
        for name in required:
            self.assertTrue(hasattr(self.BinanceScanner, name), name)


if __name__ == '__main__':
    unittest.main()
