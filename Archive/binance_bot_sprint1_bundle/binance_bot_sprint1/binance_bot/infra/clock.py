from __future__ import annotations

import time
from datetime import datetime, timezone


class Clock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)

    def utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()
