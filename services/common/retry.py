"""Retry with exponential backoff and jitter (ADR-16).

During a failover or a coordinator restart, storage answers 503 for a short while. If every
caller retried immediately (or at a fixed interval), they would all hit the recovering
component at the same moments and slow its recovery (a "retry storm"). So each retry waits
about twice as long as the previous one, and the wait is randomised (jitter) so callers
spread out instead of retrying in lockstep.

    attempt:   1      2      3      4      5
    wait up to 0.2 s  0.4 s  0.8 s  1.6 s  2.0 s (capped)   -> gives up after ~5 s in total
"""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from services.common.config import env_float, env_int
from services.common.errors import Unavailable

log = logging.getLogger("retry")
T = TypeVar("T")

MAX_RETRIES = env_int("RETRY_MAX", 5)
BASE_DELAY_S = env_float("RETRY_BASE_S", 0.2)
MAX_DELAY_S = env_float("RETRY_CAP_S", 2.0)


def backoff_delay(attempt: int, base: float = BASE_DELAY_S, cap: float = MAX_DELAY_S,
                  rand: Callable[[], float] = random.random) -> float:
    """Delay before retry number `attempt` (1-based): exponential, capped, with "equal jitter"
    (half fixed, half random), so the delay is never near zero and never synchronised."""
    ceiling = min(cap, base * 2 ** (attempt - 1))
    return ceiling / 2 + rand() * ceiling / 2


async def retry_unavailable(call: Callable[[], Awaitable[T]], what: str, max_retries: int = MAX_RETRIES) -> T:
    """Run `call`; on Unavailable (503 / cannot connect) retry with backoff, then give up.
    Only Unavailable is retried: it means the operation was NOT applied, so retrying is safe.
    Conflicts, not-found and validation errors are answers, not outages, and are never retried."""
    for attempt in range(1, max_retries + 1):
        try:
            return await call()
        except Unavailable as exc:
            delay = backoff_delay(attempt)
            log.warning(f"{what} unavailable ({exc.code}); retry {attempt}/{max_retries} in {delay:.2f}s")
            await asyncio.sleep(delay)
    return await call()  # last try: let the error propagate to the caller (-> 503 + Retry-After)
