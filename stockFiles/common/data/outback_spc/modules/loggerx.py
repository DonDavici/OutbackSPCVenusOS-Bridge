# -*- coding: utf-8 -*-
"""
Kleines Logging-Framework:
- kompaktes Textformat wie gefordert
- Ratenlimit & Dedupe
- wahlweise JSON-Ausgabe
- periodische Summenzeile
"""

import logging
import sys
import time
from dataclasses import dataclass

_START = time.monotonic()


def _elapsed():
    return time.monotonic() - _START


_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARN,
    "WARNING": logging.WARN,
    "ERROR": logging.ERROR,
}


class RateLimiter:
    def __init__(self, ms: int = 500):
        self.ms = ms
        self._last_msg = None
        self._last_t = 0.0

    def allow(self, msg: str) -> bool:
        now = time.monotonic()
        if msg != self._last_msg:
            self._last_msg = msg
            self._last_t = now
            return True
        if (now - self._last_t) * 1000.0 >= self.ms:
            self._last_t = now
            return True
        return False


class LoggerX:
    def __init__(self, tag: str, level: str = "INFO", fmt: str = "text", rate_limit_ms: int = 500):
        self.tag = (tag or "LOG")[:5].ljust(5)
        self.level = _LEVELS.get(level.upper(), logging.INFO)
        self.fmt = fmt
        self.rl = RateLimiter(rate_limit_ms)

    def _emit(self, lvl_name: str, text: str):
        if _LEVELS.get(lvl_name, 999) < self.level:
            return
        if not self.rl.allow(f"{lvl_name}:{text}"):
            return
        t = _elapsed()
        if self.fmt == "json":
            import json
            obj = {"t_rel_s": round(t, 3), "module": self.tag.strip(), "level": lvl_name, "text": text}
            sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
        else:
            sys.stdout.write(f"[T+{t:.3f}s] {self.tag} {lvl_name:<5} {text}\n")
        sys.stdout.flush()

    def debug(self, text: str): self._emit("DEBUG", text)
    def info(self, text: str): self._emit("INFO", text)
    def warn(self, text: str): self._emit("WARN", text)
    def warning(self, text: str): self._emit("WARN", text)
    def error(self, text: str): self._emit("ERROR", text)


def make_logger(tag: str, level: str = "INFO", fmt: str = "text", rate_limit_ms: int = 500) -> LoggerX:
    return LoggerX(tag, level, fmt, rate_limit_ms)


@dataclass
class Summary:
    """Einfache periodische Summenzeilen-Ausgabe."""
    period_s: int = 5
    _last: float = 0.0

    def due(self) -> bool:
        now = time.monotonic()
        if now - self._last >= self.period_s:
            self._last = now
            return True
        return False

    def emit(self, text: str):
        t = _elapsed()
        sys.stdout.write(f"[T+{t:.1f}s] SUM  INFO  {text}\n")
        sys.stdout.flush()
