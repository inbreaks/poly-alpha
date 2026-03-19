from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _install_duckdb_stub() -> None:
    if "duckdb" in sys.modules:
        return

    duckdb = types.ModuleType("duckdb")

    def _connect(*args, **kwargs):  # pragma: no cover - 测试里不允许真的连库
        raise RuntimeError("duckdb.connect should not be called in unit tests")

    duckdb.connect = _connect  # type: ignore[attr-defined]
    sys.modules["duckdb"] = duckdb


def _install_ccxt_stub() -> None:
    if "ccxt" in sys.modules:
        return

    ccxt = types.ModuleType("ccxt")

    class _BaseError(Exception):
        pass

    class _NetworkError(_BaseError):
        pass

    for name, value in {
        "BaseError": _BaseError,
        "DDoSProtection": _NetworkError,
        "ExchangeNotAvailable": _NetworkError,
        "NetworkError": _NetworkError,
        "RateLimitExceeded": _NetworkError,
        "RequestTimeout": _NetworkError,
    }.items():
        setattr(ccxt, name, value)

    def _binanceusdm(*args, **kwargs):  # pragma: no cover - 测试里不发起网络请求
        raise RuntimeError("ccxt.binanceusdm should not be called in unit tests")

    ccxt.binanceusdm = _binanceusdm  # type: ignore[attr-defined]
    sys.modules["ccxt"] = ccxt


def load_script_module(module_name: str, relative_path: str):
    _install_duckdb_stub()
    _install_ccxt_stub()

    target_path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, target_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load module from {target_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

