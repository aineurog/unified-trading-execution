"""Extend the core package namespace with modules from the Binance adapter."""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
