"""Direct HTTP providers; no imports from legacy collectors or schedulers."""

from .eastmoney import EastmoneyProvider
from .cninfo import CninfoProvider

__all__ = ["EastmoneyProvider", "CninfoProvider"]
