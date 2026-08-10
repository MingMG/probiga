"""Independent Trading V5 research package.

Submodules are intentionally not imported here.  The evidence-only CLI stays
stdlib-only, while model callers explicitly opt into the pinned NumPy/Pandas
research environment by importing :mod:`server.trading_v5.models`.
"""

__all__: list[str] = []
__version__ = "5.0.0-research"
