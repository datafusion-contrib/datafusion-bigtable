from ._private import BigtableTable

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata

__version__ = importlib_metadata.version(__name__)
__all__ = [
    "BigtableTable",
]
