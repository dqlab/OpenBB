"""Live collection engine registration."""

from openbb_collector_core import CollectorExtension

from .config import load_config
from .service import Collector

collector = CollectorExtension(name="live", load_config=load_config, factory=Collector)
