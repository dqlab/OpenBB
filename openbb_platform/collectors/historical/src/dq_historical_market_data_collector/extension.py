"""Historical collection engine registration."""

from openbb_collector_core import CollectorExtension

from .config import load_config
from .runtime import Collector

collector = CollectorExtension(name="historical", load_config=load_config, factory=Collector)
