import os
import prometheus_client

from .common import Type
from .discovery import Discoverer, AsyncDiscoverer


__all__ = ['Type', 'Discoverer', 'AsyncDiscoverer']


__version__ = '0.0'


PROM_VERSION = prometheus_client.Gauge('version', "Datamart version",
                                       ['version'])
PROM_VERSION.labels(os.environ['DATAMART_VERSION']).set(1)
