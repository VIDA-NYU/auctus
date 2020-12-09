import os
import prometheus_client

from .discovery import Discoverer, AsyncDiscoverer


__all__ = ['Discoverer', 'AsyncDiscoverer']


__version__ = '0.0'


PROM_VERSION = prometheus_client.Gauge('version', "Datamart version",
                                       ['version'])
PROM_VERSION.labels(os.environ['DATAMART_VERSION']).set(1)
