from distributed import LocalCluster
import logging


_local_cluster = None


def local_dask_cluster():
    """Get a local Dask cluster.

    You should consider managing a cluster yourself, and passing the client to
    the functions that make use of it.
    """
    global _local_cluster

    if _local_cluster is None:
        logging.warning("Creating local dask cluster")
        _local_cluster = LocalCluster()

    logging.info("Using local dask cluster")

    return _local_cluster
