from dask.distributed import LocalCluster, Client

def start_dask():
    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=5,
        memory_limit="1GB",
        dashboard_address=":8787"
    )
    return Client(cluster)
#local cluster lets say developer to develop contast,test and proof destributed computing locally before developing 2 real cluster
#client is the interface to interact with the cluster (connection) 
#threads per worker means every chunk per 5 worker will be processed by 5 threads 
