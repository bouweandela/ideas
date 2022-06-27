import sys
from pathlib import Path

import dask.distributed
import dask_jobqueue
import dask
import iris
import xarray

def compute_example(in_file, out_file):
    cube = iris.load_cube(in_file)
    cube = cube.collapsed('time', iris.analysis.MEAN)
    #result = iris.save(cube, out_file, compute=False)
    ds = xarray.DataArray.from_iris(cube)
    result = ds.to_netcdf(out_file, compute=False)
    return result

def main(in_files):
    # start a dask cluster, e.g.
    #cluster = dask.distributed.LocalCluster(processes=False)
    # or when on Jasmin/Levante/etc:
    cluster = dask_jobqueue.SLURMCluster(cores=8, memory='24GB')
    # or a even cluster in the cloud hopefully at some point
    client = dask.distributed.Client(cluster)

    results = []
    for i, in_file in enumerate(in_files):
        out_file = Path(f'result{i}.nc')
        result = compute_example(in_file, out_file)
        results.append(result)

    dask.compute(*results)

if __name__ == '__main__':
    main(sys.argv[1:])
