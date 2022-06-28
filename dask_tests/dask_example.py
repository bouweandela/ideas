import sys
from pathlib import Path

import dask
import dask.distributed
import dask_jobqueue
import iris
import xarray


def compute_iris_example(in_file, out_file):
    cube = iris.load_cube(in_file)
    cube = cube.collapsed(["latitude", "longitude"], iris.analysis.MEAN)
    # result = iris.save(cube, out_file, compute=False)
    ds = xarray.DataArray.from_iris(cube)
    result = ds.to_netcdf(out_file, compute=False)
    return result


def compute_xarray_example(in_file, out_file):
    ds = xarray.open_dataset(in_file, chunks="auto")
    ds = ds.mean(dim=["lat", "lon"])
    result = ds.to_netcdf(out_file, compute=False)
    return result


def main(in_files):
    # start a dask cluster, e.g.
    # cluster = dask.distributed.LocalCluster(processes=False)
    # or when on Jasmin/Levante/etc:
    cluster = dask_jobqueue.SLURMCluster(
        cores=8,
        memory="24GiB",
        header_skip=["--mem"],
        local_directory="/var/scratch/bandela/dask-tmp",
    )
    cluster.scale(1)
    # or a even cluster in the cloud hopefully at some point

    print(cluster.dashboard_link)
    client = dask.distributed.Client(cluster)

    results = []
    for i, in_file in enumerate(in_files):
        out_file = Path(f"result{i}.nc")
        result = compute_iris_example(in_file, out_file)
        results.append(result)

    dask.compute(results)


if __name__ == "__main__":
    main(sys.argv[1:])
