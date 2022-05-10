import textwrap
import iris
import sys
import xarray as xr
from netCDF4 import Dataset
import dask
import dask.array as da
import os
from dask.distributed import Client, LocalCluster, performance_report
from pathlib import Path
from dask_jobqueue import SLURMCluster
from concurrent.futures import ProcessPoolExecutor, as_completed

def create_target(filename, shape):
    with Dataset(filename, "w", format="NETCDF4") as dataset:
        dataset.createDimension("time", shape[0])
        dataset.createDimension("lat", shape[1])
        dataset.createDimension("lon", shape[2])
        dataset.createVariable(
            "var",
            "f4",
            (
                "time",
                "lat",
                "lon",
            ),
        )

class Target:

    def __init__(self, filename, shape):
        create_target(filename, shape)
        self.filename = filename

    def __setitem__(self, key, value):
        with Dataset(self.filename, 'a') as ds:
            ds["var"][key] = value

def save(cube, target, compute):
    """Save the data from a 3D cube to file using da.store."""
    target = Target(target, cube.shape)
    return da.store(cube.lazy_data(), target, compute=compute)



def print_layers(graph):
    txt  = textwrap.indent("\n".join(f"{layer}: {graph.layers[layer]}" for layer in graph.layers), prefix="  ")
    return txt

def main(test, *filenames):

    cluster = LocalCluster(processes=False)
    client = Client(cluster)
    results = []
    for i, filename in enumerate(filenames):
        target = Path(f'/var/scratch/bandela/tmp/test{i}.nc')
        print(f"Will write {target}", flush=True)
        target.unlink(missing_ok=True)
        if test == 'iris':
            cube = iris.load_cube(filename)
            print(cube.lazy_data().chunks)
            #iris.save(cube, target)
            res = save(cube, target, False)
            graph = res.__dask_graph__()
            print(pprint.pformat(graph))
            results.append(res)
        else:
            ds = xr.open_dataset(filename, chunks={'time': 495})
            res = ds.to_netcdf(target, format="NETCDF4", engine="netcdf4", compute=False)
            results.append(res)
    print(results)
    x = dask.compute(results)
    print(x)


def compute_example(filename, target):
    print(f"Will write {target}", flush=True)
    target.unlink(missing_ok=True)
    cube = iris.load_cube(filename)
    cube = cube.collapsed('time', iris.analysis.MEAN)
    iris.save(cube, target)

def copy_file(filename, target):
    print(f"Will write {target}", flush=True)
    target.unlink(missing_ok=True)
    cube = iris.load_cube(filename)
    iris.save(cube, target)

def processpool(func, *filenames):
    with ProcessPoolExecutor() as executor:
        futures = {}
        for i, filename in enumerate(filenames):
            target = Path(f'/var/scratch/bandela/tmp/test{i}.nc')
            future = executor.submit(func, filename, target)
            futures[future] = filename

        for future in as_completed(futures):
            print("Done with ", futures[future])
            future.result()

def one_by_one(func, *filenames):
    for i, filename in enumerate(filenames):
        target = Path(f'/var/scratch/bandela/tmp/test{i}.nc')
        func(filename, target)

if __name__ == '__main__':

    # cluster = SLURMCluster(
    #     header_skip=['--mem'],
    #     cores=8,
    #     processes=1,
    #     memory="64GB",
    #     local_directory="/local/bandela/dask",
    #     interface='eth4',
    #     walltime="0:10:00",
    #     queue="defq",
    # )
    # cluster.scale(2)
    #print(client, client.dashboard_link, flush=True)

    #with performance_report(filename="dask-report.html"):
    func = vars()[sys.argv[1]]
    filenames = sys.argv[2:]
    processpool(func, *filenames)
    # one_by_one(func, *filenames)