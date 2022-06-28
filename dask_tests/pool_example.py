import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import iris
import xarray as xr


def compute_iris_example(in_file, out_file):
    cube = iris.load_cube(in_file)
    cube = cube.collapsed(["latitude", "longitude"], iris.analysis.MEAN)
    iris.save(cube, out_file)


def compute_xarray_example(in_file, out_file):
    ds = xr.open_dataset(in_file, chunks="auto")
    ds = ds.mean(dim=["lat", "lon"])
    ds.to_netcdf(out_file, format="NETCDF4", engine="netcdf4")


def main(in_files):

    # Run sequentially

    # for i, in_file in enumerate(in_files):
    #     out_file = Path(f"result{i}.nc")
    #     compute_example(in_file, out_file)

    # Run using a processpool
    with ProcessPoolExecutor() as executor:
        futures = {}
        for i, in_file in enumerate(in_files):
            out_file = Path(f"result{i}.nc")
            future = executor.submit(compute_example, in_file, out_file)
            futures[future] = in_file

        for future in as_completed(futures):
            print("Saved ", futures[future])
            future.result()


if __name__ == "__main__":
    main(sys.argv[1:])
