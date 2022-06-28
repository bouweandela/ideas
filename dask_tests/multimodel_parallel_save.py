import os
import sys

import dask
import dask.array as da
import iris
from netCDF4 import Dataset

from esmvalcore.preprocessor import multi_model_statistics, regrid


def save(cube, target, compute):
    """Save the data from a 3D cube to file using da.store."""
    dataset = Dataset(target, "w")
    dataset.createDimension("time", cube.shape[0])
    dataset.createDimension("lat", cube.shape[1])
    dataset.createDimension("lon", cube.shape[2])
    dataset.createVariable(
        "var",
        "f4",
        (
            "time",
            "lat",
            "lon",
        ),
    )

    return da.store(cube.core_data(), dataset["var"], compute=compute)


def store(cubes, out_filenames):
    """Save the data from a list of 3D cubes to a list of files using da.store."""
    targets = []
    data = []
    for cube, filename in zip(cubes, out_filenames):

        dataset = Dataset(filename, "w")
        dataset.createDimension("time", cube.shape[0])
        dataset.createDimension("lat", cube.shape[1])
        dataset.createDimension("lon", cube.shape[2])
        dataset.createVariable(
            "var",
            "f4",
            (
                "time",
                "lat",
                "lon",
            ),
        )

        targets.append(dataset["var"])
        data.append(cube.lazy_data())

    da.store(data, targets)


def main(in_filenames):
    """Compute multi-model statistics over the input files."""
    target_grid = "1x1"
    cubes = {}
    for in_filename in in_filenames:
        cube = iris.load_cube(in_filename)
        cube = regrid(cube, target_grid, scheme="linear")
        out_filename = os.path.basename(in_filename)
        cubes[out_filename] = cube

    statistics = multi_model_statistics(cubes.values(), "overlap",
                                        ["mean", "std_dev"])
    for statistic, cube in statistics.items():
        out_filename = statistic + ".nc"
        cubes[out_filename] = cube

    results = []
    for out_filename, cube in cubes.items():
        result = save(cube, out_filename, compute=False)
        results.append(result)

    dask.compute(results)

    # store(cubes.values(), cubes.keys())

    # for out_filename, cube in cubes.items():
    #     iris.save(cube, out_filename)


if __name__ == "__main__":
    # This script takes a list of netCDF files containing 3D variables as arguments
    main(sys.argv[1:])
