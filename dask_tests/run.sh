#!/bin/bash
. $HOME/mambaforge/etc/profile.d/conda.sh
conda activate dask
export HDF5_USE_FILE_LOCKING=FALSE
$(which time) -v python test.py "$@"
