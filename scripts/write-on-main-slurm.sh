#!/bin/zsh -l
#SBATCH -J mpipool-write
#SBATCH -o logs/log-write.o
#SBATCH -e logs/log-write.e
#SBATCH -N 1
#SBATCH -t 00:20:00
#SBATCH -p gen

cd /mnt/ceph/users/apricewhelan/projects/mpipool-2025
source .venv/bin/activate

mpirun python -m mpi4py.futures scripts/write-on-main-process.py
