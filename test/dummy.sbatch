#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=3850
#SBATCH --gres=gpu:RTX6000:1
#SBATCH --partition=gpu
#SBATCH --time=72:00:00
#SBATCH --account=xx111

module purge