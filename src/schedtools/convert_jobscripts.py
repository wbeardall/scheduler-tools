import argparse
import os

from schedtools.job_script import convert_to_pbs, convert_to_slurm

def conversion_helper(path,recursive, to, updates = {}):
    path = os.path.abspath(path)
    if os.path.isfile(path):
        base, ext = os.path.splitext(path)
        if ext ==".pbs" and to == "slurm":
            convert_to_slurm(path, base + ".sbatch", updates=updates)
        elif ext==".sbatch" and to == "pbs":
            convert_to_pbs(path, base + ".pbs", updates=updates)
    elif os.path.isdir(path) and recursive:
        for file in os.listdir(path):
            conversion_helper(os.path.join(path,file), recursive=recursive, to=to)

def convert_jobscripts():
    """Convert jobscripts from PBS format to SLURM format and vice versa."""
    parser = argparse.ArgumentParser(description=convert_jobscripts.__doc__)
    parser.add_argument("format",type=str,
        choices=["pbs", "slurm"],
        help="Format to convert jobscripts to.")
    parser.add_argument("path", default=os.getcwd(),
        help="Path of jobscript or dir containing jobscripts to convert.")
    parser.add_argument("-r","--recursive",action="store_true",
        help="Convert recursively.")
    parser.add_argument("-u","--updates", nargs="*",
        help="Any updates to include when converting jobscripts. For example, changes to job duration or cluster account.")

    args = parser.parse_args()

    updates = {}
    if args.updates:
        for pair in args.updates:
            key, value = pair.split('=')
            updates[key] = value
    if os.path.isdir(args.path):
        for file in os.listdir(args.path):
            conversion_helper(os.path.join(args.path,file), args.recursive, args.format, updates=updates)
    else:
        conversion_helper(args.path, args.recursive, args.format, updates=updates)
    

if __name__=="__main__":
    convert_jobscripts()