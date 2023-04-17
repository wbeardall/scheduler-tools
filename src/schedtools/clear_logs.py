import argparse
import os

import regex as re # We need expanded regex functionality for the `\K` (keep) regex char

def clear_cluster_logs(path: str, up_to: int, pattern: str = "pbs", recursive: bool=False,  force: bool = False):
    """Clear old cluster logs (stdout and stderr streams captured by the batch management system).

    Args:
        path: Directory in which to clear cluster logs.
        up_to: Job id up to which to remove matching cluster logs (non-inclusive).
        pattern: "pbs", "slurm", or arbitrary regex pattern. If regex is used, it must match the numeric job id of
            the cluster logs. Defaults to "pbs".
        recursive: Recurse into directories. Defaults to False.
        force: Remove cluster logs without prompting for user confirmation. Defaults to False.
    """
    assert os.path.exists(path) and os.path.isdir(path), "`path` must be a directory"
    if pattern == "pbs":
        pattern = ".*\.pbs\.(e|o)\K\d+$"
    elif pattern == "slurm":
        pattern = "^\d+(?=\..*\.(out|err)$)"
    for file in os.listdir(path):
        filepath = os.path.join(path,file)
        if os.path.isdir(filepath) and recursive:
            clear_cluster_logs(filepath, up_to=up_to, pattern=pattern, recursive=recursive-1, force=force)
        match = re.match(pattern, file)
        if match is not None:
            job_id = int(file[slice(*match.span())])
            if job_id < up_to:
                if force:
                    do_remove = True
                else:
                    do_remove = input(f"Remove job file '{filepath}'? [y/N]: ") == "y"
                if do_remove:
                    os.remove(filepath)

def clear_logs():
    parser = argparse.ArgumentParser()
    parser.add_argument("path",type=str,help="Directory in which to clear logs.")
    parser.add_argument("up_to", type=int, help="Job ID up to which to clear logs (non-inclusive).")
    parser.add_argument("pattern", type=str, 
                        help="'pbs','slurm', or arbitrary regex to match job log files. If a regex, must "
                        "match the numeric job ID in the log name.")
    parser.add_argument("-r","--recursive", type=int, nargs='?', const=-1, default=0,
                        help="Recurse into subdirectories. Optionally, a max recursion depth can be specified. "
                        "For example `-r 1` will only explore the provided path and its immediate subdirectories. "
                        "If no recursion depth is provided, and `-r` is specified, the program will recurse down to "
                        "the bottom of the directory tree.")
    parser.add_argument("-f","--force", action="store_true",
                        help="Remove log files without prompting the user first. CAUTION: using this option with a"
                        "regex pattern can silently remove arbitrary files.")
    args = parser.parse_args()
    clear_cluster_logs(args.path, args.up_to, args.pattern, args.recursive, args.force)

if __name__=="__main__":
    clear_logs()