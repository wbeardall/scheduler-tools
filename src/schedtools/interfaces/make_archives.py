import argparse
import os
import re
import subprocess
from multiprocessing import Pool
from typing import Optional

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


def archive_directory(path: str) -> None:
    """
    Create or update a zip archive of the given directory.

    Args:
        path: Path to the directory to archive
    """
    if not os.path.isdir(path):
        raise ValueError(f"Path {path} is not a directory")

    # Get the parent directory and target directory name
    parent_dir = os.path.dirname(path)
    dir_name = os.path.basename(path)

    # Create zip file path in parent directory
    zip_path = os.path.join(parent_dir, f"{dir_name}.zip")

    subprocess.run(
        ["zip", "-r", "-u", zip_path, path],
        check=True,
        cwd=parent_dir,
    )


def make_archives(
    path: str,
    pattern: Optional[str] = None,
    cores: Optional[int] = None,
) -> None:
    """
    Make archives of the files matching the pattern in the given path.
    """
    if cores is None:
        cores = os.cpu_count()

    candidates = os.listdir(path)
    if pattern is not None:
        candidates = [f for f in candidates if re.match(pattern, f)]

    with Pool(cores) as pool:
        it = pool.imap(archive_directory, candidates)
        if tqdm is not None:
            it = tqdm(it, total=len(candidates), desc=f"Archiving {path}...")
        for _ in it:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str)
    parser.add_argument("--pattern", type=str, default=None)
    parser.add_argument("--cores", type=int, default=None)
    args = parser.parse_args()
    make_archives(args.path, args.pattern, args.cores)


if __name__ == "__main__":
    main()
