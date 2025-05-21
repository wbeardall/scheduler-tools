import sys
from pathlib import Path

from rich.console import Console
from rich.markdown import Markdown

console = Console()


def help():
    with open(Path(__file__).parent.parent.parent / "README.md", "r+") as help_file:
        console.print(Markdown(help_file.read()))
    sys.exit(0)


if __name__ == "__main__":
    help()
