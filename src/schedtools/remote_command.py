import argparse
import sys

from schedtools.shell_handler import ShellHandler

def remote_command():
    """Utility for running arbitrary commands on a remote machine."""
    parser = argparse.ArgumentParser(description=remote_command.__doc__)
    parser.add_argument("host",help="Hostname upon which to run command.")
    parser.add_argument("command",nargs="+",help="Arbitrary shell command to run on remote machine.")
    args = parser.parse_args()
    handler = ShellHandler(args.host)
    command = []
    for el in args.command:
        if " " in el:
            # Check for presence of single and double quotes, and quote with the other
            if '"' in el:
                el = f"'{el}'"
            else:
                el = f'"{el}"'
        command.append(el)
    result = handler.execute(" ".join(command))
    print("".join(result.stdout),file=sys.stdout)
    print("".join(result.stderr),file=sys.stderr)
    exit(result.returncode)
    
if __name__=="__main__":
    remote_command()