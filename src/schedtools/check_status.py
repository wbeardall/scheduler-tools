import os
import shutil
import subprocess

from schedtools.service import get_service_file

def check_status():
    """Utility for removing schedtools services from systemd."""
    services = ["rerun"]
    registered = [el for el in services if os.path.exists(get_service_file(el))]
    if len(registered)==0:
        print("No `schedtools` services are registered with `systemd`.")
        return
    print("The following services are registered: \n" + "\n".join(registered))
    terminal_width, _ = shutil.get_terminal_size()
    print("_"*terminal_width)
    for service in registered:
        subprocess.call(f"sudo systemctl status {service}.service".split())
        print("_"*terminal_width)

if __name__=="__main__":
    check_status()