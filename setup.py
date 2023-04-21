import subprocess
from configparser import ConfigParser

from setuptools import setup  # type: ignore

config = ConfigParser()
config.read("setup.cfg")
install_requires = config["options"]["install_requires"].splitlines()


def check_system_packages_installed(packages):
    """
    Check if system packages are installed using dpkg.
    Args:
        packages (list): List of system packages to check for.

    Returns:
        bool: True if all packages are installed, False otherwise.
    """
    if isinstance(packages, str):
        packages = [packages]
    cmd = ["dpkg", "-s"]
    cmd.extend(packages)
    return not subprocess.run(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    ).returncode


# If GCC, pkg-config and libsystemd-dev are installed, we can also install systemd-python,
# which will enable journald logging
if check_system_packages_installed(["gcc", "pkg-config", "libsystemd-dev"]):
    install_requires.append("systemd-python")

if __name__ == "__main__":
    setup(install_requires=install_requires)
