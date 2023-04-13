import os
import subprocess
import sys

def get_service_file(name):
    name = name.lower().replace(' ', '-')
    return f"/etc/systemd/system/{name}.service"

def copy_config():
    config_dir = ".schedtools"
    user_config = os.path.join(os.path.expanduser("~"), config_dir)
    root_config = os.path.join("/etc",config_dir)
    subprocess.call(["sudo", "mkdir", "-p", root_config])
    if os.path.exists(user_config):
        for file in os.listdir(user_config):
            subprocess.call(["sudo", "cp", "-n", os.path.join(user_config, file), root_config])
    # Set the config directory to only be accessible to root
    subprocess.call(["sudo", "chown", "-R", "root:root", root_config])
    subprocess.call(["sudo", "chmod", "-R", "600", root_config])

def make_service(name: str, command: str, environment: dict = {}):
    copy_config()
    if len(environment):
        env_str = "\n".join([f'{k}="{v}"' for k,v in environment.items()]) + "\n"
        pass_env = "PassEnvironment=" + " ".join(environment) + "\n"
    else:
        env_str = ""
        pass_env = ""
    python = sys.executable
    service_id = name.lower().replace(' ', '-')

    service_file = get_service_file(service_id)
    service_conf = f"/etc/{service_id}-service.conf"

    service_def = f"""[Unit]
Description={name} Service

[Service]
ExecStart={python} {command}
Restart=always
EnvironmentFile={service_conf}
User=root
{pass_env}
[Install]
WantedBy=multi-user.target
"""

    
    subprocess.call(["sudo", "bash", "-c", "echo '" + service_def + "' > " + service_file])
    subprocess.call(["sudo", "bash", "-c", "echo '" + env_str + "' > " + service_conf])

    # Set the conf file to only be accessible to root
    subprocess.call(["sudo", "chown", "root:root", service_conf])
    subprocess.call(["sudo", "chmod", "600", service_conf])

    subprocess.call(["sudo", "systemctl", "daemon-reload"])
    subprocess.call(["sudo", "systemctl", "enable", service_id])
    subprocess.call(["sudo", "systemctl", "start", service_id])

def remove_service(name: str):
    allowed = ["rerun"]
    assert name in allowed, "We do not allow removal of arbitrary services with this script."
    service_id = name.lower().replace(' ', '-')
    service_file = get_service_file(service_id)
    service_conf = f"/etc/{service_id}-service.conf"
    subprocess.call(["sudo", "systemctl", "stop", service_id])
    subprocess.call(["sudo", "rm", service_file])
    subprocess.call(["sudo", "rm", service_conf])
    subprocess.call(["sudo", "systemctl", "daemon-reload"])