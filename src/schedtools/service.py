import subprocess
import sys

def make_service(name: str, command: str, environment: dict = {}):
    if len(environment):
        env_str = "\n".join([f'{k}="{v}"' for k,v in environment.items()]) + "\n"
        pass_env = "PassEnvironment=" + " ".join(environment) + "\n"
    else:
        env_str = ""
        pass_env = ""
    python = sys.executable
    service_id = name.lower().replace(' ', '-')

    service_file = f"/etc/systemd/system/{service_id}.service"
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
    subprocess.call(["sudo", "chmod", "700", service_conf])

    subprocess.call(["sudo", "systemctl", "daemon-reload"])
    subprocess.call(["sudo", "systemctl", "enable", service_id])
    subprocess.call(["sudo", "systemctl", "start", service_id])