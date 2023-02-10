import subprocess
import sys

def make_service(name, command, environment):
    env_str = "\n".join([f'Environment="{k}={v}"' for k,v in environment.items()])
    if env_str:
        env_str = f"\n{env_str}"
    python = sys.executable
    service_config = f"""[Unit]
Description={name} Service

[Service]{env_str}
ExecStart={python} {command}
Restart=always
User=root

[Install]
WantedBy=multi-user.target
"""

    filename = f"/etc/systemd/system/{name.lower().replace(' ', '-')}.service"
    subprocess.call(["sudo", "bash", "-c", "echo '" + service_config + "' > " + filename])

    subprocess.call(["sudo", "systemctl", "daemon-reload"])
    subprocess.call(["sudo", "systemctl", "enable", "example.service"])
    subprocess.call(["sudo", "systemctl", "start", "example.service"])