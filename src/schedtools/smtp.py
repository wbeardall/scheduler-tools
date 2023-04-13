from getpass import getpass
import json
import os
import smtplib
import ssl

from schedtools.utils import config_dir

default_server = "smtp.office365.com"
default_port = 587

class SMTPCredentials:
    def __init__(self, server, port, sender_address, password, destination_address=None):
        self.server = server
        self.port = int(port)
        self.sender_address = sender_address
        self.password = password
        self.destination_address = destination_address

    @classmethod
    def read_json(cls, file):
        with open(file,"r") as f:
            return cls(**json.load(f))
        
    def to_json(self, file):
        with open(file,"w") as f:
            json.dump(self.to_dict(), f)

    @property
    def destination_address(self):
        return self._destination_address or self.sender_address
    
    @destination_address.setter
    def destination_address(self, destination_address):
        self._destination_address = destination_address

    def to_dict(self):
        return dict(
            server=self.server,port=self.port,sender_address=self.sender_address,
            password=self.password,destination_address=self.destination_address
        )
    
    def is_valid(self):
        return smtp_valid(server=self.server,port=self.port,sender_address=self.sender_address,
            password=self.password)

def smtp_valid(server, port, sender_address, password):
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(server, port) as server:
            server.starttls(context=context)
            server.login(sender_address, password)
        return True
    except smtplib.SMTPAuthenticationError:
        return False
    
def create_credentials():
    while True:
        server = input(f"SMTP server (defaults to '{default_server}'): ") or default_server
        port = int(input(f"SMTP port (defaults to {default_port}): ") or default_port)
        sender_address = input("Sender address: ")
        password = getpass(f"Password for {sender_address}: ")
        if smtp_valid(server,port,sender_address,password):
            break
        else:
            print("Invalid SMTP credentials. Please try again.")
    destination_address = input(f"Destination address (defaults to '{sender_address}'): ") or sender_address

    creds = SMTPCredentials(server=server,port=port,sender_address=sender_address,password=password,destination_address=destination_address)
    creds.to_json(os.path.join(config_dir(),"smtp.json"))
    return creds

def load_credentials():
    try:
        creds = SMTPCredentials.read_json(os.path.join(config_dir(),"smtp.json"))
    except FileNotFoundError as e:
        raise RuntimeError("SMTP credentials file not found.") from e
    if creds.is_valid():
        return creds
    else:
        raise RuntimeError("Invalid SMTP credentials.")
