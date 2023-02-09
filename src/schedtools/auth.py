from schedtools.shell_handler import ShellHandler

def check_auth(host, **kwargs):
    handler = ShellHandler(host, **kwargs)
    handler.close()