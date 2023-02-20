import argparse

from schedtools.service import remove_service as remove

def remove_service():
    """Utility for removing schedtools services from systemd."""
    services = ["rerun"]

    parser = argparse.ArgumentParser(description=remove_service.__doc__)
    parser.add_argument("service",choices=services+["all"],
        help="Schedtools service to remove. `all` removes all registered schedtools services.")
    args = parser.parse_args()

    if args.service=="all":
        for service in services:
            remove(service)
    else:
        remove(args.service)

if __name__=="__main__":
    remove_service()