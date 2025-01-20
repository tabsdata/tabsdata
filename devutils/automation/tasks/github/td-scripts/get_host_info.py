#
# Copyright 2024 Tabs Data Inc.
#

import socket


def get():
    try:
        hostname = socket.gethostname()
        address = socket.gethostbyname(hostname)
        if address.startswith("127."):
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                address = s.getsockname()[0]
        return {"hostname": hostname, "address": address}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    host_info = get()
    if "error" in host_info:
        print(f"Error retrieving host info: {host_info['error']}")
    else:
        print(f"Hostname: {host_info['hostname']}")
        print(f"Address: {host_info['address']}")
