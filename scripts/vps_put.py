"""
Upload a local file's bytes to a remote path over SFTP with a given mode.
Reads VPS_HOST/VPS_USER/VPS_PWD from env. Args: <local> <remote> [mode_octal]

Used to push secret files (env files, session strings) without echoing them
through a shell command line.
"""
import os
import sys
import paramiko

host = os.environ["VPS_HOST"] if "VPS_HOST" in os.environ else "43.133.13.132"
user = os.environ.get("VPS_USER", "ubuntu")
pwd = os.environ["VPS_PWD"]

local, remote = sys.argv[1], sys.argv[2]
mode = int(sys.argv[3], 8) if len(sys.argv) > 3 else 0o600

t = paramiko.Transport((host, 22))
t.connect(username=user, password=pwd)
sftp = paramiko.SFTPClient.from_transport(t)
sftp.put(local, remote)
sftp.chmod(remote, mode)
attrs = sftp.stat(remote)
print(f"uploaded {remote} ({attrs.st_size} bytes, mode {oct(attrs.st_mode & 0o777)})")
sftp.close()
t.close()
