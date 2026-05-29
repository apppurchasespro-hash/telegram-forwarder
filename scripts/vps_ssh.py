"""
One-off helper to run a command on the acct3 VPS over SSH with password auth.
Reads VPS_HOST, VPS_USER, VPS_PWD from the environment. Command is taken from
argv (joined) or stdin if argv is empty. Prints stdout+stderr.

Intended for short-lived debugging — do NOT commit any password into env files.
"""
import os
import sys
import paramiko

# Telethon prints checkmarks / box-drawing chars; default Windows cp1252 chokes on them.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

host = os.environ.get("VPS_HOST", "43.133.13.132")
user = os.environ.get("VPS_USER", "ubuntu")
pwd = os.environ.get("VPS_PWD")
if not pwd:
    print("VPS_PWD not set in env", file=sys.stderr)
    sys.exit(2)

cmd = " ".join(sys.argv[1:]).strip() or sys.stdin.read()
if not cmd.strip():
    print("no command provided", file=sys.stderr)
    sys.exit(2)

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(host, username=user, password=pwd, timeout=15, allow_agent=False, look_for_keys=False)

stdin, stdout, stderr = client.exec_command(cmd, get_pty=False, timeout=120)
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
rc = stdout.channel.recv_exit_status()
sys.stdout.write(out)
if err:
    sys.stderr.write(err)
client.close()
sys.exit(rc)
