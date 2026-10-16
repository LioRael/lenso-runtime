#!/usr/bin/env python3
"""Native kernel startup + loopback readiness, owned child cleanup on every OS."""
import json, pathlib, socket, subprocess, sys
p = subprocess.Popen([str(pathlib.Path(sys.argv[1]).resolve()), "--listen", "127.0.0.1:0"], stderr=subprocess.PIPE, text=True)
try:
    event = json.loads(p.stderr.readline())
    assert event["event"] == "runtime_listening"
    host, port = event["address"].rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=5):
        pass
finally:
    p.terminate()
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=5)
