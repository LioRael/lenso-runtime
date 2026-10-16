#!/usr/bin/env python3
"""Package one native executable; merge per-target checksum files after CI fan-in."""
import argparse, hashlib, pathlib, shutil, subprocess
p = argparse.ArgumentParser()
p.add_argument("--binary", required=True, type=pathlib.Path)
p.add_argument("--target", required=True)
p.add_argument("--version", required=True)
p.add_argument("--output", required=True, type=pathlib.Path)
a = p.parse_args()
version = a.version.removeprefix("v")
assert len(version.split(".")) == 3 and all(s.isascii() and s.isdigit() for s in version.split(".")), "exact stable version required"
expected = "lenso-runtime " + version
assert subprocess.check_output([str(a.binary.resolve()), "--version"], text=True).strip() == expected, "tag and executable version differ"
a.output.mkdir(parents=True, exist_ok=True)
name = f"lenso-runtime-v{version}-{a.target}" + (".exe" if "windows" in a.target else "")
asset = a.output / name
shutil.copy2(a.binary, asset)
(a.output / (name + ".sha256")).write_text(hashlib.sha256(asset.read_bytes()).hexdigest() + "  " + name + "\n")
