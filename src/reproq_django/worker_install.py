"""Consume the worker repository's canonical source build and bundle verifier."""

import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def build_verified_source(source, destination, *, system, architecture):
    source = Path(source).resolve()
    builder = source / "scripts/build_releases.sh"
    verifier = source / "scripts/verify_release_bundle.sh"
    if not builder.is_file() or not verifier.is_file():
        raise ValueError("Worker source must provide the canonical release builder and bundle verifier")
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(GIT_TERMINAL_PROMPT="0")

    def output(arguments):
        return subprocess.check_output(arguments, cwd=source, env=environment, text=True).strip()

    identity = [output(["git", "rev-parse", "--verify", "HEAD"]),
                output(["git", "rev-parse", "--verify", "HEAD^{tree}"])]
    with tempfile.TemporaryDirectory(prefix="reproq-source-install-") as temporary:
        bundle = Path(temporary) / "bundle"
        # This owns compiler selection, clean-source checks, flags, targets and
        # checksums. The SDK never maintains another go build command.
        subprocess.run(["bash", str(builder), "--unreleased", "--output", str(bundle)],
                       cwd=source, env=environment, check=True)
        manifest_digest = hashlib.sha256((bundle / "BUILD-MANIFEST.txt").read_bytes()).hexdigest()
        checksums_digest = hashlib.sha256((bundle / "SHA256SUMS").read_bytes()).hexdigest()
        verify = ["sh", str(verifier), str(bundle), manifest_digest, checksums_digest]
        metadata = output(verify).split("\t")
        if len(metadata) != 6 or metadata[1:3] != identity or metadata[3] != "UNRELEASED":
            raise ValueError("Canonical build does not identify the selected committed worker source")
        name = output(verify + [system, architecture])
        if Path(name).name != name or not name.startswith("reproq-"):
            raise ValueError("Canonical verifier returned an invalid platform artifact")
        artifact = bundle / name
        digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
        if identity != [output(["git", "rev-parse", "--verify", "HEAD"]),
                        output(["git", "rev-parse", "--verify", "HEAD^{tree}"])]:
            raise ValueError("Worker source revision changed during installation")
        shutil.copyfile(artifact, destination)
        if hashlib.sha256(Path(destination).read_bytes()).hexdigest() != digest:
            raise ValueError("Worker artifact changed during staging")
    return digest
