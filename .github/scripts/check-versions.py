"""Keep published version labels aligned with the code and vendored schemas they describe."""
from pathlib import Path
import tomllib

root = Path(__file__).resolve().parents[2]
properties = dict(
    line.split("=", 1) for line in (root / "gradle.properties").read_text().splitlines()
    if line and not line.startswith("#") and "=" in line
)
locked = tomllib.loads((root / "core-bridge/rust/kt-bridge/Cargo.lock").read_text())
versions = {package["version"] for package in locked["package"] if package["name"] == "temporalio-sdk-core"}
assert versions == {properties["sdkCoreVersion"]}, (versions, properties["sdkCoreVersion"])
pinned = tomllib.loads((root / "protos/PINNED.toml").read_text())
assert pinned["version"] == properties["protosSdkCoreVersion"], "Published proto version differs from PINNED.toml"
