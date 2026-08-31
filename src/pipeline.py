"""Starter entrypoint for the Dexwin take-home assessment."""

from __future__ import annotations

import argparse
from pathlib import Path


def run(data_dir: Path, output_dir: Path) -> None:
    """Build the required reporting and alert outputs."""
    output_dir.mkdir(parents=True, exist_ok=True)
    raise NotImplementedError("Implement the assessment pipeline")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    args = parser.parse_args()
    run(args.data_dir, args.output_dir)


if __name__ == "__main__":
    main()
