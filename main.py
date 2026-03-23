"""Thin entrypoint wrapper.

This module intentionally stays small; the consumer loop lives in core.runner.
Functionality preserved.
"""

from app.entrypoint import main as _main


def main() -> None:
    _main()


if __name__ == "__main__":
    main()
