from importlib.metadata import version

import schemas


def test_package_is_installed() -> None:
    assert version("configstream") == "0.1.0"
    assert schemas.__doc__
