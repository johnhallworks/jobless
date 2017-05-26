# -*- coding: utf-8; -*-
""" A base test class that provides some utilities."""
import unittest
from pathlib import Path


CURRENT_FOLDER = Path(__file__).parent.absolute()


class TestCase(unittest.TestCase):
    """A base class for test cases in the contenthub project."""

    @staticmethod
    def fixture(fixture_filename, mode="rt"):
        """Reads a fixture from a file from fixtures/
        Args:
           - mode (str): a mode for opening a file, see help(open),

        Returns a file object
        """

        return CURRENT_FOLDER.joinpath(
            "fixtures", fixture_filename).open(mode=mode)
