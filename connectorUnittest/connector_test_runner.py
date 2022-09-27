#!/usr/bin/python3

import unittest

import test_connector_factory as con_factory
import test_connector_validator as con_valid
import test_yaml_reader as yaml_reader


def connector_suite_1():
    # initialize test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # add test cases:
    suite.addTests(loader.loadTestsFromModule(con_factory))
    suite.addTests(loader.loadTestsFromModule(con_valid))
    suite.addTests(loader.loadTestsFromModule(yaml_reader))

    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=3)
    result = runner.run(connector_suite_1())
    print(result)
