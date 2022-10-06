import unittest
import os
import yaml
from connectorProject.connectors import read_test_cfg_info

def setUpModule():
    """call setup methods common to all test cases defined here"""
    print(f"setting up module {__name__}")


def tearDownModule():
    """call cleanup methods common to all test cases defined here"""
    print(f"done! Tearing Donw module {__name__}")
    
    
class test_yaml_reader(unittest.TestCase):
    def test_validate_data(self):
        cfg_path = os.path.dirname(os.path.abspath(__file__))
        cfg_file = cfg_path + "/" + "bad_connector_config.yaml"

        with self.assertRaises(yaml.YAMLError):
            test_cfg = read_test_cfg_info(cfg_file)
            print(test_cfg)


if __name__ == "__main__":
    unittest.main()
