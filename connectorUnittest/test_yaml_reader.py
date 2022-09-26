import unittest
import yaml
from connectorProject.connectors import read_test_cfg_info


class test_yaml_reader(unittest.TestCase):
    def test_validate_data(self):
        cfg_file = "bad_connector_config.yaml"

        with self.assertRaises(yaml.YAMLError):
            test_cfg = read_test_cfg_info(cfg_file)
            print(test_cfg)


if __name__ == "__main__":
    unittest.main()
