import unittest
import os
from unittest.mock import Mock
from connectorProject.connectors import connector_data_validator, read_test_cfg_info

def setUpModule():
    """call setup methods common to all test cases defined here"""
    print(f"setting up module {__name__}")


def tearDownModule():
    """call cleanup methods common to all test cases defined here"""
    print(f"done! Tearing Donw module {__name__}")
    
    
class mock_connector:
    def __init__(self):
        self.retvalue = (True, "dir_path", ["file1.csv", "file2.csv"])

    def mock_postProcessor(self):
        return self.retvalue


class test_connector_validator(unittest.TestCase):
    def test_validate_data(self):

        cfg_path = os.path.dirname(os.path.abspath(__file__))
        cfg_file = cfg_path + "/" + "connector_config.yaml"

        test_cfg = read_test_cfg_info(cfg_file)

        connector_info = test_cfg["mock_connector_return"]

        mockRetvalue = (
            connector_info["status"],
            connector_info["directory"],
            connector_info["file_list"],
        )
        cl = connector_info["validation_columns"]
        print("Check if columns {cl} are in fetched data ")
        mock_cl = mock_connector()
        mock_cl.mock_postProcessor = Mock(return_value=mockRetvalue)
        # cf = connector_factory(c_type.ssh)
        # cf.connect()
        # cf.getData()
        # ret_status, wdir, files = cf.postProcessLocalFile()
        ret_status, wdir, files = mock_cl.mock_postProcessor()
        print(ret_status, wdir, files)

        actual = connector_data_validator(f"{wdir}/{files[0]}", columns=cl)
        self.assertTrue(actual)


if __name__ == "__main__":
    unittest.main()
