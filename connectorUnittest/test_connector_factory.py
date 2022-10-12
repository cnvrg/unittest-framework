import unittest
import os
import shutil
from connectorProject.connectors import connector_factory, connectorTypeError, c_type
from connectorProject.connectors import read_test_cfg_info


def setUpModule():
    """call setup methods common to all test cases defined here"""
    print(f"setting up module {__name__}")


def tearDownModule():
    """call cleanup methods common to all test cases defined here"""
    print(f"done! Tearing Down module {__name__}")


class test_unsupported_connector(unittest.TestCase):
    def test_instantiate_connector(self):
        con_type = c_type.cnn
        host = "vk1xusr02"
        file = "somefile"

        print(f"testing for unsuppported connector type: {con_type}")
        with self.assertRaises(connectorTypeError):
            connector_factory(con_type, host=host, file=file)


class test_connector_factory(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        cfg_path = os.path.dirname(os.path.abspath(__file__))
        cfg_file = cfg_path + "/" + "connector_config.yaml"

        test_cfg = read_test_cfg_info(cfg_file)

        connector_info = test_cfg["connectors"]["ssh"]
        self.con_type = c_type.ssh
        host = connector_info["host"]
        file = connector_info["file"]
        print(f"setting up tests for {self.con_type} connector")
        self.con = connector_factory(self.con_type, host=host, file=file)
        self.location = None

    @classmethod
    def tearDownClass(self):
        if self.location is not None:
            shutil.rmtree(self.location)

    def test_connection(self):
        actual = self.con.connect()
        self.assertTrue(actual)

    def test_data_fetech(self):
        actual, filename = self.con.getData()
        self.assertTrue(os.path.exists(filename))
        self.assertTrue(actual)

    def test_data_postprocessing(self):
        actual, location, file_list = self.con.postProcessLocalFile()
        self.location = location
        self.assertTrue(actual)


@unittest.skip("Skip due problem with kaggle")
class test_connector_factory_kaggle(test_connector_factory):
    @classmethod
    def setUpClass(self):
        cfg_path = os.path.dirname(os.path.abspath(__file__))
        cfg_file = cfg_path + "/" + "connector_config.yaml"

        test_cfg = read_test_cfg_info(cfg_file)

        connector_info = test_cfg["connectors"]["kaggle"]
        self.con_type = c_type.kaggle
        host = connector_info["host"]
        file = connector_info["file"]
        print(f"setting up tests for {self.con_type} connector")
        self.con = connector_factory(self.con_type, host=host, file=file)
        self.location = None
        super(test_connector_factory, self).setUpClass()


if __name__ == "__main__":
    unittest.main()
