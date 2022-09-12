import unittest
import os
import shutil 
import time
from connectorProject.connectors import connector_factory , connectorTypeError, c_type

class test_unsupported_connector(unittest.TestCase):
    def test_instantiate_connector(self):
        con_type = c_type.cnn
        print(f'testing for unsuppported connector type: {con_type}')
        with self.assertRaises(connectorTypeError):
            connector_factory(con_type)


class test_connector_factory(unittest.TestCase):
    

    @classmethod
    def setUpClass(self):
        self.con_type = c_type.kaggle
        print(f'setting up tests for {self.con_type} connector')
        self.con = connector_factory(self.con_type)
        self.location = None
        

    @classmethod
    def tearDown(self):
        if self.location != None:
            shutil.rmtree(self.location)
        
    def test_connection(self):
        actual = self.con.connect()
        self.assertTrue(actual)

    def test_data_fetech(self):
        actual, filename = self.con.getData()
        self.assertTrue(os.path.exists(filename))
        self.assertTrue(actual)

    #@unittest.skip( "Skip due problem with kaggle")
    def test_data_postprocessing(self):
        actual, location, file_list  = self.con.postProcessLocalFile()
        self.location = location
        self.assertTrue(actual)





class test_connector_factory_ssh(test_connector_factory):
        
    @classmethod
    def setUpClass(self):
        self.con_type = c_type.ssh
        print(f'setting up tests for {self.con_type} connector')
        self.con = connector_factory(self.con_type)
        self.location = None
        super(test_connector_factory, self).setUpClass()





if __name__ == "__main__":
        unittest.main()
