import unittest
import os
from unittest.mock import Mock
from connectorProject.connectors import connector_factory , connector_data_validator, c_type

class mock_connector:
    def __init__(self):
        self.retvalue = (True, 'dir_path', ['file1.csv', 'file2.csv'])

    def mock_postProcessor(self):
        return self.retvalue

class test_connector_validator(unittest.TestCase):

    def test_validate_data(self):
        mockRetvalue = (True, '/tmp/mockDemo-09-08-2022-20-18-19', 
                         ['DailyDelhiClimateTest.csv', 'DailyDelhiClimateTrain.csv'])
        cl=['date', 'meantemp', 'humidity', 'wind_speed', 'meanpressure']
        print("Check if columns {cl} are in feteced data ")
        mock_cl =mock_connector()
        mock_cl.mock_postProcessor = Mock(return_value = mockRetvalue)
        #cf = connector_factory(c_type.ssh)
        #cf.connect()
        #cf.getData()
        #ret_status, wdir, files = cf.postProcessLocalFile()
        ret_status, wdir, files = mock_cl.mock_postProcessor()
        print(ret_status   ,wdir, files)

        actual = connector_data_validator(f'{wdir}/{files[0]}', columns=cl)        
        self.assertTrue(actual)

if __name__ == "__main__":
        unittest.main()
