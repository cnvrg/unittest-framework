#!/usr/bin/python3


import os
import subprocess
import zipfile
import re
import pprint
from enum import Enum, auto
from abc import ABC, abstractmethod
from datetime import datetime
from fabric import Connection
import numpy as np
import pandas as pd
#from connectorAbstraction import dataConnector_abc

""" 
    Define connector abstract base class with required methods
"""
class dataConnector_abc(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def connect(slef, user, password):
        pass
    

    @abstractmethod
    def getData(self):
        pass

    @abstractmethod
    def postProcessData(self):
        pass

    def getTimeStampStr(self) -> str:
        dt = datetime.now()
        return dt.strftime("%m-%d-%Y-%H-%M-%S")

    def getTempWorkingDirName(self, prefix, wdir) -> str:
        return f'{wdir}/{prefix}-{self.getTimeStampStr()}/'

    def makeLocalWorkingDir(self, prefix='wrkTmp', wdir="/tmp") -> str:
        dir_name = self.getTempWorkingDirName(prefix=prefix, wdir=wdir)
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        
        os.chdir(dir_name)

        return dir_name

    def unzipHelper(self, file):
        try:
            with zipfile.ZipFile(file) as z:
                z.extractall()
                print(f"Extracted all files in {file}")
                return True
        except:
            print("Invalid zip file {file}")
            return False

        
""" 
    Implement ssh connector as child class 
"""
class dataConnector_ssh(dataConnector_abc):
    def __init__(self):
        super().__init__()
        self.resultsPattern = dict(inflatingFiles="\s+inflating:\s*(\S+)")
        self.c = None

    def __del__(self):
        if(self.c != None):
            self.c.close()

    def connect(self, host, user=None):
        
        self.c = Connection(host)

        """ Test te connection with a trivial pwd command """
        with self.c.cd('/tmp'):
            results = self.c.run('pwd', hide=True)
        return not results.failed

    def getData(self, file):
        file_name  = file.split('/')[-1]        
        self.working_dir = self.makeLocalWorkingDir(prefix='sshTempDir')

        """ get data from ssh host """
        file_return = f'{self.working_dir}/{file_name}'
        with self.c.cd(self.working_dir):
            results = self.c.get(file, file_return)
            cmd_status = os.path.exists(results.local)
            #results = self.c.local( 'ls', hide=True)
        
        return cmd_status, file_return

    def postProcessData(self, file):
        with self.c.cd(self.working_dir):
            results = self.c.local(f'unzip {file}', hide=True)
            cmd_status = not results.failed
            #print(f'command: {results.command}, results: {results.stdout}')
            inflatingResults = re.findall(self.resultsPattern['inflatingFiles'], results.stdout)
            pprint.pprint(inflatingResults)
        return cmd_status, self.working_dir, inflatingResults

""" 
    Implement kaggle connector as child class 
"""
class dataConnector_kaggle(dataConnector_abc):
    def __init__(self):
        super().__init__()
        self.resultsPattern = dict(inflatingFiles="\s+inflating:\s*(\S+)")
        self.c = None

    def __del__(self):
        if(self.c != None):
            self.c.close()

    def connect(self, host, user=None):
        
        self.c = Connection(host)

        """ Test te connection with a trivial pwd command """
        with self.c.cd('/tmp'):
            results = self.c.run('pwd', hide=True)
        return not results.failed

    def getData(self, file):
        file_name  = file.split('/')[-1]        
        self.working_dir = self.makeLocalWorkingDir(prefix='kaggleTempDir')

        """ return file always is a zip file. 
            To fix: parse results class for actual filename. 
        """
        file_return = f'{self.working_dir}/{file_name}.zip'

        """ get data from kaggle """
        cmd = f'kaggle datasets download -d {file}'
        with self.c.cd(self.working_dir):
            results = self.c.local( cmd, hide=True)
            cmd_status = not results.failed


        print(f'fetched {file_return}')
        return cmd_status, file_return

    def postProcessData(self, file):
        with self.c.cd(self.working_dir):
            results = self.c.local(f'unzip {file}', hide=True)
            cmd_status = not results.failed
            #print(f'command: {results.command}, results: {results.stdout}')
            inflatingResults = re.findall(self.resultsPattern['inflatingFiles'], results.stdout)
            pprint.pprint(inflatingResults)

        return cmd_status, self.working_dir, inflatingResults

""" 
    Define connector type error exception 
    Needed to catch issues within class constructor 
"""
class connectorTypeError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.m = msg

    def __str__(self):
        return f'Unsupport connector: {self.m}! '
    def __repr__(self):
        return f"connectoryTypeError: {self.m}"


""" 
    Define type class a Enum 
    This will control the scope of type naming 
"""
class c_type(Enum):
    """ Class connector type """
    ssh     = auto()
    kaggle  = auto()
    cnn     = auto()


"""
    Wrap connectory classes with a factory class for an easy access
"""
class connector_factory():
    def __init__(self, con_type: c_type):

        self.cwd = os.getcwd()
        
        self.c_table = {
        c_type.ssh:    dict( connector=dataConnector_ssh(),    host='localhost', file=self.cwd+'/datasets/daily-climate-time-series-data.zip'),
        c_type.kaggle: dict( connector=dataConnector_kaggle(), host='vk1xusr02', file=self.cwd+'/sumanthvrao/daily-climate-time-series-data')
        }


        if con_type not in self.c_table.keys():
            #raise ValueError(f"{con_type} connector is not supported")
            raise connectorTypeError(f'{con_type} connector is not supported')

        c_info = self.c_table[con_type]

        self.con = c_info['connector']
        self.host = c_info['host']
        self.remote_file = c_info['file']

        
    def connect(self):
        return self.con.connect(self.host)

    def getData(self):
        status, self.local_fetched_file = self.con.getData(self.remote_file)
        return status, self.local_fetched_file

    def postProcessLocalFile(self):
        return self.con.postProcessData(self.local_fetched_file)


""" 
    Implement data handler / validator in a separate function/class 
"""

def connector_data_validator(file, columns):
    """ Read fetech results file into panda Dataframe """
    df = pd.read_csv(file, sep=",", engine='python',index_col=False)
    #pprint.pprint(df.columns)

    return set(columns).issubset(df.columns)

"""
    main 
"""
def local_demo():



    cf = connector_factory(c_type.ssh)
    cf.connect()
    cf.getData()
    ret_status, wdir, files = cf.postProcessLocalFile()

    #print(ret_status   ,wdir, files)

    cf = connector_factory(c_type.kaggle)
    cf.connect()
    cf.getData()
    ret_status, wdir, files = cf.postProcessLocalFile()

    print(ret_status   ,wdir, files)

    connector_data_validator(f'{wdir}/{files[0]}', columns=['date', 'meantemp', 'humidity', 'wind_speed', 'meanpressure'])
    



if __name__ == "__main__":
  local_demo()
