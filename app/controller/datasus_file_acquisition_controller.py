import os
from dotenv import load_dotenv
import json 
from app.services.acquisition_file_service import AcquisitionFileService

class DataSusFileAcquisition:
    def __init__(self):
        self.FTP_HOST = ""        
        self.LOCAL_DIR = ""

    def atualiza_arquivos(self):
        self.read_env()        
        repositories = self.read_repository_files()

        for prefix in repositories['startwith']:
             AcquisitionFileService().look_for_files_starting_with(self.FTP_HOST, prefix['file'], prefix['directory'], self.LOCAL_DIR)

    def processa_arquivo(self, directory,file):
        self.read_env()
        AcquisitionFileService().look_for_files_starting_with(self.FTP_HOST, file, directory, self.LOCAL_DIR)
        
    def read_env(self):
        load_dotenv()
        self.FTP_HOST = os.getenv('FTP_HOST')
        self.LOCAL_DIR = os.getenv('LOCAL_DIR')        

    def read_repository_files(self):
        with open('app/MonitoredFiles.json', 'r') as file:
            dados = json.load(file)
        return dados