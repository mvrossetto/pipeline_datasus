import ftplib  
import os
from app.utils.utils import Utils
from app.database.data_handler import DataHandler
from app.services.data_process_service import DataProcess

from app.logger.logger_config import get_logger
logger = get_logger("AcquisitionFileService")

class AcquisitionFileService:
    def __init__(self):
        self.ftp_host = ""
        self.file_dir = ""

    def look_for_files_starting_with(self, _ftp_host, prefix, ftp_dir, local_dir):
        self.ftp_host = _ftp_host        

        # Extrair o diretório do arquivo
        self.file_dir = os.path.dirname(ftp_dir)        
        
        # Verifica se o diretório local existe
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        
        ftp = ftplib.FTP(self.ftp_host)
        ftp.login()
        ftp.cwd(self.file_dir)

        # Listar arquivos no diretório FTP
        files = ftp.nlst()
        ftp.quit()

        # Filtrar arquivos que começam com o prefixo especificado
        matching_files = [file for file in files if file.startswith(prefix)]

        matching_files.sort(reverse=True)

        for file in matching_files:
            self.process_file(file,local_dir)

        

    def process_file(self,file,local_dir):
        file_name = os.path.basename(file)       
        logger.info(f"Processando o arquivo: {file_name}")

        local_file_path = os.path.join(local_dir, file_name)
            
        # Baixar o arquivo do FTP
        with open(local_file_path, 'wb') as local_file:
            ftp = ftplib.FTP(self.ftp_host)
            ftp.login()
            ftp.cwd(self.file_dir)    
            logger.info(f"Baixando arquivo {file_name} do FTP para {local_file_path}")                
            ftp.retrbinary(f'RETR {file_name}', local_file.write)
            ftp.quit()            

        ftp_file_hash = Utils().calculate_file_hash(local_file_path)
        
        # Teste para verificar Hash
        name_file = os.path.basename(local_file_path)
        if len(DataHandler().read_files_processed(name_file,ftp_file_hash)) == 0:
            # Descompactar o arquivo usando o comando shell            
            if file_name.endswith('.dbc'):
                Utils().decompress_dbc_file(local_file_path)                
            
            #Processa para inserir no banco de dados
            name_file_extracted = Utils().change_extention(local_file_path,'.dbf')
            DataProcess().process_files(name_file_extracted,ftp_file_hash)
            DataHandler().delete_local_file(name_file_extracted)
            logger.info(f"Arquivo processado: {file_name}")
            return
        else:
            logger.info(f"Arquivo estava atualizado: {file_name}")
            DataHandler().delete_local_file(local_file_path)