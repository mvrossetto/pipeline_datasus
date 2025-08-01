import ftplib  
import os
from app.Utils import Utils
from app.DataHandler import DataHandler
from app.DataProcess import DataProcess

class AcquisitionFileService:

    def process_file_in_repository(self, ftp_host, ftp_dir, local_dir):
        local_dir = local_dir.replace('\\', '/')

        # Verifica se o diretório local existe
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        ftp = ftplib.FTP(ftp_host)
        ftp.login()
        ftp.cwd(ftp_dir)

        # Listar arquivos no FTP
        ftp_files = ftp.nlst()

        # Comparar arquivos e baixar novos arquivos
        for file in ftp_files:
            self.process_file(ftp,file,local_dir)          

        # Fechar conexão FTP
        ftp.quit()

    def look_for_files_starting_with(self, ftp_host, prefix, ftp_dir, local_dir):
        
        # Extrair o diretório do arquivo
        file_dir = os.path.dirname(ftp_dir)        
        
        # Verifica se o diretório local existe
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        
        ftp = ftplib.FTP(ftp_host)
        ftp.login()
        ftp.cwd(file_dir)

        # Listar arquivos no diretório FTP
        files = ftp.nlst()

        # Filtrar arquivos que começam com o prefixo especificado
        matching_files = [file for file in files if file.startswith(prefix)]

        matching_files.sort(reverse=True)

        for file in matching_files:
            self.process_file(ftp,file,local_dir)

        ftp.quit()

    def process_file(self,ftp,file,local_dir):
        file_name = os.path.basename(file)
        local_file_path = os.path.join(local_dir, file_name)
            
        # Baixar o arquivo do FTP
        with open(local_file_path, 'wb') as local_file:
            ftp.retrbinary(f'RETR {file_name}', local_file.write)
        
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
        else:
            DataHandler().delete_local_file(local_file_path)