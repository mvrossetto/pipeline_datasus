import os
from dbfread import DBF
import pandas as pd
from app.PostgresConnector import PostgresConnector
from app.DataHandler import DataHandler 
from app.logger_config import logger
class DataProcess:
    def get_files_to_process(self):  
        database = DataHandler()
        return database.read_file_list()                        

    def process_files(self,file,file_hash):  
        database = DataHandler()
        
        #Verifica onde será inserir        
        table = self.choose_table_to_insert(file)
        
        if table != False:
            ##Deleta todos os registros antigos desse arquivo
            try:                
                if DataHandler().verify_table(table):
                    # Deleta os registros antigos do arquivo
                    comando = f"DELETE FROM {table} WHERE upper(file_name) = upper('{os.path.basename(file)}') ;"
                    with PostgresConnector() as db:
                        db.executar(comando)
            except Exception as e:
                logger.error(f"Erro ao deletar dados: {e}", exc_info=True)

            #Carrega arquivo
            try:
                records = self.read_dbf_file(file)    
                table_records = self.list_to_table(records)
                
                #Verifica se a tabela existe
                exists = database.verify_table(table)
                if not exists:
                    database.create_table(table_records,table)
            
                #Insere registros
                database.insert_table_data_many(table_records,table,os.path.basename(file), file_hash)
                #database.insert_table_data(table_records,table,os.path.basename(file), file_hash)

                DataHandler().insert_to_processed_file(file,file_hash)            
            except Exception as e:
                logger.error(f"Erro ao processar arquivo: {e}", exc_info=True)
                database.file_with_error(file)
                
    
    def read_dbf_file(self,file_path):        
        try:            

            table = DBF(file_path, encoding='latin-1', ignore_missing_memofile=True, char_decode_errors='ignore')
            records = []
            for record in table:
                records.append(record)
            return records    
        except Exception as e:            
            logger.error(f"Erro ao preocessar arquivo: {e}", exc_info=True)
        

        

        
    def choose_table_to_insert(self,file_path):   # Nesse link temos as legendas: https://datasus.saude.gov.br/transferencia-de-arquivos/ 
        file_name = os.path.basename(file_path)        
        if file_name.startswith('CIHA'):
            return 'CIHA'
        elif file_name.startswith('DNRS'):
            return 'SINASC'
        elif file_name.startswith('ACBI'):
            return 'ACBI'
        elif file_name.startswith('ACGR'):
            return 'ACGR'
        elif file_name.startswith('AIDA'):
            return 'AIDA'
        elif file_name.startswith('AIDC'):
            return 'AIDC'
        elif file_name.startswith('ANIM'):
            return 'ANIM'
        elif file_name.startswith('ANTR'):
            return 'ANTR'
        elif file_name.startswith('BOTU'):
            return 'BOTU'
        elif file_name.startswith('CANC'):
            return 'CANC'
        elif file_name.startswith('CHAG'):
            return 'CHAG'
        elif file_name.startswith('CHIK'):
            return 'CHIK'
        elif file_name.startswith('COLE'):
            return 'COLE'
        elif file_name.startswith('COQU'):
            return 'COQU'
        elif file_name.startswith('DENG'):
            return 'DENG'
        elif file_name.startswith('DERM'):
            return 'DERM'
        elif file_name.startswith('DERM'):
            return 'DERM'
        elif file_name.startswith('DIFT'):
            return 'DIFT'
        elif file_name.startswith('ESPO'):
            return 'ESPO'
        elif file_name.startswith('ESQU'):
            return 'ESQU'
        elif file_name.startswith('EXAN'):
            return 'EXAN'
        elif file_name.startswith('FMAC'):
            return 'FMAC'
        elif file_name.startswith('FTIF'):
            return 'FTIF'
        elif file_name.startswith('HANS'):
            return 'HANS'
        elif file_name.startswith('HANT'):
            return 'HANT'
        elif file_name.startswith('HEPA'):
            return 'HEPA'
        elif file_name.startswith('HIVA'):
            return 'HIVA'
        elif file_name.startswith('HIVC'):
            return 'HIVC'
        elif file_name.startswith('HIVE'):
            return 'HIVE'
        elif file_name.startswith('HIVG'):
            return 'HIVG'
        elif file_name.startswith('IEXO'):
            return 'IEXO'
        elif file_name.startswith('INFL'):
            return 'INFL'
        elif file_name.startswith('LEIV'):
            return 'LEIV'
        elif file_name.startswith('LEPT'):
            return 'LEPT'
        elif file_name.startswith('LERD'):
            return 'LERD'
        elif file_name.startswith('LTAN'):
            return 'LTAN'
        elif file_name.startswith('MALA'):
            return 'MALA'
        elif file_name.startswith('MENI'):
            return 'MENI'
        elif file_name.startswith('MENT'):
            return 'MENT'
        elif file_name.startswith('NTRA'):
            return 'NTRA'
        elif file_name.startswith('PAIR'):
            return 'PAIR'
        elif file_name.startswith('PEST'):
            return 'PEST'
        elif file_name.startswith('PFAN'):
            return 'PFAN'
        elif file_name.startswith('PNEU'):
            return 'PNEU'
        elif file_name.startswith('RAIV'):
            return 'RAIV'
        elif file_name.startswith('ROTA'):
            return 'ROTA'
        elif file_name.startswith('SDTA'):
            return 'SDTA'
        elif file_name.startswith('SIFA'):
            return 'SIFA'
        elif file_name.startswith('SIFC'):
            return 'SIFC'
        elif file_name.startswith('SIFG'):
            return 'SIFG'
        elif file_name.startswith('SRC'):
            return 'SRC'
        elif file_name.startswith('TETA'):
            return 'TETA'
        elif file_name.startswith('TETN'):
            return 'TETN'
        elif file_name.startswith('TOXC'):
            return 'TOXC'
        elif file_name.startswith('TOXG'):
            return 'TOXG'
        elif file_name.startswith('TRAC'):
            return 'TRAC'
        elif file_name.startswith('TUBE'):
            return 'TUBE'
        elif file_name.startswith('VARC'):
            return 'VARC'
        elif file_name.startswith('VIOL'):
            return 'VIOL'
        elif file_name.startswith('ZIKA'):
            return 'ZIKA'
        elif file_name.startswith('DOBR'):
            return 'SIM'
        else:
            return False    
    def list_to_table(self,data):    
        df = pd.DataFrame(data)
        return df