import hashlib
import os
import datasus_dbc

from app.logger.logger_config import get_logger
logger = get_logger("Utils")

class Utils:

    def calculate_file_hash(self, file_path):
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def change_extention(self,file,new_extention):    
        return os.path.splitext(file)[0] + new_extention
    
    def decompress_dbc_file(self, dbc_path):
        if not os.path.exists(dbc_path):
            raise FileNotFoundError(f"The file {dbc_path} does not exist.")        
        dbf_path = self.change_extention(dbc_path, '.dbf')

        # Decompress the DBC file to DBF
        logger.info(f"Decompressing DBC file: {dbc_path} to {dbf_path}")
        datasus_dbc.decompress(dbc_path, dbf_path)
        os.remove(dbc_path)  # Remove the original DBC file after decompression        