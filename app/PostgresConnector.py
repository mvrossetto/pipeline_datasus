import psycopg2
import os
from dotenv import load_dotenv
from app.logger_config import logger
class PostgresConnector:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.database = os.getenv('POSTGRES_DB', 'postgres')
        self.user = os.getenv('POSTGRES_USER', 'postgres')
        self.password = os.getenv('POSTGRES_PASSWORD', 'master')        
        self.conexao = None
        self.cursor = None

    def __enter__(self):
        try:
            self.conexao = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conexao.cursor()
            return self
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}", exc_info=True)
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        if self.cursor:
            self.cursor.close()
        if self.conexao:
            self.conexao.close()
        # Se houver erro, ele não será suprimido
        return False

    def executar(self, query, params=None):
        """Executa uma query sem retorno (INSERT, UPDATE, DELETE)."""
        self.cursor.execute(query, params)
        self.conexao.commit()

    def buscar(self, query, params=None):
        """Executa uma query SELECT e retorna os resultados."""
        self.cursor.execute(query, params)
        return self.cursor.fetchall()


    def executar_many(self, query, data):
        try:
            self.cursor.executemany(query, data)
            self.conexao.commit()
        except Exception as e:
            self.conexao.rollback()            
            logger.error(f"Erro ao executar inserção em massa: {e}", exc_info=True)
            raise