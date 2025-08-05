import psycopg2
import psycopg2.extras as extras
import os
from dotenv import load_dotenv

from app.logger.logger_config import get_logger
logger = get_logger("PostgresConnector")

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
    
    def salvar(self, df, table_name, if_exists='fail'):
        if df.empty:
            logger.warning("DataFrame vazio. Nenhum dado salvo.")
            return
    
        try:
            cursor = self.conexao.cursor()
    
            # Drop ou truncate se solicitado
            if if_exists == 'replace':
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            elif if_exists == 'append':
                pass
            elif if_exists == 'fail':
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{table_name}'
                    );
                """)
                existe = cursor.fetchone()[0]
                if existe:
                    raise ValueError(f"Tabela {table_name} já existe e if_exists='fail'.")
    
            # Criação da tabela automaticamente
            cols = ", ".join([f"{col} TEXT" for col in df.columns])
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols});")
    
            # Inserção eficiente
            tuples = [tuple(x) for x in df.to_numpy()]
            cols = ','.join(list(df.columns))
            query = f"INSERT INTO {table_name}({cols}) VALUES %s"
            extras.execute_values(cursor, query, tuples)
    
            self.conexao.commit()
            cursor.close()
            logger.info(f"Dados salvos na tabela {table_name} com sucesso.")
    
        except Exception as e:
            logger.error(f"Erro ao salvar dados na tabela {table_name}: {e}", exc_info=True)
            self.conexao.rollback()
            raise