import pandas as pd
from app.database.data_handler import DataHandler

from app.database.postgres_connector import PostgresConnector
from app.logger.logger_config import get_logger
logger = get_logger("ProcessingDataService")

class ProcessingDataService:

    def process_data(self, year_begins: int, year_ends: int):
        logger.info(f"Processando dados...")

        # seleciona dados
        with PostgresConnector() as db:
            df = db.buscar(f"SELECT sg_uf_not,id_unidade,id_municip,nu_ano,sem_not from public.deng where nu_ano BETWEEN '{year_begins}' AND '{year_ends}'")

        if not df or len(df) == 0:
            logger.warning("Nenhum dado encontrado para processar.")
            return
    
        df = pd.DataFrame(df, columns=["sg_uf_not","id_unidade", "id_municip", "nu_ano", "sem_not"])

        df_desnormalizada = (df.groupby(["sg_uf_not","id_unidade", "id_municip", "nu_ano", "sem_not"]).size().reset_index(name="total_casos"))
        df_desnormalizada = df_desnormalizada.sort_values(by=["sg_uf_not","id_unidade", "id_municip", "nu_ano", "sem_not"])

        # salva dados
        with PostgresConnector() as db:
            db.salvar(df_desnormalizada, "deng_desnormalizada", if_exists="replace")
        
        logger.info(f"Dados processados e salvos com sucesso.")        