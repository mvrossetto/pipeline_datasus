import pandas as pd
from app.database.data_handler import DataHandler

from app.database.postgres_connector import PostgresConnector
from app.logger.logger_config import get_logger
logger = get_logger("ProcessingDataService")

class ProcessingDataService:

    def process_data(self, year_begins: int, year_ends: int,isConfirmado:bool):
        logger.info(f"Processando dados...")

        # seleciona dados
        with PostgresConnector() as db:
            logger.info(f"Buscando dados entre {year_begins} e {year_ends}")
            if isConfirmado:
                logger.info("Notificações confirmadas")
                df = db.buscar(f"SELECT sem_pri,sg_uf_not,id_unidade,id_municip,nu_ano from public.deng where nu_ano BETWEEN '{year_begins}' AND '{year_ends}' and classi_fin in ('10','11','12')")
            else:
                logger.info("Todas as notificações")
                df = db.buscar(f"SELECT sem_pri,sg_uf_not,id_unidade,id_municip,nu_ano from public.deng where nu_ano BETWEEN '{year_begins}' AND '{year_ends}'")

        if not df or len(df) == 0:
            logger.warning("Nenhum dado encontrado para processar.")
            return

        df = pd.DataFrame(df, columns=["sem_pri","sg_uf_not","id_unidade", "id_municip", "nu_ano"])

        logger.info(f"Desnormalizando dados...")
        df_desnormalizada = (df.groupby(["sem_pri","sg_uf_not","id_unidade", "id_municip", "nu_ano"]).size().reset_index(name="total_casos"))
        logger.info(f"Ordenando dados...")
        df_desnormalizada = df_desnormalizada.sort_values(by=["sem_pri","sg_uf_not","id_unidade", "id_municip", "nu_ano"])

        # salva dados
        with PostgresConnector() as db:
            logger.info(f"Inserindo dados desnormalizados na tabela 'deng_desnormalizada'")
            db.salvar(df_desnormalizada, "deng_desnormalizada", if_exists="replace")
        
        logger.info(f"Dados processados e salvos com sucesso.")        