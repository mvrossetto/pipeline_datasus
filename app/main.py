from fastapi import FastAPI,BackgroundTasks
import threading
import schedule
import time
from typing import List
from contextlib import asynccontextmanager
from app.controller.datasus_file_acquisition_controller import DataSusFileAcquisition
from app.database.postgres_connector import PostgresConnector
from app.database.data_handler import DataHandler
from app.services.processing_data_service import ProcessingDataService
from app.validators.file_request_validator import FileRequest
from app.logger.logger_config import get_logger
logger = get_logger("Main")



app = FastAPI()

@app.get("/")
def root():
    return {"message": "API rodando com sucesso!"}


@app.get("/health")
def health_check():
    try:
        with PostgresConnector() as db:
            result = db.buscar("SELECT 1;")
        return {
            "status": "ok",
            "database": "conectado"            
        }
    except Exception as e:
        return {
            "status": "erro",
            "database": "falha na conexão",
            "error": str(e)
        }


def tarefa(): 
    DataSusFileAcquisition().atualiza_arquivos()

@app.post("/process-files")
async def process_files(files: List[FileRequest], background_tasks: BackgroundTasks):
    processed_files = []

    for file_item in files:
        logger.info(f"Recebendo arquivo {file_item.file} do diretório {file_item.directory}")

        # Adiciona a tarefa ao processamento em segundo plano
        background_tasks.add_task(DataSusFileAcquisition().processa_arquivo, file_item.directory, file_item.file)

        processed_files.append({
            "directory": file_item.directory,
            "file": file_item.file,
            "status": "Processamento iniciado em background"
        })

    return {
        "status": "Arquivos recebidos",
        "detalhes": processed_files
    }


@app.post("/desnormalize-data")
async def process_files(year_begins: int, year_ends: int, background_tasks: BackgroundTasks):
    logger.info(f"Processamento de dados iniciado para o período {year_begins} a {year_ends}")

    try:
        ProcessingDataService().process_data(year_begins, year_ends)
        return {
            "status": "Dados processados com sucesso",
            "year_begins": year_begins,
            "year_ends": year_ends
        }
    except Exception as e:
        return {
            "status": "erro",            
            "error": str(e)
        }



def run_scheduler():
    schedule.every().monday.at("03:00").do(tarefa)
    while True:
        schedule.run_pending()
        time.sleep(1)

@asynccontextmanager
async def lifespan(app):
    thread = threading.Thread(target=run_scheduler, daemon=True)
    thread.start()
    data_handler = DataHandler()
    data_handler.tabelas_iniciais()
    yield

app.router.lifespan_context = lifespan