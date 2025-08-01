from fastapi import FastAPI
import threading
import schedule
import time
from typing import List
from contextlib import asynccontextmanager
from app.DataSusFileAcquisitionController import DataSusFileAcquisition
from app.PostgresConnector import PostgresConnector
from app.DataHandler import DataHandler
from app.Validators.FileRequestValidator import FileRequest
from app.logger_config import get_logger
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
def process_files(files: List[FileRequest]):
    try:
        # Itera pelos arquivos recebidos
        processed_files = []
        for file_item in files:            
            logger.info(f"Recebendo arquivo {file_item.file} do diretório {file_item.directory}")

            # Aqui você pode chamar sua função para processar cada arquivo
            # Exemplo fictício:
            DataSusFileAcquisition().processa_arquivo(file_item.directory, file_item.file)

            processed_files.append({
                "directory": file_item.directory,
                "file": file_item.file,
                "status": "Processado com sucesso"
            })
        
        return {
            "status": "Arquivos recebidos",
            "detalhes": processed_files
        }
    except Exception as e:
        return {
            "status": "Erro",
            "mensagem": str(e)
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


