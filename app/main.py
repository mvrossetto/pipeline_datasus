from fastapi import FastAPI
import threading
import schedule
import time
from contextlib import asynccontextmanager
from app.DataSusFileAcquisitionController import DataSusFileAcquisition
from app.PostgresConnector import PostgresConnector
from app.DataHandler import DataHandler

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

@app.get("/teste")
def root():
    print("Iniciando atualização de arquivos...")
    DataSusFileAcquisition().atualiza_arquivos()
    return {
            "status": "Atualizando arquivos",
        }

def run_scheduler():
    # Executar todo dia às 09:00
    schedule.every().day.at("09:58").do(tarefa)
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