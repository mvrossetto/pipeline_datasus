import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

# === CONFIGURAÇÕES GERAIS === #
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # Cria a pasta se não existir

LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")


# === FORMATOS DE LOG === #
class CustomFormatter(logging.Formatter):
    # Cores ANSI para terminal
    COLORS = {
        'DEBUG': '\033[94m',   # Azul
        'INFO': '\033[92m',    # Verde
        'WARNING': '\033[93m', # Amarelo
        'ERROR': '\033[91m',   # Vermelho
        'CRITICAL': '\033[95m' # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        log_format = f"{log_color}%(asctime)s - %(name)s - %(levelname)s - %(message)s{self.RESET}"
        formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)


def get_logger(module_name: str):
    """Retorna um logger configurado para o módulo especificado."""
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.DEBUG)  # Alterar para INFO em produção

    # Evita adicionar handlers duplicados
    if not logger.handlers:

        # === Handler para console com cores === #
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(CustomFormatter())
        logger.addHandler(console_handler)

        # === Handler para arquivo com rotação diária === #
        file_handler = TimedRotatingFileHandler(
            LOG_FILE,
            when="midnight",   # Rotaciona a cada meia-noite
            interval=1,
            backupCount=7,     # Mantém últimos 7 dias de logs
            encoding="utf-8"
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger
