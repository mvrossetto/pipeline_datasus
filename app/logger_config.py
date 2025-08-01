import logging
import os
from datetime import datetime

# Criar pasta para logs se não existir
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Nome do arquivo de log baseado na data
LOG_FILE = os.path.join(LOG_DIR, f"app_{datetime.now().strftime('%Y-%m-%d')}.log")

# Configuração do logger
logger = logging.getLogger("pipeline_logger")
logger.setLevel(logging.DEBUG)  # Pode mudar para INFO em produção

# Formato da mensagem
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Handler para arquivo
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setFormatter(formatter)

# Handler para console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Evitar duplicidade de handlers
if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
