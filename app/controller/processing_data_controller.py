from app.logger.logger_config import get_logger
from app.services.processing_data_service import ProcessingDataService
logger = get_logger("ProcessingDataController")

class ProcessingData:
    def __init__(self):
        pass

    def build_desnormalized_data(self):
        logger.info(f"build_desnormalized_data called")
        ProcessingDataService().process_data(2019, 2019,True)
        pass
        