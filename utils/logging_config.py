# logging_config.py
import logging

def setup_logging():
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('app.log')
        ])

    # logging.basicConfig(
    #     level=logging.DEBUG,
    #     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    #     handlers=[
    #         logging.StreamHandler(),
    #         logging.FileHandler('app.log')
    #     ]
    # )
