import logging
from simple_settings import LazySettings

settings = LazySettings("settings")

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('abracadabra.log')
                    ]
)
