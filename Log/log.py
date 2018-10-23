import logging
import yaml
import logging.config
import os
log_dir=os.path.dirname(os.path.abspath(__file__))

path=os.path.join(log_dir,'config.yaml')

with open(path, 'r', encoding='utf-8') as f:
    config = yaml.load(f)
    logging.config.dictConfig(config)

log_crawler=logging.getLogger("crawler")



