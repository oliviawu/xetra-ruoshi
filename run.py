"""Run the Xetra ETL APP"""

import logging
import logging.config
import yaml

def main():
 """
 Entry point
 """
 # Parsing YAML file

 config_path='../xetra-ruoshi/config/xetra_report1_config.yml'
 config=yaml.safe_load(open(config_path))
 #print(config)
 # configure logging
 log_config=config['logging']
 logging.config.dictConfig(log_config)
 # creat logger as the name of the file
 logger=logging.getLogger(__name__)
 logger.info("This is a test.")

if __name__=='__main__':
    main()