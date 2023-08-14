import json
import uuid
import glob
import pandas as pd
import os
import logging

def get_columns(ds):
    schema_file_path = os.environ.setdefault('SCHEMA_FILE_PATH', 'data/retail_db/schemas.json')  
    with open (f'{schema_file_path}') as fp:
        schemas = json.load(fp)
    try:
        schema = schemas.get(ds)
        if not schema:
            raise KeyError
        cols = sorted(schema, key=lambda s: s['column_position'] )
        columns = [col['column_name'] for col in cols]
        return columns
    except KeyError as ke:
        logging.error(f'Schema not found for {ds}')
        return
    
def process_files(src_base_dir, tgt_base_dir,ds):
    try:
        for file in glob.glob(f'{src_base_dir}/{ds}/part*'):
            df = pd.read_csv(file,names= get_columns(ds))
            os.makedirs(f'{tgt_base_dir}/{ds}',exist_ok=True)
            df.to_json(
                f'{tgt_base_dir}/{ds}/part-{str(uuid.uuid1())}.json',
                orient='records',
                lines=True
            )
            logging.info(f'no of records processed for {os.path.split(file)[1]} in {ds} is {df.shape}]')
    except KeyError as ke:
        raise()
        

def create_json_files():
    # loglevel = os.environ.setdefault('LOG_LEVEL', str(logging.INFO))
    logging.basicConfig(level=logging.INFO,
                    filename='logs/ffc.log',
                    format='%(levelname)s %(asctime)s %(message)s)',
                    datefmt='%m-%d-%Y %I:%M:%S %p')
    src_base_dir = os.environ['SRC_ENV_DIR'] 
    tgt_base_dir = os.environ['TGT_ENV_DIR']
    logging.info('File format processing started')
    datasets = os.environ.get('DATASETS') 
    if not datasets:   
        for path in glob.glob(f'{src_base_dir}/*'):
            if os.path.isdir(path):
                ds = os.path.split(path)[1]
                process_files(src_base_dir,tgt_base_dir,ds) 
    else:
        dirs = datasets.split(',')
        for ds in dirs:
            try:       
                process_files(src_base_dir,tgt_base_dir,ds)
            except Exception as ex:
                logging.error(f'file format conversion for {ds} is not succesful')
                return 
 
    logging.info('File format processing is successful')


if __name__ == '__main__':
    create_json_files()
