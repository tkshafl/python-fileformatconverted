import json
import uuid
import glob
import pandas as pd
import os

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
    except KeyError:
        print(f'Schema not found for {ds}')
        return
    
def process_files(src_base_dir, tgt_base_dir,ds):
    for file in glob.glob(f'{src_base_dir}/{ds}/part*'):
        print(file)
        df = pd.read_csv(file,names= get_columns(ds))
        os.makedirs(f'{tgt_base_dir}/{ds}',exist_ok=True)
        df.to_json(
            f'{tgt_base_dir}/{ds}/part-{str(uuid.uuid1())}.json',
            orient='records',
            lines=True
        )
        print(f'no of records processed for {os.path.split(file)[1]} in {ds} is {df.shape}]')

def create_json_files():
    src_base_dir = os.environ['SRC_ENV_DIR'] 
    tgt_base_dir = os.environ['TGT_ENV_DIR']    
    for path in glob.glob(f'{src_base_dir}/*'):
        if os.path.isdir(path):
            ds = os.path.split(path)[1]
            process_files(src_base_dir,tgt_base_dir,ds)      

if __name__ == '__main__':
    create_json_files()
