from os import path
from pathlib import Path

cur_path = path.curdir
name_dir_upload = 'upload'
upload_path = Path(cur_path, name_dir_upload)

file_db_name = "MatrixWER.db"
file_name_Matrix = 'Matrix.csv'
file_name_Zone = 'Zones.csv'

path_DB = Path(upload_path, file_db_name)
path_Matrix = Path(upload_path, file_name_Matrix)
path_Zone = Path(upload_path, file_name_Zone)
