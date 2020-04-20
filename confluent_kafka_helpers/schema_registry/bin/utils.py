import glob
from pathlib import Path


def is_key(schema_file: str, key_schema_postfix='-key') -> bool:
    return Path(schema_file).stem.endswith(key_schema_postfix)


def get_schema_files(folder, extension='.avsc') -> list:
    return (
        (schema_file, is_key(schema_file=schema_file))
        for schema_file in glob.glob(f'{folder}/*{extension}')
    )
