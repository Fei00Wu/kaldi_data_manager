import os
import sys
import fire
import typing
from pony.orm import *

from utils import *
from setup_db import setup_db


speaker_files = ['spk2gender', 'spk2utt', 'cmvn.scp']
recording_files = ['wav.scp', 'reco2dur']
utterance_files = ['utt2spk', 'text', 'feats.scp', 'utt2dur', 'segments']


def _write_from_table(*, db,
                      data_dir: str,
                      table: str,
                      condition: typing.Callable[['db.Entity'], bool] = lambda _: True) -> None:
    """
    Writes a specified table to Kaldi-style files
    :param data_dir: Full path to Kaldi-style files
    :param table: Can be either Recording, Speaker, or Utterance
    :return: None
    """
    if table == 'Utterance':
        files = utterance_files
        iterator = db.Utterance.select().order_by(lambda utt: utt.utt_id)[:]
    elif table == 'Speaker':
        files = speaker_files
        iterator = db.Speaker.select().order_by(lambda spk: spk.spk_id)[:]
    elif table == 'Recording':
        files = recording_files
        iterator = db.Recording.select().order_by(lambda reco: reco.reco_id)[:]
    else:
        raise ValueError(f"table can be either Utterance, Speaker, or Recording, got {table}")

    handler = {f: open(os.path.join(data_dir, f), 'w+')
               for f in files}

    for row in iterator:
        if condition(row):
            content = row.to_file()
            for file_name, fp in handler.items():
                if file_name in content:
                    fp.write(content[file_name])

    for file_name, fp in handler.items():
        fp.close()


def db2data(db_file: str,
            data_dir: str,
            db_provider: str = 'sqlite', ) -> None:
    """
    Writes a db to Kaldi-style file directory
    :param db_file: Full path to db_file
    :param data_dir: Full path to Kaldi-style directory
    :param db_provider: db type.
    :return: None
    """

    db = setup_db()
    db.bind(provider=db_provider,
            filename=db_file,
            create_db=False)
    db.generate_mapping(create_tables=False)
    if os.path.exists(data_dir):
        print(f"{data_dir} already exists. Remove or rename it before proceed.")
        sys.exit(1)
    os.mkdir(data_dir)

    with db_session:
        _write_from_table(db=db,
                          data_dir=data_dir,
                          table='Recording')
        _write_from_table(db=db,
                          data_dir=data_dir,
                          table='Speaker')
        _write_from_table(db=db,
                          data_dir=data_dir,
                          table='Utterance')

    remove_empty(data_dir)


if __name__ == '__main__':
    fire.Fire(db2data)
