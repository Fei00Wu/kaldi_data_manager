import os
import fire
import typing

from utils import *
from setup_db import *
from db2data import db2data

train_dev_test = {
    'train': 0.7,
    'dev': 0.15,
    'test': 0.15
}


def split_db(db_file: str,
             split_by: str,
             data_dir: str = None,
             db_provider: str = 'sqlite',
             split_ratio: typing.Optional[typing.Dict[str, float]] = None,
             save_db: bool = True,
             save_data: bool = True) -> None:
    db_original = setup_db()
    db_original.bind(provider=db_provider,
                     filename=db_file,
                     create_db=False)
    db_original.generate_mapping(create_tables=False)

    split_ratio = train_dev_test if split_ratio is None else split_ratio
    db_indexer = []
    subset_ratio = []
    subset_names = []
    subset_dbs = []

    for n, r in split_ratio.items():
        subset_names.append(n)
        subset_ratio.append(r)
        db_indexer.append(setup_db())
        new_db_file = os.path.join(
            os.path.dirname(db_file),
            f"{'.'.join(os.path.basename(db_file).split('.')[:-1])}_{split_by}_{n}.db"
        )
        db_indexer[-1].bind(provider=db_provider,
                            filename=new_db_file,
                            create_db=True)
        db_indexer[-1].generate_mapping(create_tables=True)
        subset_dbs.append(new_db_file)

    with db_session:
        if split_by == 'utt':
            num_rows = count(utt for utt in db_original.Utterance)
            assignments = random_assignment(ratio=subset_ratio, num_id=num_rows)
            index2assignment = {utt.utt_id: assignment
                                for assignment, utt in zip(assignments,
                                                           db_original.Utterance
                                                           .select()
                                                           .order_by(lambda utt: utt.utt_id)[:])}

            def get_assignment(u):
                return index2assignment[u.utt_id]

        elif split_by == 'spk':
            num_rows = count(utt for utt in db_original.Speaker)
            assignments = random_assignment(ratio=subset_ratio, num_id=num_rows)
            index2assignment = {spk.spk_id: assignment
                                for assignment, spk in zip(assignments,
                                                           db_original.Speaker
                                                           .select()
                                                           .order_by(lambda spk: spk.spk_id)[:])}

            def get_assignment(u):
                return index2assignment[u.speaker.spk_id]

        elif split_by == 'sent':
            num_rows = count(utt for utt in db_original.Sentence)
            assignments = random_assignment(ratio=subset_ratio, num_id=num_rows)
            index2assignment = {sent.sent_id: assignment
                                for assignment, sent in zip(assignments,
                                                            db_original.Sentence
                                                            .select()
                                                            .order_by(lambda sent: sent.sent_id)[:])}

            def get_assignment(u):
                return index2assignment[u.transcript.sent_id]
        else:
            raise ValueError(f"split_by can be either 'spk', 'utt', or 'sent'. Got {split_by}")

        for u in db_original.Utterance.select().order_by(lambda u: u.utt_id):
            tgt_db = db_indexer[get_assignment(u)]
            if not tgt_db.Utterance.get(utt_id=u.utt_id):
                u.make_copy(tgt_db)

        commit()
    if save_data:
        if not data_dir:
            raise ValueError(f"data_dir required when save_data is True")
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        for sub_db, sub_name in zip(subset_dbs, subset_names):
            sub_data_dir = os.path.join(data_dir, sub_name)
            db2data(db_file=sub_db, data_dir=sub_data_dir)

    if not save_db:
        for sub_db in subset_dbs:
            os.remove(sub_db)


if __name__ == '__main__':
    import random
    import numpy as np

    random.seed(3)
    np.random.seed(3)
    fire.Fire(split_db)
