import os
import sys
import fire
import typing
from pony.orm import *

from setup_db import setup_db

required_files = ['text', 'wav.scp', 'utt2spk']
optional_files_map = {
    'feats.scp': ('Utterance', 'feat'),
    'utt2dur': ('Utterance', 'duration'),
    'reco2gender': ('Recording', 'duration'),
    'cmvn.scp': ('Speaker', 'cmvn'),
    'spk2gender': ('Speaker', 'gender')
}


def _check_segment(data_dir: str) -> bool:
    """
    Creates dummy segments file if not exists
    :param data_dir: Full path to Kaldi data directory
    :return: Boolean: whether segment file is dummy
    """
    seg_file = os.path.join(data_dir, 'segments')
    text_file = os.path.join(data_dir, 'text')
    if not os.path.exists(seg_file):
        with open(seg_file, 'w+') as seg_fp:
            with open(text_file) as utt_fp:
                for line in utt_fp:
                    utt_id = line.strip().split()[0]
                    seg_fp.write(f"{utt_id} {utt_id} -1.0 -1.0\n")
        return True
    else:
        return False


def _build_recordings(*, wav_file: str,
                      corpus: str, db) -> typing.Dict[str, 'Recording']:
    """
    Builds all recordings from wav.scp
    :param wav_file: Full path to wav file
    :param corpus: Corpus
    :return: Dictionary that maps reco_id/utt_id -> Recording
    """
    recordings = {}
    with open(wav_file, 'r') as fp:
        for line in fp:
            toks = line.strip().split()
            reco_id = toks[0] 
            path = ' '.join(toks[1:])
            recordings[reco_id] = db.Recording(wav=path, corpus=corpus, reco_id=reco_id)
    return recordings


def _build_sentences(*, text: str, db) -> typing.Dict[str, 'Sentence']:
    """
    Builds all sentences from 'text'
    :param text: Full path to Kaldi style text file
    :return: utt_id -> Sentence indexer
    """
    seen_transcripts = {}
    sentences = {}
    with open(text, 'r') as fp:
        for line in fp:
            tokens = line.strip().split()
            utt_id = tokens[0]
            length = len(tokens) - 1
            transcript = ' '.join(tokens[1:])

            if transcript not in seen_transcripts:
                sent_id = f"sent_{str(len(seen_transcripts)).zfill(5)}"
                sentences[utt_id] = db.Sentence(sent_id=sent_id,
                                                text=transcript,
                                                length=length)
                seen_transcripts[transcript] = sentences[utt_id]
            else:
                sentences[utt_id] = seen_transcripts[transcript]
    return sentences


def _build_speakers(*, utt2spk: str, db) -> typing.Dict[str, 'Speaker']:
    """
    Builds speakers from utt2spk
    :param utt2spk: Kaldi style utt2spk file
    :return: utt_id  -> Speaker indexer
    """
    speakers = {}
    seen_speaker = {}
    with open(utt2spk, 'r') as fp:
        for line in fp:
            utt_id, spk_id = line.strip().split()
            if spk_id not in seen_speaker:
                seen_speaker[spk_id] = db.Speaker(spk_id=spk_id)
            speakers[utt_id] = seen_speaker[spk_id]
    return speakers


def _build_utterances(*, segments: str, db,
                      recordings: typing.Dict[str, 'Recording'],
                      sentences: typing.Dict[str, 'Sentence'],
                      speakers: typing.Dict[str, 'Speaker']) -> None:
    """
    Builds all utterances from built Speaker, Sentence, and Recording.
    :param segments: Kaldi style segments file, either provided or a faked dummy.
    :param recordings: Indexer, reco_id -> Recording
    :param sentences: Indexer, utt_id -> Sentence
    :param speakers: Indexer, utt_id -> Speaker
    :return: None
    """
    with open(segments, 'r') as fp:
        for line in fp:
            utt_id, reco_id, start_time, end_time = line.strip().split()
            record = recordings[reco_id]
            spk = speakers[utt_id]
            sent = sentences[utt_id]
            if start_time != '-1.0' and end_time != '-1.0':
                utt = db.Utterance(utt_id=utt_id,
                                   transcript=sent,
                                   speaker=spk,
                                   recording=record,
                                   start_time=float(start_time),
                                   end_time=float(end_time),
                                   duration=float(end_time) - float(start_time),
                                   is_segment=True)
            else:
                utt = db.Utterance(utt_id=utt_id,
                                   transcript=sent,
                                   speaker=spk,
                                   recording=record,
                                   is_segment=False)
            spk.utterances.add(utt)
            sent.utterances.add(utt)
            record.utterances.add(utt)


def _update_db_from_file(*, db,
                         file: str,
                         entity: str,
                         field: str) -> None:
    """
    Updates Table given a Kaldi style file.
    :param file: Kaldi style file
    :param entity: What table to update, can be either Utterance, Speaker, Sentence, or Recording
    :param field: Field/column to update.
    :return: None
    """
    with open(file, 'r') as fp:
        for line in fp:
            tokens = line.strip().split()
            index = tokens[0]
            value = ' '.join(tokens[1:])
            if entity == 'Sentence':
                db.Sentence[index].update(field, value)
            elif entity == 'Utterance':
                db.Utterance[index].update(field, value)
            elif entity == 'Speaker':
                db.Speaker[index].update(field, value)
            elif entity == 'Recording':
                db.Recording[index].update(field, value)
            else:
                raise ValueError(f"Unknown entity {entity}")


def main(data_dir: str,
         db_file: str = None,
         db_provider: str = 'sqlite',
         corpus: str = None) -> None:
    db_file = db_file if db_file is not None else os.path.basename(data_dir)
    # Set up database
    if os.path.exists(db_file):
        print(f"{db_file} already exist. Remove or rename it before proceed.")
        sys.exit(1)

    db = setup_db()
    db.bind(provider=db_provider,
            filename=db_file,
            create_db=True)
    db.generate_mapping(create_tables=True)

    assert all(os.path.exists(os.path.join(data_dir, f)) for f in required_files), \
        f"Required these file to exist in {data_dir}:\n\t{required_files}"

    corpus = corpus if corpus else os.path.basename(data_dir)
    dummy_segment = _check_segment(data_dir)

    # Build from required files
    with db_session:
        recordings = _build_recordings(wav_file=os.path.join(data_dir, 'wav.scp'),
                                       corpus=corpus,
                                       db=db)
        sentences = _build_sentences(text=os.path.join(data_dir, 'text'),
                                     db=db)
        speakers = _build_speakers(utt2spk=os.path.join(data_dir, 'utt2spk'),
                                   db=db)
        _build_utterances(recordings=recordings,
                          speakers=speakers,
                          sentences=sentences,
                          segments=os.path.join(data_dir, 'segments'),
                          db=db)

        # Build from optional files
        for file, t in optional_files_map.items():
            entity, field = t
            file = os.path.join(data_dir, file)
            if os.path.exists(file):
                _update_db_from_file(db=db,
                                     file=file,
                                     entity=entity,
                                     field=field)

        commit()
        if dummy_segment:
            os.remove(os.path.join(data_dir, 'segments'))


if __name__ == '__main__':
    fire.Fire(main)
