import typing
from pony.orm import *


def setup_db():
    db = Database()

    class Sentence(db.Entity):
        sent_id = PrimaryKey(str)
        text = Required(str)
        length = Required(int)

        utterances = Set("Utterance", reverse='transcript')

        def update(self,
                   field: str,
                   value: str) -> None:
            if field == 'text':
                self.text = value
                self.length = len(self.text.strip().split())
            else:
                raise ValueError(f"Entity Sentence cannot update {field}")

        def make_copy(self, new_db):
            return new_db.Sentence(sent_id=self.sent_id,
                                   text=self.text,
                                   length=self.length)

    class Speaker(db.Entity):
        spk_id = PrimaryKey(str)
        gender = Optional(str)
        cmvn = Optional(str)

        utterances = Set("Utterance", reverse='speaker')

        def update(self,
                   field: str,
                   value: str) -> None:
            if field == 'gender':
                self.gender = value
            elif field == 'cmvn':
                self.cmvn = value
            else:
                raise ValueError(f"Entity Speaker cannot update {field}")

        def get_utt_in_str(self) -> str:
            return ' '.join(sorted(list(u.utt_id for u in self.utterances)))

        def to_file(self) -> typing.Dict[str, str]:
            return {
                'spk2gender': f"{self.spk_id} {self.gender}\n",
                'cmvn.scp': f"{self.spk_id} {self.cmvn}\n",
                'spk2utt': f"{self.spk_id} {self.get_utt_in_str()}\n"
            }

        def make_copy(self, new_db):
            return new_db.Speaker(spk_id=self.spk_id,
                                  gender=self.gender,
                                  cmvn=self.cmvn)

    class Recording(db.Entity):
        reco_id = PrimaryKey(str)
        wav = Required(str)

        corpus = Optional(str)
        duration = Optional(float)

        utterances = Set("Utterance", reverse='recording')

        def update(self, field: str,
                   value: str) -> None:
            if field == 'duration':
                self.duration = float(value)
            elif field == 'wav':
                self.wav = value
            else:
                raise ValueError(f"Entity Recording cannot update {field}")

        def to_file(self) -> typing.Dict[str, str]:
            lines = {'wav.scp': f"{self.reco_id} {self.wav}\n"}
            if self.duration:
                lines['reco2dur'] = f"{self.reco_id} {self.duration}\n"
            return lines

        def make_copy(self, new_db):
            return new_db.Recording(reco_id=self.reco_id,
                                    wav=self.wav,
                                    duration=self.duration,
                                    corpus=self.corpus)

    class Utterance(db.Entity):
        utt_id = PrimaryKey(str)
        recording = Required(Recording, cascade_delete=False)

        feat = Optional(str)
        transcript = Required(Sentence, cascade_delete=False)
        speaker = Required(Speaker, cascade_delete=False)

        duration = Optional(float)

        # Used if utterance is a segment
        start_time = Optional(float)
        end_time = Optional(float)
        is_segment = Required(bool, default=False, sql_default='0')

        def update(self, field: str, value: str):
            if field == 'feat':
                self.feat = value
            elif field == 'duration':
                self.duration = float(value)
            else:
                raise ValueError(f"Entity Utterance cannot update {field}")

        def to_file(self):
            lines = {
                'text': f"{self.utt_id} {self.transcript.text}\n",
                'utt2spk': f"{self.utt_id} {self.speaker.spk_id}\n",
                'feats.scp': f"{self.utt_id} {self.feat}\n"
            }
            if self.is_segment:
                lines['segments'] = f"{self.utt_id} {self.recording.reco_id} {self.start_time} {self.end_time}\n"
            if self.duration:
                lines['utt2dur'] = f"{self.utt_id} {self.duration}\n"
            return lines

        def make_copy(self, new_db):
            if not new_db.Sentence.get(sent_id=self.transcript.sent_id):
                self.transcript.make_copy(new_db)
            if not new_db.Recording.get(reco_id=self.recording.reco_id):
                self.recording.make_copy(new_db)
            if not new_db.Speaker.get(spk_id=self.speaker.spk_id):
                self.speaker.make_copy(new_db)
            new_utt = new_db.Utterance(utt_id=self.utt_id,
                                       speaker=new_db.Speaker[self.speaker.spk_id],
                                       recording=new_db.Recording[self.recording.reco_id],
                                       transcript=new_db.Sentence[self.transcript.sent_id],
                                       feat=self.feat,
                                       duration=self.duration,
                                       start_time=self.start_time,
                                       end_time=self.end_time,
                                       is_segment=self.is_segment)
            new_db.Speaker[self.speaker.spk_id].utterances.add(new_utt)
            new_db.Recording[self.recording.reco_id].utterances.add(new_utt)
            new_db.Sentence[self.transcript.sent_id].utterances.add(new_utt)
            return new_utt

    return db
