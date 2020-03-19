# Kaldi Data Manager
Manage Kaldi-style data as [ORM](https://en.wikipedia.org/wiki/Object-relational_mapping) 
for easier data management (train-dev split, ect).

### Example usage:
- Convert Kaldi-style data directory to database
```bash
python3 data2db.py --data_dir "data/cmu_kids" --db_file "data/cmu.db" 
```
- Convert database to Kaldi-style directory
``` bash
python3 db2data.py --db_file "data/cmu_train.db" --data_dir "data/cmu_kids_train"
```
- Split database by transcribed sentence (reserved certain amount of sentence). Also supports splitting 
by "spk"(reserves certain amount of speakers), and splitting by "utt" (reserves certain amount of utterances).
```bash
python3 split_db.py --db_file "data/cmu.db" \
  --split_by "sent" --data_dir "data/cmu_sent" \
  --split_ratio "{'train':0.7, 'dev': 0.15, 'test': 0.15}" 
```
