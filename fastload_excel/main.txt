import pickle
import pandas as pd
import os
import hashlib


import time

METAPATH = "./input/metadata"
INPUTPATH = "./input"
TEMPPATH = "./input/temp"


class fileHashMap:
    def __init__(self, METAPATH, INPUTPATH, TEMPPATH):
        self.METAPATH = METAPATH
        self.INPUTPATH = INPUTPATH
        self.TEMPPATH = TEMPPATH
        self.metafilename = "filemeta.pkl"

    def create_load_hashmap(self):
        if not os.listdir(METAPATH) or self.metafilename not in os.listdir(METAPATH):
            metadf = pd.DataFrame(
                None,
                columns=["filename", "original_filepath", "pickle_filepath", "hash"],
            )
            metadf.to_pickle(os.path.join(METAPATH, self.metafilename))
            return metadf
        else:
            return pd.read_pickle(os.path.join(METAPATH, self.metafilename))

    def update_hashmap(self, df):
        try:
            df.to_pickle(os.path.join(METAPATH, self.metafilename))
        except Exception as e:
            print(e)
        return

    def calculate_filehash(self, filepath):
        md5_hash = hashlib.md5()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
            return md5_hash.hexdigest()

    def load_input(self):
        hm = self.create_load_hashmap()
        c = 0
        for files in os.scandir(INPUTPATH):
            if files.path.endswith(".xlsx"):
                # Vars
                filename = files.path.split("/")[2].split(".")[0]
                original_filepath = files.path
                file_hash = self.calculate_filehash(original_filepath)
                lookup = hm.loc[hm["original_filepath"] == original_filepath]
                # Check if file has ever been read before
                if lookup.shape[0] > 2:
                    print(f"Duplicate entry found.")
                    continue
                elif lookup.empty:
                    print(f"{filename} not seen before. Creating pickle.")
                    df = pd.read_excel(original_filepath, index_col=False)
                    pkl_filepath = os.path.join(TEMPPATH, filename + ".pkl")
                    df.to_pickle(pkl_filepath)
                    hm.loc[hm.shape[0]] = [
                        filename,
                        original_filepath,
                        pkl_filepath,
                        file_hash,
                    ]
                else:
                    current_file_hash = lookup["hash"].values[0]
                    if file_hash != current_file_hash:
                        c += 1
                        print(f"{filename} has changed. Updating pickle file.")
                        # File hash changed
                        df = pd.read_excel(original_filepath, index_col=False)
                        pkl_filepath = os.path.join(TEMPPATH, filename + ".pkl")
                        df.to_pickle(pkl_filepath)
                        hm.loc[hm["hash"] == current_file_hash, "hash"] = file_hash
                    else:
                        continue
        print(f"Input loaded and hashmap updated, {c} were updated.")
        self.update_hashmap(hm)
        return hm


start_time = time.time()
main_df = pd.DataFrame(None)
for files in os.scandir(INPUTPATH):
    if files.path.endswith(".xlsx"):
        temp = pd.read_excel(files.path)
        main_df = pd.concat([main_df, temp], axis=0)
print(main_df.shape)
print(f"Looping through excel files runtime: {round(time.time()-start_time,2)}s")
start_time = time.time()
fhm = fileHashMap(METAPATH, INPUTPATH, TEMPPATH)
hm = fhm.load_input()
main_df = pd.DataFrame(None)
for i, k in hm.iterrows():
    temp = pd.read_pickle(k["pickle_filepath"])
    main_df = pd.concat([main_df, temp], axis=0)
print(main_df.shape)
print(f"Optimized file run: {round(time.time()-start_time,2)}s")
