from pathlib import Path
import pandas as pd
import subprocess

subprocess.run(["mkdir", "-p", "csv_output"])

all_ids = [i.name for i in Path("data/02_Južne_vesti_asr/").glob("[0-9]" * 6)]

for idx in all_ids:
    whisper = Path("data/02_Južne_vesti_asr") / idx / (idx + ".out.whisper")
    whisper_sagic = Path("data/02_Južne_vesti_asr") / idx / (idx + ".out.whisper_sagic")

    dfw = pd.read_csv(whisper)
    dfw = dfw.rename(columns={"transcript": "vanilla-whisper"})

    dfs = pd.read_csv(whisper_sagic)
    dfs = dfs.rename(columns={"transcript": "whisper-sagicc"})
    new = dfw.merge(dfs, on="file")

    new.to_csv(f"csv_output/{idx}.csv", index=False)
