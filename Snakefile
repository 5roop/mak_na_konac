from pathlib import Path
import subprocess


all_ids = [i.name for i in Path("data/Pescanik_STT/").glob('[0-9]' * 6)]


rule gather:
    default_target: True
    input: expand("data/Pescanik_STT/{num}/{num}.wav", num=all_ids)
    run:
        for num in all_ids:
            p = Path(f"data/Pescanik_STT/{num}/{num}.exb")
            t = p.read_text()
            t = t.replace(f"{num}.mp3", f"{num}.wav")
            p.write_text(t)

rule transform_wav:
    input: "data/Pescanik_STT/{num}/{num}.mp3"
    output: "data/Pescanik_STT/{num}/{num}.wav"
    threads: 5
    shell:
        """
        ffmpeg -i {input} -ac 1 -ar 16000 -acodec pcm_s16le {output}
        """
