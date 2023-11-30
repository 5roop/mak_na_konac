from pathlib import Path
import subprocess

import subprocess
subprocess.run("""
mv 'data/Pescanik_STT/211006/Protesti u Ljubljani.mp3' 'data/Pescanik_STT/211006/211006.mp3';
mv 'data/Pescanik_STT/131122/131222.mp3' 'data/Pescanik_STT/131122/131122.mp3';
mv 'data/Pescanik_STT/221210/221210r.mp3' 'data/Pescanik_STT/221210/221210.mp3';
mv 'data/Pescanik_STT/221210/221210r.exb' 'data/Pescanik_STT/221210/221210.exb';
""", shell=True)

for p in [Path("data/Pescanik_STT/131122/131122.exb")]:
    t = p.read_text()
    t = t.replace('<referenced-file url="131222.mp3"/>', '<referenced-file url="131122.mp3"/>')
    p.write_text(t)
for p in [Path("data/Pescanik_STT/211006/211006.exb")]:
    t = p.read_text()
    t = t.replace('<referenced-file url="Protesti%20u%20Ljubljani.mp3"/>', '<referenced-file url="211006.mp3"/>')
    p.write_text(t)
for p in [Path("data/Pescanik_STT/181203/181203.exb")]:
    t = p.read_text()
    t = t.replace('<referenced-file url="Peščanik/181203/181203.mp3"/>', '<referenced-file url="181203.mp3"/>')
    p.write_text(t)
for p in [Path("data/Pescanik_STT/221219/221219.exb")]:
    t = p.read_text()
    t = t.replace('<referenced-file url="Pozdrav%20za%20Borču.mp3"/>', '<referenced-file url="221219.mp3"/>')
    p.write_text(t)


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
    shell:
        """
        ffmpeg -i {input} -ac 1 -ar 16000 -acodec pcm_s16le {output}
        """
