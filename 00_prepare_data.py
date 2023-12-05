from pathlib import Path
import subprocess

subprocess.run(
    """
rm -r data/Pescanik_STT;
rm -r data/Pescanik_STT_labelled;
rm -r data/Pescanik_STT_labelled_and_merged;
cd data; unzip Pescanik_STT.zip; rm Pescanik_STT/Spisak*; rm -r Pescanik_STT/output*; cd ..;
mv 'data/Pescanik_STT/211006/Protesti u Ljubljani.mp3' 'data/Pescanik_STT/211006/211006.mp3';
mv 'data/Pescanik_STT/131122/131222.mp3' 'data/Pescanik_STT/131122/131122.mp3';
mv 'data/Pescanik_STT/221210/221210r.mp3' 'data/Pescanik_STT/221210/221210.mp3';
mv 'data/Pescanik_STT/221210/221210r.exb' 'data/Pescanik_STT/221210/221210.exb';
""",
    shell=True,
    capture_output=True,
)

# Remove mistakes
for p in [Path("data/Pescanik_STT/131122/131122.exb")]:
    t = p.read_text()
    t = t.replace(
        '<referenced-file url="131222.mp3"/>', '<referenced-file url="131122.mp3"/>'
    )
    p.write_text(t)
for p in [Path("data/Pescanik_STT/211006/211006.exb")]:
    t = p.read_text()
    t = t.replace(
        '<referenced-file url="Protesti%20u%20Ljubljani.mp3"/>',
        '<referenced-file url="211006.mp3"/>',
    )
    p.write_text(t)
for p in [Path("data/Pescanik_STT/181203/181203.exb")]:
    t = p.read_text()
    t = t.replace(
        '<referenced-file url="Peščanik/181203/181203.mp3"/>',
        '<referenced-file url="181203.mp3"/>',
    )
    p.write_text(t)
for p in [Path("data/Pescanik_STT/221219/221219.exb")]:
    t = p.read_text()
    t = t.replace(
        '<referenced-file url="Pozdrav%20za%20Borču.mp3"/>',
        '<referenced-file url="221219.mp3"/>',
    )
    p.write_text(t)

subprocess.run(["snakemake", "-j", "20", "-s", "Snakefile_transform_wav"], check=True)


# At this point the data is prepped.

subprocess.run(
    "cp -r data/Pescanik_STT data/Pescanik_STT_labelled", shell=True, check=True
)
subprocess.run(
    "cp -r data/Pescanik_STT data/Pescanik_STT_labelled_and_merged",
    shell=True,
    check=True,
)
subprocess.run(
    "rm data/Pescanik_STT_labelled_and_merged/*/*.exb", shell=True, check=True
)
subprocess.run("snakemake -s Snakefile_label_and_fix -j 20", shell=True, check=True)

subprocess.run("rm data/Pescanik_STT_labelled/*/*.exb", shell=True, check=True)
subprocess.run("snakemake -s Snakefile_label_only -j 20", shell=True, check=True)
