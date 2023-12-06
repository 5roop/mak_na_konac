from pathlib import Path
import subprocess

subprocess.run(
    """
rm -r data/02_Južne_vesti data/02_Južne_vesti_asr;
cd data; unzip 02_Južne_vesti.zip; cd ..;
cp -r data/02_Južne_vesti data/02_Južne_vesti_asr
""",
    shell=True,
    capture_output=False,
)
