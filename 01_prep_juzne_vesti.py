from pathlib import Path
import subprocess

subprocess.run(
    """
rm -r data/02_Južne_vesti data/02_Južne_vesti_asr data/02_Južne_vesti_out;
cd data; unzip 02_Južne_vesti.zip; cd ..;
cp -r data/02_Južne_vesti data/02_Južne_vesti_asr;
cp updated_220404/220404.exb data/02_Južne_vesti/220404/
""",
    shell=True,
    capture_output=False,
)


subprocess.run(
    """
    rm data/02_Južne_vesti_asr/220404/segments/*;
    rm data/02_Južne_vesti_asr/220404/*out*;
    rm data/02_Južne_vesti_asr/220404/*_asr*exb;
    rm data/02_Južne_vesti_asr/220404/*_done;
    cp updated_220404/220404.exb data/02_Južne_vesti_asr/220404/;
    cp updated_220404/220404.exb data/02_Južne_vesti/220404/
    """,
    shell=True,
)
