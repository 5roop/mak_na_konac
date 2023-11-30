# mak_na_konac
Repo for tracking our mak na konac progress



Data was transfered from SwitchDrive in a zip and unzipped with `unzip Pescanik_STT.zip`.

A [script](0_prepare_data.py) was setup for unzipping and preprocessing (removing the simple mistakes we discovered earlier) and then a [snakefile](Snakefile) was written to perform the conversion from mp3 to wav.