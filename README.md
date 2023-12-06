# mak_na_konac
Repo for tracking our mak na konac progress



Data was transfered from SwitchDrive in a zip and unzipped with `unzip Pescanik_STT.zip`.

A [script](0_prepare_data.py) was setup for unzipping and preprocessing (removing the simple mistakes we discovered earlier) and then a [snakefile](Snakefile) was written to perform the conversion from mp3 to wav.

#  Ideas on resplitting 2023-11-30T14:18:42

Speech on the edges of segments can be quite nicely seen when looking at the RMS in the last 20ms of the segment. A reasonable metric seems to be RMS of the last 20ms vs RMS of the whole segment. If the ratio of the two is above 0.5, there is speech at the edge.

To improve results, the window could be extended into the next segment, so that RMS would be calculated on the last 20ms of the segment and the first 20ms of the next segment. This then 

# Meeting notes 2023-11-30T14:52:24
Write a mail to Zurich explaining next steps:
* Keep rms_20_20 metric, ratio of 2.
* Label the segments with bad ends  as bad in a new tier
* Deliver to Zurich
* Annotators can then decide to fix it or leave it be.
* What won't be manually fixed, we'll merge together automatically.

# 2023-12-04T09:42:35
Tanja proposes we will do the merging.

Mira delivered Južne vesti corpus. All the transcriptions are missing for now, is this a bug or a feature? In Južne Vesti some segments will likely also have to be joined.

Proposed response to Tanja: criteria:
* Never merge between two speakers
* Do not merge if the total length would then be > 20s

# 2023-12-05T12:46:41

Implemented changes: in accordance with the email correspondence we now merge segments of the same speaker, but at most 3 at the time.

This was implemented and has been sent to Zurich. 

Further work: fix the excel files that now relate to unexisting spans.

# 2023-12-06T20:46:11

TODO:
* link the audio properly in Južne Vesti.