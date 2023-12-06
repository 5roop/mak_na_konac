import lxml.etree as ET
from pathlib import Path
from pydub import AudioSegment
from scipy.io import wavfile
import pandas as pd
import numpy as np
import snakemake
import logging


class FishyInputError(Exception):
    pass


def fix_excel(inpath: str, outpath: str, history: list[dict]):
    from pandas import read_excel

    df = read_excel(inpath)
    if df.shape[1] == 3:
        raise FishyInputError("There seem to be only 3 columns, expected 4.")
    if "emisija" not in df.columns:
        df = read_excel(inpath, names="emisija start end govornik".split(), header=None)

    for i, row in df.iterrows():
        for mapper in history:
            if not (
                (mapper["old_start"] == row["start"])
                and (mapper["old_end"] == row["end"])
            ):
                continue
            df.loc[i, "start"] = mapper["new_start"]
            df.loc[i, "end"] = mapper["new_end"]
    df.drop_duplicates().to_excel(outpath, index=False)


def process_file_label_only(inpath: Path | str, outpath: Path | str) -> None:
    inpath = Path(inpath)
    audio_path = inpath.with_suffix(".wav")
    audio = AudioSegment.from_wav(audio_path)
    sample_rate, data = wavfile.read(audio_path)
    data = data - int(data.mean())
    data = data.astype(np.int32)
    doc = ET.fromstring(inpath.read_bytes())
    timeline_element = doc.find(".//common-timeline")
    timeline_dict = {
        tli.get("id"): tli.get("time") for tli in timeline_element.findall(".//tli")
    }

    for event in doc.findall(".//{*}event"):
        event.set("start_s", timeline_dict[event.get("start")])
        event.set("end_s", timeline_dict[event.get("end")])
    dfs = []
    for tier in list(doc.findall(".//{*}tier")):
        tier_events = [{**i.attrib, "text": i.text} for i in tier.findall(".//event")]
        if not tier_events:
            continue
        df = pd.DataFrame(tier_events)
        df["start_s"] = df.start_s.astype(float)
        df["end_s"] = df.end_s.astype(float)
        df = df.sort_values(by="start_s")

        def get_rms_20_20(end_s):
            left_ms_window = 20
            right_ms_window = 20

            lower_bound_s = end_s - left_ms_window * 1e-3
            upper_bound_s = end_s + right_ms_window * 1e-3

            lower_index = int(sample_rate * lower_bound_s)
            upper_index = int(sample_rate * upper_bound_s)
            upper_index = min(upper_index, data.shape[0])
            subset = data[lower_index:upper_index]
            return np.sqrt(np.square(subset).mean())

        def get_rms_whole_segment(start_s, end_s):
            lower_bound_s = start_s
            upper_bound_s = end_s

            lower_index = int(sample_rate * lower_bound_s)
            upper_index = int(sample_rate * upper_bound_s)
            subset = data[lower_index:upper_index]
            return np.sqrt(np.square(subset).mean())

        df["rms_20_20"] = df.end_s.apply(get_rms_20_20).fillna(99999).astype(int)
        df["rms_segment"] = df.apply(
            lambda row: get_rms_whole_segment(row["start_s"], row["end_s"]), axis=1
        ).astype(int)
        df["bad"] = ""
        df.loc[df.rms_20_20 > df.rms_segment / 2, "bad"] = "bad"
        dfs.append(df)
    df = pd.concat(dfs).sort_values(by="start_s")
    newtier = ET.Element("tier")
    for key, value in tier.attrib.items():
        newtier.set(key, value)
    newtier.set("id", "bad_segments")
    newtier.set("display-name", "bad segments")
    for i, row in df.iterrows():
        if not row["bad"]:
            continue
        event = ET.Element("event")
        event.set("start", row["start"])
        event.set("end", row["end"])
        event.text = "bad ->"
        newtier.append(event)
    tier.addnext(newtier)
    ET.indent(doc, space="\t")
    doc.getroottree().write(
        Path(outpath),
        pretty_print=True,
        encoding="utf8",
        xml_declaration='<?xml version="1.0" encoding="UTF-8"?>',
    )
    Path(outpath).write_text(
        Path(outpath)
        .read_text()
        .replace(
            "<?xml version='1.0' encoding='UTF8'?>",
            '<?xml version="1.0" encoding="UTF-8"?>',
        )
    )


def process_file_join_and_label(inpath: Path | str, outpath: Path | str) -> list:
    history = []
    inpath = Path(inpath)
    audio_path = inpath.with_suffix(".wav")
    audio = AudioSegment.from_wav(audio_path)
    sample_rate, data = wavfile.read(audio_path)
    data = data - int(data.mean())
    data = data.astype(np.int32)
    doc = ET.fromstring(inpath.read_bytes())
    timeline_element = doc.find(".//common-timeline")
    timeline_dict = {
        tli.get("id"): tli.get("time") for tli in timeline_element.findall(".//tli")
    }

    for event in doc.findall(".//{*}event"):
        event.set("start_s", timeline_dict[event.get("start")])
        event.set("end_s", timeline_dict[event.get("end")])
    dfs = []
    for tier in list(doc.findall(".//{*}tier")):
        tier_events = [{**i.attrib, "text": i.text} for i in tier.findall(".//event")]
        if not tier_events:
            continue
        df = pd.DataFrame(tier_events)
        df["start_s"] = df.start_s.astype(float)
        df["end_s"] = df.end_s.astype(float)
        df = df.sort_values(by="start_s")

        def get_rms_20_20(end_s):
            left_ms_window = 20
            right_ms_window = 20

            lower_bound_s = end_s - left_ms_window * 1e-3
            upper_bound_s = end_s + right_ms_window * 1e-3

            lower_index = int(sample_rate * lower_bound_s)
            upper_index = int(sample_rate * upper_bound_s)
            upper_index = min(upper_index, data.shape[0])
            subset = data[lower_index:upper_index]
            return np.sqrt(np.square(subset).mean())

        def get_rms_whole_segment(start_s, end_s):
            lower_bound_s = start_s
            upper_bound_s = end_s

            lower_index = int(sample_rate * lower_bound_s)
            upper_index = int(sample_rate * upper_bound_s)
            subset = data[lower_index:upper_index]
            return np.sqrt(np.square(subset).mean())

        df["rms_20_20"] = df.end_s.apply(get_rms_20_20).fillna(99999).astype(int)
        df["rms_segment"] = df.apply(
            lambda row: get_rms_whole_segment(row["start_s"], row["end_s"]), axis=1
        ).astype(int)
        df["bad"] = ""
        df.loc[df.rms_20_20 > df.rms_segment / 2, "bad"] = "bad"
        # Real processing:
        df["join_with"] = [list() for i in range(df.shape[0])]
        df["to_skip"] = ""
        df["continues"] = df.start.shift(-1) == df.end
        df["terminates_merge_badly"] = False
        df["new_bad"] = df.bad.values
        skip_indices = []
        for i, row in df.iterrows():
            if i in skip_indices:
                continue
            if not row["continues"]:
                continue
            # total_duration = row["end_s"] - row["start_s"]
            if not (i + 1 < df.shape[0]):
                continue
            if row["bad"]:
                df.loc[i, "join_with"].append(i + 1)
                skip_indices.append(i + 1)
                df.loc[i, "new_bad"] = ""
                if not (i + 2 < df.shape[0]):
                    continue
                if df.loc[i + 1, "bad"] and df.loc[i + 1, "continues"]:
                    df.loc[i, "join_with"].append(i + 2)
                    df.loc[i + 1, "new_bad"] = ""
                    skip_indices.append(i + 2)
                    if df.loc[i + 2, "bad"]:
                        df.loc[i + 2, "terminates_merge_badly"] = True
        df["to_skip"] = df.index.isin(skip_indices)
        for i, row in df.iterrows():
            if row["to_skip"]:
                continue
            if not row["join_with"]:
                continue
            new_element_bounds = [i] + row["join_with"]
            new_start = df.loc[new_element_bounds, "start"].values[0]
            new_end = df.loc[new_element_bounds, "end"].values[-1]
            for idx in new_element_bounds:
                start = df.loc[idx, "start"]
                end = df.loc[idx, "end"]
                el = doc.find(f".//event[@start='{start}'][@end='{end}']")
                assert el is not None, "Could not find this event!"
                if idx == new_element_bounds[0]:
                    start_index = el.getparent().index(el)
                el.getparent().remove(el)
                history.append(
                    {
                        "old_start": start,
                        "new_start": new_start,
                        "old_end": end,
                        "new_end": new_end,
                    }
                )
            newevent = ET.Element("event")
            newevent.set("start", new_start)
            newevent.set("end", new_end)
            newevent.set(
                "start_s", df.loc[new_element_bounds, "start_s"].astype(str).values[0]
            )
            newevent.set(
                "end_s", df.loc[new_element_bounds, "end_s"].astype(str).values[-1]
            )
            newevent.text = " ".join(df.loc[new_element_bounds, "text"].values)
            tier.insert(start_index, newevent)
        tier[:] = sorted(tier, key=lambda child: float(child.get("start_s")))
        dfs.append(df)
    df = pd.concat(dfs).sort_values(by="start_s")

    newtier = ET.Element("tier")
    for key, value in tier.attrib.items():
        newtier.set(key, value)
    newtier.set("id", "bad_segments")
    newtier.set("display-name", "bad segments")
    for i, row in df.iterrows():
        if row["new_bad"]:
            event = ET.Element("event")
            event.set("start", row["start"])
            event.set("end", row["end"])
            event.text = "bad ->"
            newtier.append(event)
    tier.addnext(newtier)
    ET.indent(doc, space="\t")
    doc.getroottree().write(
        Path(outpath),
        pretty_print=True,
        encoding="utf8",
        xml_declaration='<?xml version="1.0" encoding="UTF-8"?>',
    )
    Path(outpath).write_text(
        Path(outpath)
        .read_text()
        .replace(
            "<?xml version='1.0' encoding='UTF8'?>",
            '<?xml version="1.0" encoding="UTF-8"?>',
        )
    )
    return history


if __name__ == "__main__":
    inpath = "data/Pescanik_STT/150918/150918.exb"
    outpath = "test.exb"
    history = process_file_join_and_label(inpath, outpath)
    inpath = Path(inpath).with_suffix(".xlsx")
    fix_excel(inpath, "test.xlsx", history)
