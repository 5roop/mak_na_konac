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


# def get_excel(path: str):
#     from pandas import read_excel

#     df = read_excel(path)
#     if df.shape[1] == 3:
#         raise FishyInputError("There seem to be only 3 columns, expected 4.")
#     if "emisija" not in df.columns:
#         df = read_excel(path, names="emisija start end govornik".split(), header=None)

#     exbpath = Path(path).with_suffix(".exb")
#     df["start"] = df.start.str.rstrip().str.lstrip()
#     df["end"] = df.end.str.rstrip().str.lstrip()
#     timeline = get_timestamps_from_exb(exbpath)
#     starts = [timeline.get(i) for i in df.start]
#     ends = [timeline.get(i) for i in df.end]
#     df["start_s"] = starts
#     df["end_s"] = ends

#     if "150918" in str(path):
#         c = df.index.isin([65, 66])
#         df = df[~c]
#         logging.warning(f"Skipping the weird {c.sum()} lines for 150918 as agreed")
#     assert not df.start_s.isna().any(), "Missing values in start_s"
#     assert not df.end_s.isna().any(), "Missing values in end_s"
#     assert (df.end_s > df.start_s).all(), "Segments start after they end"
#     return df


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


def process_file_join_and_label(inpath: Path | str, outpath: Path | str) -> None:
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

                if not (i + 2 < df.shape[0]):
                    continue
                if df.loc[i + 2, "bad"] and df.loc[i + 1, "continues"]:
                    df.loc[i, "join_with"].append(i + 2)
                    skip_indices.append(i + 2)
        df["to_skip"] = df.index.isin(skip_indices)
        for i, row in df.iterrows():
            if row["to_skip"]:
                continue
            if not row["join_with"]:
                continue
            new_element_bounds = [i] + row["join_with"]
            for idx in new_element_bounds:
                start = df.loc[idx, "start"]
                end = df.loc[idx, "end"]
                el = doc.find(f".//event[@start='{start}'][@end='{end}']")
                assert el is not None, "Could not find this event!"
                if idx == new_element_bounds[0]:
                    start_index = el.getparent().index(el)
                el.getparent().remove(el)
            newevent = ET.Element("event")
            newevent.set("start", df.loc[new_element_bounds, "start"].values[0])
            newevent.set("end", df.loc[new_element_bounds, "end"].values[-1])
            newevent.set(
                "start_s", df.loc[new_element_bounds, "start_s"].astype(str).values[0]
            )
            newevent.set(
                "end_s", df.loc[new_element_bounds, "end_s"].astype(str).values[-1]
            )
            newevent.text = " ".join(df.loc[new_element_bounds, "text"].values).replace(
                "  ", " "
            )
            tier.insert(start_index, newevent)
        tier[:] = sorted(tier, key=lambda child: float(child.get("start_s")))
        dfs.append(df)
    df = pd.concat(dfs).sort_values(by="start_s")

    newtier = ET.Element("tier")
    for key, value in tier.attrib.items():
        newtier.set(key, value)
    newtier.set("id", "bad_segments")
    newtier.set("display-name", "bad segments")
    for i, row in df[(df.bad == "bad") & (df.join_with.apply(bool))].iterrows():
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


if __name__ == "__main__":
    inpath = "data/Pescanik_STT/150918/150918.exb"
    outpath = "test.exb"
    process_file_join_and_label(inpath, outpath)
