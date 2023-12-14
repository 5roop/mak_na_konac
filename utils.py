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



def parse_excel(inpath: str):
    from pandas import read_excel

    df = read_excel(inpath)
    if df.shape[1] == 3:
        if "start" not in df.columns:
            df = read_excel(inpath, names="start end govornik".split(), header=None)
        df["emisija"] = Path(inpath).with_suffix("").name
    if "start" not in df.columns:
        df = read_excel(inpath, names="emisija start end govornik".split(), header=None)
    return df


def fix_excel(inpath: str, outpath: str, history: list[dict]):

    df = parse_excel(inpath)

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


def process_file_label_only(inpath: Path, outpath: Path) -> None:
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


def process_file_join_and_label(inpath: Path, outpath: Path) -> list:
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


def add_results_to_exb(
    exb_path: str, asr_path: str, modelname: str = "whisper", outpath: str = ""
):
    from transliterate import translit

    asr = pd.read_csv(asr_path)
    asr["file"] = asr.file.apply(lambda s: Path(s).with_suffix("").name)
    asr["file_fragments"] = asr.file.str.split("_")
    asr["start"] = asr.file_fragments.apply(lambda l: l[0])
    asr["end"] = asr.file_fragments.apply(lambda l: l[-1])
    asr["transcript"] = asr.transcript.apply(lambda s: translit(s, "sr", reversed=True))
    exb = ET.fromstring(Path(exb_path).read_bytes())
    timeline_element = exb.find(".//common-timeline")
    timeline_dict = {
        tli.get("id"): tli.get("time")
        for tli in timeline_element.findall(".//tli[@time]")
    }
    last_tier = exb.findall(".//{*}tier")[-1]

    new_tier = ET.Element("tier")
    new_tier.set("id", modelname)
    # new_tier.set("speaker", last_tier.get("speaker"))
    new_tier.set("category", "v")
    new_tier.set("type", "t")
    new_tier.set("display-name", f"ASR - {modelname}")

    for i, row in asr.iterrows():
        event = ET.Element("event")
        event.set("start", row["start"])
        event.set("end", row["end"])
        event.text = row["transcript"]
        new_tier.append(event)

    new_tier[:] = sorted(
        new_tier, key=lambda child: float(timeline_dict.get(child.get("start")))
    )
    i = last_tier.getparent().index(last_tier)
    last_tier.getparent().insert(i + 1, new_tier)
    ET.indent(exb, space="\t")
    exb.getroottree().write(
        outpath,
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




def add_results_to_exb_filtered(
    exb_path: str, asr_path: str,   xlsx_path: str, modelname: str = "whisper",  outpath: str = ""
):
    from transliterate import translit
    exceldf = parse_excel(xlsx_path)
    asr = pd.read_csv(asr_path)
    asr["file"] = asr.file.apply(lambda s: Path(s).with_suffix("").name)
    asr["file_fragments"] = asr.file.str.split("_")
    asr["start"] = asr.file_fragments.apply(lambda l: l[0])
    asr["end"] = asr.file_fragments.apply(lambda l: l[-1])
    asr["transcript"] = asr.transcript.apply(lambda s: translit(s, "sr", reversed=True))
    # filtering:
    merged_asr = asr.merge(exceldf["start end".split()], on=["start", "end"], how="right").drop_duplicates(subset="start end transcript".split(), keep="first").reset_index(drop=True)
    exb = ET.fromstring(Path(exb_path).read_bytes())
    timeline_element = exb.find(".//common-timeline")
    timeline_dict = {
        tli.get("id"): tli.get("time")
        for tli in timeline_element.findall(".//tli[@time]")
    }
    last_tier = exb.findall(".//{*}tier")[-1]

    new_tier = ET.Element("tier")
    new_tier.set("id", modelname)
    # new_tier.set("speaker", last_tier.get("speaker"))
    new_tier.set("category", "v")
    new_tier.set("type", "t")
    new_tier.set("display-name", f"ASR - {modelname}")

    for i, row in merged_asr.iterrows():
        # if row["start"] == "T129":
        #     print(i, row, sep="\n")
        #     pass
        if row.isna().any():
            logging.warning(f"Row with NAs found: \n{row}\n\n")
            continue
        event = ET.Element("event")
        event.set("start", row["start"])
        event.set("end", row["end"])
        event.text = row["transcript"]
        new_tier.append(event)

    new_tier[:] = sorted(
        new_tier, key=lambda child: float(timeline_dict.get(child.get("start")))
    )
    i = last_tier.getparent().index(last_tier)
    last_tier.getparent().insert(i + 1, new_tier)
    ET.indent(exb, space="\t")
    exb.getroottree().write(
        outpath,
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
    
    
    
def fix_220404():
    raise NotImplementedError("This is currently not implemented, Mira provided updated input files.")
    from subprocess import run
    # run("""
    #     rm data/02_Južne_vesti_asr/220404/segments/*;
    #     rm data/02_Južne_vesti_asr/220404/*out*;
    #     rm data/02_Južne_vesti_asr/220404/*_asr*exb;
    #     rm data/02_Južne_vesti_asr/220404/*_done;
    #     """, shell=True)
    exb = ET.fromstring(Path("data/02_Južne_vesti_asr/220404/220404.exb").read_bytes())
    timeline_element = exb.find(".//common-timeline")
    timeline_dict = {
        tli.get("id"): tli.get("time")
        for tli in timeline_element.findall(".//tli[@time]")
    }
    timeline_df = pd.DataFrame({"tli": [i for i in timeline_dict]})
    timeline_df["time"] = timeline_df.tli.map(timeline_dict)
    timeline_df["time"] = timeline_df.time.astype(float)
    timeline_df = timeline_df.sort_values(by="time", ascending=True)
    timeline_df["start_s"] = timeline_df.time.values
    timeline_df["end_s"]  = timeline_df.time.shift(-1).values
    timeline_df["start"] = timeline_df.tli.values
    timeline_df["end"] = timeline_df.tli.shift(-1).values
    
    pairs = timeline_df[["start", "end"]]
    
    pairs["is_already_in"] = False

    for i, row in pairs.iterrows():
        if row.isna().any():
            continue
        s = row["start"]
        e = row["end"]
        if exb.find(f".//event[@start='{s}'][@end='{e}']"):
            pairs.loc[i, "is_already_in"] = True
    
    pass
    # ET.indent(exb, space="\t")
    # exb.getroottree().write(
    #     "data/02_Južne_vesti_asr/220404/220404.exb",
    #     pretty_print=True,
    #     encoding="utf8",
    #     xml_declaration='<?xml version="1.0" encoding="UTF-8"?>',
    # )


def fix_audio_reference(infile: str) -> None:
    exb = ET.fromstring(Path(infile).read_bytes())
    ref = exb.find(".//referenced-file")
    new_ref_url = Path(ref.get("url")).with_suffix(".wav").name
    ref.set("url", new_ref_url)
    ET.indent(exb, space="\t")
    exb.getroottree().write(
        infile,
        pretty_print=True,
        encoding="utf8",
        xml_declaration='<?xml version="1.0" encoding="UTF-8"?>',
    )
    Path(infile).write_text(
        Path(infile)
        .read_text()
        .replace(
            "<?xml version='1.0' encoding='UTF8'?>",
            '<?xml version="1.0" encoding="UTF-8"?>',
        )
    )


if __name__ == "__main__":
    
    df = parse_excel("data/02_Južne_vesti_asr/230410/230410.xlsx")
    
    df
    # inpath = "data/Pescanik_STT/150918/150918.exb"
    # outpath = "test.exb"
    # history = process_file_join_and_label(inpath, outpath)
    # inpath = Path(inpath).with_suffix(".xlsx")
    # fix_excel(inpath, "test.xlsx", history)
    # add_results_to_exb_filtered(
    #     exb_path="data/02_Južne_vesti_asr/230410/230410.exb",
    #     asr_path="data/02_Južne_vesti_asr/230410/230410.out.whisper",
    #     xlsx_path="data/02_Južne_vesti_asr/230410/230410.xlsx",
    #     modelname="whisper",
    #     outpath="test.ext",
    # )
    # fix_audio_reference("data/02_Južne_vesti_asr/230313/230313.exb")
    
    # fix_220404()