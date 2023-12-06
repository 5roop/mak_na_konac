import torch
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline
from pathlib import Path
import torch

device = torch.device(snakemake.params.cuda if torch.cuda.is_available() else "cpu")
torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
model_id = snakemake.params.model
try:
    model = AutoModelForSpeechSeq2Seq.from_pretrained(
        model_id, torch_dtype=torch_dtype, low_cpu_mem_usage=True, use_safetensors=True
    )
except:
    model = AutoModelForSpeechSeq2Seq.from_pretrained(
        model_id, torch_dtype=torch_dtype, low_cpu_mem_usage=True, use_safetensors=False
    )
model.to(device)
processor = AutoProcessor.from_pretrained(model_id)
pipe = pipeline(
    "automatic-speech-recognition",
    model=model,
    tokenizer=processor.tokenizer,
    feature_extractor=processor.feature_extractor,
    max_new_tokens=128,
    chunk_length_s=30,
    batch_size=16,
    return_timestamps=True,
    torch_dtype=torch_dtype,
    device=device,
)


files_to_process = [
    str(i) for i in Path(snakemake.input[0]).parent.glob("segments/*.wav")
]

from datasets import Dataset, Audio
from transformers.pipelines.pt_utils import KeyDataset

ds = Dataset.from_dict({"audio": files_to_process}).cast_column(
    "audio", Audio(sampling_rate=16000)
)


result = pipe(
    KeyDataset(ds, "audio"),
    generate_kwargs={"language": "serbian"},
)
transcripts = [i.get("text") for i in result]
import pandas as pd

pd.DataFrame({"file": files_to_process, "transcript": transcripts}).to_csv(
    f"{snakemake.output[0]}", index=False
)
