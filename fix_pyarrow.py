import os
import re
from datasets import Dataset
from datasets.arrow_writer import ArrowWriter
from tqdm import tqdm


old_base_path = r"D:\FullTemp\F5-TTS\src\f5_tts\..\..\data\multispeaker_pinyin"
new_base_path = r"../../input/f5tts-multispeaker-dataset/multispeaker_pinyin"

input_file = r"D:\FullTemp\F5-TTS\data\multispeaker_pinyin\raw.arrow"
output_file = r"D:\FullTemp\F5-TTS\data\multispeaker_pinyin\raw_backup.arrow"

def modify_path(path):
	return re.sub(f'^{re.escape(old_base_path)}', new_base_path, path).replace('\\', '/')
	
dataset = Dataset.from_file(input_file)

os.rename(input_file, output_file)

with ArrowWriter(path=input_file, writer_batch_size=1) as writer:
    # Пройтись по датасету построчно, модифицировать и записать
    for row in tqdm(dataset, desc="Processing and writing"):
        row["audio_path"] = modify_path(row["audio_path"])  # Изменяем путь
        writer.write(row)

print(f"Файл сохранён в: {output_file}")
