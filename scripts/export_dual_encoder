#!.venv/bin/python3
import os
import sys
import transformers

MODEL = "sentence-transformers/all-MiniLM-L6-v2"

if os.path.exists("data/dual_encoder/"):
    print("data/dual_encoder/ already exists. Exiting...")
    sys.exit()

os.system("mkdir -p data/dual_encoder")

model = transformers.AutoModel.from_pretrained(MODEL)
tokenizer = transformers.AutoTokenizer.from_pretrained(MODEL)

model.save_pretrained("data/dual_encoder")
tokenizer.save_pretrained("data/dual_encoder")
