from pathlib import Path
import polars as pl

p = Path(r"G:\PhD_archive\QPR Data\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")

extensions = [".txt", ".csv"]

if p.is_file():
    files = [p]
elif p.is_dir():
    files = [f for f in p.iterdir() if f.is_file() and f.suffix.lower() in extensions]
else:
    raise FileNotFoundError(f"Path not found: {p}")

lfs = []
for f in files:
    sep = "\t" if f.suffix.lower() == ".txt" else ","
    lfs.append(pl.scan_csv(f, separator=sep))
all_data = pl.concat(lfs)
print(all_data)