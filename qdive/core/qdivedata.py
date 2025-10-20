"""
QDiveData objects to load and operate QPR measurements, including methods and tools APIs
"""
import polars as pl
import dataclasses
from dataclasses import dataclass, field
from pathlib import Path

from loader import load_csv, load_hdf5

@dataclass(frozen=True)
class QDiveData:
    """QDiveData objects to load and operate QPR measurements,
    including methods and tools APIs.
    """
    experiment: str | None = None
    data: pl.DataFrame = field(default_factory=pl.DataFrame)

    @classmethod
    def load_data(cls, data_link: str) -> "QDiveData":
        p = Path(data_link)
        if p.is_dir():
            df = load_csv(data_link)
            return cls(data=df, experiment="Experiment")
        elif p.suffix.lower() in [".hdf5", ".h5"]:
            return cls(data=df, experiment="Experiment")

