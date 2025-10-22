"""
QData objects to load and operate QPR measurements, including methods and tools APIs
"""
import polars as pl
import dataclasses
from dataclasses import dataclass, field
from pathlib import Path

from .loader import load_csv_data, load_csv_metadata, dress_csv

@dataclass(frozen=True)
class QData:
    """
    QData objects to load and operate QPR measurements,
    including methods and tools APIs.
    """
    metadata: dict[str, str | float | int | list | dict | None] = field(default_factory=dict)
    data: pl.DataFrame = field(default_factory=pl.DataFrame)
    files: list[Path] = field(default_factory=list)

    @classmethod
    def load_data(cls, data_link: str) -> "QData":
        p = Path(data_link)
        if p.is_dir():
            df, files = load_csv_data(data_link)
            return cls(data=df)
        elif p.suffix.lower() in [".hdf5", ".h5"]:
            """
            A placeholder for hdf5import
            """
            raise NotImplementedError("This is not implem,ented yet!")
        else:
            raise FileNotFoundError("Wrong File!")
        
    @classmethod
    def load_and_dress(
        cls,
        data_link: str | Path,
        pattern: str = "*.txt",
        to_drop: list[str] | None = None,
        separator: str = "\t",
        series_key: str = "run",
        drop_empty: bool = True,
    ) -> "QData":
        """
        Load and process measurement data from a directory or file.

        Parameters
        ----------
        - data_link: str | Path
            Path to a directory or data file (.txt, .csv, .h5).
        - pattern: str, optional
            Glob pattern for input files (passed to load_csv_data).
        - separator: str, default="\\t"
            Field separator for CSV files.
        - read_schema: dict[str, pl.DataType], optional
            Column name -> data type mapping for Polars parsing.
        - to_drop: list[str], optional
            Columns to remove from the DataFrame (passed to dress_csv).
        - seires_key: str, optional
            Column name used as a logical grouping key in analysis.
        - drop_empty: bool, default=True
            determains if all empty columns with null values will be dropped

        Returns
        -------
        QData
            Instance containing processed data, file list, and metadata.
        """
        p = Path(data_link)
        if p.is_dir():
            df, files = load_csv_data(p, pattern=pattern, separator=separator)
            metadata = load_csv_metadata(p)
            df = dress_csv(df, to_drop=to_drop, series_key=series_key, drop_empty=drop_empty)
            return cls(data=df, files=files, metadata=metadata)
        elif p.suffix.lower() in [".hdf5", ".h5"]:
            """
            A placeholder for hdf5import
            """
            raise NotImplementedError("This is not implem,ented yet!")
        else:
            raise FileNotFoundError("Wrong File!")

