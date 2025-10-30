"""
QData objects to load and operate QPR measurements, including methods and tools APIs
"""
from typing import Iterable, List, Any
import polars as pl
import dataclasses
from dataclasses import dataclass, field, replace
from pathlib import Path

from .loader import (
    load_csv_data, load_csv_metadata, dress_csv
    )
from .analysis import (
    cluster_by_proximity, aggregate_and_compute
    )


@dataclass(frozen=True)
class QData:
    """
    QData objects to load and operate QPR measurements,
    including methods and tools APIs.
    """
    metadata: dict[str, str | float | int | list | dict | None] = field(default_factory=dict)
    data: pl.DataFrame = field(default_factory=pl.DataFrame)
    files: list[Path] = field(default_factory=list)

    @classmethod #TODO: CHANGE it to normal method later!
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
            A string key in filename that will be used to derive series ID from a file
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

    def cluster_by_proximity(
        self,
        param: str = "Set Temp [K]",
        param_tol: float = 0.09,
    ) -> "QData":
        """
        A utility method. Usefull for manual analysis. 
        Cluster data points in the given DataFrame based on proximity (param_tol)
        in the specified column (param). Does not take into account any other
        columns such as series or scan_id. The resulting DataFrame 
        will contain [param]_param_groupe column with assigned groupe.

        Parameters
        ----------
        param : str, default="Set Temp [K]"
            Name of the column to group by proximity.
        param_tol : float, default=0.1
            Maximum allowed difference between consecutive values in `param`
            to consider them part of the same group.

        Returns
        -------
        QData
            A copy of QData Instance containing processed data.
        """
        grouped_df = cluster_by_proximity(self.data, param=param, param_tol=param_tol)
        return replace(self, data=grouped_df)
    
    def aggregate_and_compute(
        self,
        param: str = "Set Temp [K]",
        param_tol: float = 0.09,
        x_ax: str = "Peak Field on Sample [mT]",
        x_ax_tol: float = 1.0,
        y_ax: str = "Surface Resistance [nOhm]",
        schema: dict[str, dict[str, list[Any] | list[list[Any]] | bool]] | None = None,
        **kwargs
    ) -> "QData":
        """
        [Warning!] Chnges DataFrame Structure.
        The main stanalone analysis method

        Parameters
        ----------
        **
        kwargs
        with_aggroups: bool
        remove_multiple: bool

        Returns
        -------
        QData
            A copy of QData Instance containing processed data.
        """
        extra_stats_cols = kwargs.get("extra_stats_cols", [])
        if extra_stats_cols:
            extra_stats_cols = [extra_stats_cols] if not isinstance(extra_stats_cols, list) else extra_stats_cols

        stat_list = [x_ax, y_ax, param] + extra_stats_cols
        stat_list = list(set(stat_list))

        grouped_df = cluster_by_proximity(self.data, param=param, param_tol=param_tol)
        grouped_df = cluster_by_proximity(grouped_df, param=x_ax, param_tol=x_ax_tol)
        grouped_df = cluster_by_proximity(grouped_df, param="Set Freq [Hz]", param_tol=100000)
        aggregate_df = aggregate_and_compute(grouped_df, schema=schema, stat_list=stat_list, **kwargs)
        return replace(self, data=aggregate_df)