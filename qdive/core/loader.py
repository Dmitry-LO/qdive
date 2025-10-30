import polars as pl
from pathlib import Path
from typing import Iterable, List
import json

def find_files(
    path: Path,
    glob_pattern: str = "*.txt",
    recursive: bool = False
) -> List[Path]:
    if path.is_file():
        return [path] if path.match(glob_pattern) else []
    if path.is_dir():
        files = path.glob(glob_pattern)
        return [p for p in files if p.is_file()]
    raise FileNotFoundError(f"Path not found: {path}")

def load_csv_data(path: str | Path,
                  pattern: str = "*.txt",
                  separator: str = "\t",
                  read_schema: dict[str, pl.DataType] | None = None
                  ):
    """
    Load measurement text files into a Polars DataFrame.

    Parameters
    ----------
    - path: str | Path
        Path to a file or directory containing data files.
    - pattern: str, default="*.txt"
        Glob pattern for input files.
    - separator: str, default="\\t"
        Field separator used in files.
    - read_schema: dict[str, pl.DataType], optional
        Column name → data type mapping for Polars.

    Returns
    -------
    (pl.DataFrame, list[Path])
        Combined DataFrame and list of loaded files.
    """
    if read_schema is None:
            read_schema = {
                "Date": pl.Utf8,
                "Time": pl.Utf8,
                "Set Temp [K]": pl.Float64,
                "Set Freq [Hz]": pl.Float64,
                "Duty Cycle [%]": pl.Float64,
                "Pulse Period [ms]": pl.Float64,
                "P_forw (giga)": pl.Float64,
                "P_refl (giga)": pl.Float64,
                "P_trans (giga)": pl.Float64,
                "CW Power (Tek)": pl.Float64,
                "Pulse Power (Tek)": pl.Float64,
                "Peak Power (Tek)": pl.Float64,
                "DC meas [%] (Tek)": pl.Float64,
                "P_trans for calc": pl.Float64,
                "Freq. (meas.) [Hz]": pl.Float64,
                "Q_FPC": pl.Float64,
                "Q_Probe": pl.Float64,
                "c1": pl.Float64,
                "c2": pl.Float64,
                "Heater Resistance [Ohm]": pl.Float64,
                "Ref. Voltage": pl.Float64,
                "Heater Voltage": pl.Float64,
                "Heater Power [mW]": pl.Float64,
                "P_diss [mW]": pl.Float64,
                "Peak Field on Sample [mT]": pl.Float64,
                "Surface Resistance [nOhm]": pl.Float64,
                "LS336 A [K]": pl.Float64,
                "LS336 B [K]": pl.Float64,
                "LS336 C [K]": pl.Float64,
                "LS336 D [K]": pl.Float64,
                "Magnetic Field [uT]": pl.Float64,
                "PLL Attenuator [dB]": pl.Float64,
                "PLL Phase [deg]": pl.Float64,
                "Keysight forw [dBm]": pl.Float64,
                "Keysight refl [dBm]": pl.Float64,
                "Keysight trans [dBm]": pl.Float64,
                "DC current [mA]": pl.Float64,
                "DC Ref current [mA]": pl.Float64,
                "Freq Hameg [Hz]": pl.Float64,
            } # type: ignore
        
    
    p = Path(path)
    files = find_files(p, glob_pattern=pattern)
    if not files:
        raise FileNotFoundError(f"No file found with selected pattern: {pattern}")

    # loading data from text files and phrasing values and dates
    all_data = pl.scan_csv(
        files, 
        separator=separator,
        null_values=["NaN"],
        glob=True,
        include_file_paths="scan_desc",
        try_parse_dates=False,
        schema_overrides=read_schema,
    ).with_columns([
        pl.col("Date").str.to_date("%Y/%m/%d", strict=False), 
        pl.col("Time").str.to_time("%H:%M:%S", strict=False)
        ]
    ).collect()

    return all_data, files

def dress_csv(df: pl.DataFrame, to_drop: list[str] | None = None, series_key: str ="run", drop_empty: bool = True):
    """
    Wrapps DataFrame loaded from list of csv files with additional fields used for analysis

    Parameters
    ----------
    - df: pl.DataFrame 
        Input DataFrame loaded from CSV files.
    - to_drop: list[str], optional
        Column names to remove from the DataFrame.
    - series_key: str, default="run"
        Column name or key used to identify individual series.
    - drop_empty: bool, default=True
        determains if all empty columns with null values will be dropped

    Returns
    -------
    pl.DataFrame
        Processed DataFrame ready for analysis.
    """
    if not to_drop:
        to_drop = []

    if drop_empty:
        to_drop += [s.name for s in df if s.null_count() == df.height]

    # throwing empty columns away
    df = df.drop(to_drop)

    # creating a datetime column
    dressed_df = df.with_columns(
        pl.datetime(
            year=pl.col("Date").dt.year(),
            month=pl.col("Date").dt.month(),
            day=pl.col("Date").dt.day(),
            hour=pl.col("Time").dt.hour(),
            minute=pl.col("Time").dt.minute(),
            second=pl.col("Time").dt.second(),
        ).alias("DateTime")
    )

    # mapping unique filenames from scan_desc to a map dataframe with indecses = scan_id
    scan_map = df.select(
        pl.col("scan_desc").unique(maintain_order=True)
    ).with_row_index("scan_id").with_columns(pl.col("scan_id")+1)

    # appling indexes to all filenames from mapped dataframe and adding series field
    dressed_df = dressed_df.join(scan_map, on="scan_desc").with_columns(pl.lit(0).alias("series"))

    dressed_df = dressed_df.with_columns(
    pl.col("scan_desc")
      .str.extract(fr"(?i){series_key}[_.-]*([0-9]{{1,2}})")
      .cast(pl.Int16)
      .alias("series")
    )
    return dressed_df

def load_csv_metadata(p: Path):
    metadata_path = p / "metadata.json"
    if metadata_path.exists():
        with metadata_path.open("r", encoding="utf-8") as f:
            metadata = json.load(f)
    else:
        metadata = {}
    return metadata