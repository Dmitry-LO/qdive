import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path
    import polars as pl
    from typing import Iterable, List
    # import marimo as mo

    # p = Path(r"G:\PhD_archive\QPR Data\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    p = Path(r"D:\nextcloud\QPR tests & Operation\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    p

    return List, Path, p, pl


@app.cell
def _(List, Path, p, pl):
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

    def load_csv_data(path: str | Path, pattern="*.txt", separator: str = "\t", read_schema = None):
        p = Path(path)
        files = find_files(p, glob_pattern=pattern)
        if not files:
            raise FileNotFoundError(f"No file found with selected pattern: {pattern}")

        if not read_schema:
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
            }

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
            pl.col("Time").str.to_time("%H:%M:%s", strict=False)
            ]
        ).collect()

        return all_data, files

    def dress_csv(df: pl.DataFrame, to_drop: list[str] | None = None, seires_key: str = "run"):
        """
        Wrapps DataFrame loaded from list of csv files with additional fields used for analysis
        """
        # throwing empty columns away
        if to_drop:
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
          .str.extract(fr"(?i){seires_key}[_.-]*([0-9]{{1,2}})")
          .cast(pl.Int16)
          .alias("series")
        )


        return dressed_df


    all_data, files=load_csv_data(p, '*Measurements*.txt')
    all_data2 = dress_csv(all_data, to_drop=["P_forw (giga)", "P_refl (giga)", "P_trans (giga)"])

    all_data, all_data2
    # mo.ui.table(all_data, show_column_summaries=True)
    return


if __name__ == "__main__":
    app.run()
