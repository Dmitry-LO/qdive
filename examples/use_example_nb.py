import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    # TODO: 1. 
    return


@app.cell
def _():
    from pathlib import Path
    import polars as pl
    from typing import Iterable, List
    import os
    cwd = os.getcwd()
    cwd

    # p = Path(r"G:\PhD_archive\QPR Data\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    # p = Path(r"D:\nextcloud\QPR tests & Operation\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    p = Path("examples/2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    {"Workdir":cwd, "Path":p}
    return Iterable, p, pl


@app.cell
def _(p):
    from qdive import QData as qd

    exp1 = qd.load_and_dress(p, pattern="*Measurements*.txt")


    exp1.data, exp1.metadata
    return (exp1,)


app._unparsable_cell(
    r"""
    def group_and_combine(
        df: pl.DataFrame,
        param: str = \"Set Temp [K]\",
        param_tol: float = 0.1,
        grope_by = \"\"
        divide_by: dict | None = None,
    ) -> pl.DataFrame:
        \"\"\"
        Return a new DataFrame aggregated by proximity based on the selected column. Computes statistics.
        \"\"\"
        if devide_by is None:
            devide_by = [\"series\"]
        print(devide_by)
        pass

    schema = {
        \"split\": [\"series\",\"scan_id\"]
        \"merge\": {\"scan_id\":[1,2,3]}
    }
    new_df = grupe_and_compute(exp1.data)

    """,
    name="_"
)


@app.cell
def _(Iterable, exp1, pl):
    ## example
    def collapse_by_distance(
        df: pl.DataFrame,
        column_name: str,
        max_distance: float,
        *,
        scan_id: bool = True,
        Series: bool = True,
        extra_keys: Iterable[str] | None = None,
        group_col: str = "group",
    ) -> pl.DataFrame:
        """
        Cluster rows by proximity in `column_name` with tolerance `max_distance`,
        optionally separated by key columns, then reduce each cluster to one row:
          - float columns -> `<name>_avg` and `<name>_std`
          - all other columns -> first() value in the cluster
        """
        if column_name not in df.columns:
            raise ValueError(f"'{column_name}' not in DataFrame.")

        # Build key columns list
        keys: list[str] = []
        if scan_id and "scan_id" in df.columns:
            keys.append("scan_id")
        if Series and "Series" in df.columns:
            keys.append("Series")
        if extra_keys:
            for k in extra_keys:
                if k not in df.columns:
                    raise ValueError(f"Key column '{k}' not in DataFrame.")
            keys.extend(list(extra_keys))

        # Sort so proximity is evaluated within each key block
        sort_cols = keys + [column_name]
        df_sorted = df.sort(sort_cols)

        # Proximity cluster id within each (keys) block
        gap = (pl.col(column_name) - pl.col(column_name).shift(1))
        is_break = gap.fill_null(max_distance + 1.0) > max_distance
        if keys:
            df_tagged = df_sorted.with_columns(is_break.cum_sum().over(keys).alias(group_col))
        else:
            df_tagged = df_sorted.with_columns(is_break.cum_sum().alias(group_col))

        # Make it 1-based for readability
        df_tagged = df_tagged.with_columns(pl.col(group_col) + 1)

        # Aggregations: do NOT aggregate key columns (including group_col) to avoid duplicates
        groupby_cols = keys + [group_col]
        float_types = {pl.Float32, pl.Float64}

        agg_exprs: list[pl.Expr] = []
        for col, dtype in df.schema.items():
            if col in groupby_cols:
                continue  # keys will be included automatically by groupby
            if dtype in float_types:
                agg_exprs.append(pl.col(col).mean().alias(f"{col}_avg"))
                agg_exprs.append(pl.col(col).std(ddof=1).alias(f"{col}_std"))
            else:
                agg_exprs.append(pl.col(col).first().alias(col))

        out = (
            df_tagged
            .group_by(groupby_cols, maintain_order=True)
            .agg(agg_exprs)
            .sort(groupby_cols)
        )
        return out



    result = collapse_by_distance(exp1.data, column_name="Set Temp [K]", max_distance=0.2)
    return


if __name__ == "__main__":
    app.run()
