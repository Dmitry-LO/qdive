import marimo

__generated_with = "0.17.0"
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
    import marimo as mo
    cwd = os.getcwd()
    cwd

    # p = Path(r"G:\PhD_archive\QPR Data\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    # p = Path(r"D:\nextcloud\QPR tests & Operation\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    p = Path("examples/2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    {"Workdir":cwd, "Path":p}
    return Iterable, mo, p, pl


@app.cell
def _(p):
    from qdive import QData as qd

    exp1 = qd.load_and_dress(p, pattern="*Measurements*.txt")


    exp1.data, exp1.metadata
    return (exp1,)


@app.cell
def _(exp1, pl):
    def grupe_and_compute(
        df: pl.DataFrame,
        param: str = "Set Temp [K]",
        param_tol: float = 0.1,
        grope_by = "",
        split_by: dict | None = None,
    ) -> pl.DataFrame:
        """
        Return a new DataFrame aggregated by proximity based on the selected column. Computes statistics.
        """
        if split_by is None:
            pass
        print(split_by)
        grupped_df = (df
            .sort(by=pl.col(param))
            .with_columns(pl.col(param)
                .diff().alias("diff").fill_null(0))
            .with_columns(jump=(pl.col("diff")>param_tol))
            .cast({"jump":pl.UInt8})
            .with_columns((pl.col("jump").cum_sum()+1).alias("grupe"))
                     )

        return grupped_df

    schema = {
        "split": ["series","scan_id"],
        "merge": {"scan_id":[1,2,3]}
    }
    new_df = grupe_and_compute(exp1.data, param="Heater Power [mW]")
    new_df

    return


@app.cell(hide_code=True)
def _(Iterable, exp1, pl):
    ## TEST

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


@app.cell
def _(pl):
    df = pl.DataFrame(
        {
            "a": ["a1","a1","a1", "b1", "b1", "b1", "c1"],
            "b": [1, 2, 3, 4, 5, 6, 7],
            "c": [5, 4, 3, 2, 1, 18, 21],
        }
    )

    # df = pl.DataFrame(
    #     {
    #         "a": [["a1", "b1"], ["a1", "b1"], ["a1", "b1"], ["a1", "b1"], ["c1"]],
    #         "b": [1, 2, 1, 3, 3],
    #         "c": [5, 4, 3, 2, 1],
    #     }
    # )

    to_combine={
        "a": None, #["b1", "c1"],
        "b": None, #[1,3,2]
    }

    # Also need to do a version if it is to combine column<100
    df2=df.clone()
    for col, values in list(to_combine.items()):  # use list() to allow modification
        valid = []
        if values is not None:
            for v in values:
                if v in df2[col].to_list():
                    valid.append(v)
                else:
                    print(f"⚠️ WARNING: value '{v}' not found in column '{col}' - dropping.")
                to_combine[col] = valid
        else:
            # valid = df2[col].to_list()
            to_combine[col] = []
    
    for column_key in to_combine.keys():    
        df2 = (df2
            .with_columns(
                pl.Series(column_key+"_list", [[x] for x in df2[column_key]])
                )
            .with_columns(
                pl.when(pl.col(column_key).is_in(to_combine[column_key]))
                .then(pl.lit(to_combine[column_key]))
                .otherwise(pl.col(column_key+"_list"))
                .alias(column_key)
                )
            .drop(column_key+"_list")
            )


    computed_df = (df2
        .group_by(to_combine.keys())
        .agg([pl.col("c").mean().alias("c_mean"), 
              pl.col("c").std().alias("c_std").fill_null(0)]
            )
        )

    df, df2, computed_df
    return (computed_df,)


@app.cell
def _(mo):
    groups = mo.ui.multiselect(
        options=["a1", "b1", "c1"],   # adjust to your data
        value=["a1","b1"]
    )

    groups
    return (groups,)


@app.cell
def _(computed_df, groups, pl):
    selection = groups.value

    sel = computed_df.filter(
        pl.col("a").list.eval(pl.element().is_in(selection)).list.any()
    )

    sel
    return (sel,)


@app.cell
def _(sel):
    import plotly.express as px

    # plot c_mean over a linear x-axis (index)
    fig = px.line(
        sel.to_pandas(),
        y="c_mean",
        markers=True,
        hover_data=["a"],  # show the group list on hover
        title="c_mean for selected groups"
    )

    # (optional) label x-ticks with the group list instead of index
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(range(len(sel))),
        ticktext=[str(v) for v in sel["a"].to_list()],
    )

    fig  # just return the figure; marimo renders it interactively

    return


if __name__ == "__main__":
    app.run()
