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
    import polars.selectors as cs
    from typing import Iterable, List
    import os
    import marimo as mo
    cwd = os.getcwd()
    cwd

    # p = Path(r"G:\PhD_archive\QPR Data\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    # p = Path(r"D:\nextcloud\QPR tests & Operation\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    p = Path("examples/2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    {"Workdir":cwd, "Path":p}
    return cs, mo, p, pl


@app.cell
def _(p):
    from qdive import QData as qd # will need to rename it to something else... such as QPRScan

    exp1 = qd.load_and_dress(p, pattern="*Measurements*.txt")


    exp1.data, exp1.metadata
    return (exp1,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    /// admonition | Doing Analysis.

    After loading data we can start analysis.
    ///
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    schema2 = {
        "col_name_1": {"groups": [1, 2, [3, 4]]},
        "col_name_1": {"groups": [[1, 2, 3], [7, 8]], "unite_rest": True},
    }

    mo.output.append(mo.md("Information about parameters"))
    mo.output.append(mo.tree(schema2))
    return


@app.cell
def _(mo):
    mo.md(
        """
    Example of schema definition:

    ```python
    schema = {
        "col_name_1": {"groups": [1, 2, [3, 4]]},
        "series": {"groups": [[1, 2, 3], [7, 8]], "unite_rest": True},
    }```
    """
    )
    return


@app.cell
def _(cs, exp1):
    # Add param from cluster_by_proximity to stats_cols be default
    group_schema = {
        "series": {"groups": [[1,2,3], [7,8]], "unite_rest": True},
            }

    exp2 = exp1.aggregate_and_compute(
        params = [("Set Temp [K]", 0.09), ("Peak Field on Sample [mT]", 0.5), ("Set Freq [Hz]", 100000)],
        schema = group_schema,
        extra_stats_cols=["LS336 B [K]", "Set Freq [Hz]"],
        remove_multiple=True,
        with_aggroups=True
    )
    exp2.data.select(cs.contains(["series"])) #_aggroup
    # exp2.data.columns
    return (exp2,)


@app.cell
def _(exp2, mo, pl):
    # Use a unique name (not "df")
    agg_df: pl.DataFrame = exp2.data

    # Available temperatures from the aggregated table
    temp_values = (
        agg_df
        .select(pl.col("Set Temp [K]_mean").unique().drop_nulls())
        .to_series()
        .to_list()
    )
    temp_values = sorted(temp_values)

    # Widgets
    temp_select = mo.ui.multiselect(
        options=[str(t) for t in temp_values],
        value=[str(2.5)] if 2.5 in temp_values else ([str(temp_values[0])] if temp_values else []),
        label="Temperatures (K)",
    )
    std_k = mo.ui.number(1.0,10.0, step=0.5, label="Std multiplier (k)")
    connect_lines = mo.ui.checkbox(value=True, label="Connect points per T")

    # Show controls
    mo.vstack([temp_select, std_k, connect_lines])
    return agg_df, connect_lines, std_k, temp_select


@app.cell
def _(agg_df: "pl.DataFrame", connect_lines, std_k, temp_select):
    def _():
        # marimo
        import numpy as np
        import matplotlib.pyplot as plt
        import polars as pl

        # Read widget values created in the previous cell
        selected_ts = [float(t) for t in temp_select.value]
        k = float(std_k.value)
        connect = bool(connect_lines.value)

        fig, ax = plt.subplots()

        for t in selected_ts:
            sub = agg_df.filter(pl.col("Set Temp [K]_mean") == t)
            if sub.height == 0:
                continue

            x = sub["Peak Field on Sample [mT]_mean"].to_numpy()
            xerr = k * sub["Peak Field on Sample [mT]_std"].to_numpy()

            y = sub["Surface Resistance [nOhm]_mean"].to_numpy()
            yerr = k * sub["Surface Resistance [nOhm]_std"].to_numpy()

            ax.errorbar(x, y, xerr=xerr, yerr=yerr, fmt="o", capsize=3, label=f"T = {t} K")

            if connect and x.size > 1:
                order = np.argsort(x)
                ax.plot(x[order], y[order])

        ax.set_xlabel("Peak Field on Sample [mT] (mean)")
        ax.set_ylabel("Surface Resistance [nOhm] (mean)")
        ax.grid(True)
        if selected_ts:
            ax.legend()
        fig.tight_layout()
        return fig  # returning the figure is enough for Marimo


    _()
    return


@app.cell
def _(cs, new_df, pl):
    # df = pl.DataFrame(
    #     {
    #         "a": ["a1","a1","a1", "b1", "b1", "c1", "d1"],
    #         "b": [2, 2, 3, 4, 5, 6, 7],
    #         "c": [5.1, 4.2, 3.3, 2.7, 1.7, 18.0, 21.9],
    #         "n": ["Noah", "Jacob", "Elijah", "Adam", "Samuel", "David", "Isaac"],
    #     }
    # )

    # df = pl.DataFrame(
    #     {
    #         "a": [["a1", "b1"], ["a1", "b1"], ["a1", "b1"], ["a1", "b1"], ["c1"]],
    #         "b": [1, 2, 1, 3, 3],
    #         "c": [5, 4, 3, 2, 1],
    #     }
    # )

    # schema = {
    #     "a": ["a1", "c1", ["d1", "b1"]], # Add requirement that should allways be list or empty list or list of lists.
    #     "b": [1,[2,3]],
    # }

    df = new_df
    schema = {
        "series": [],
        # "scan_id": [],
    }


    schema.update({name: [] for name in df.select(cs.contains("_param_groupe")).columns})

    stat_list = [
        "Set Temp [K]",
        "Peak Field on Sample [mT]",
        "Surface Resistance [nOhm]",
    ]

    remove_nultiple = False

    def build_mapping(schema: dict) -> dict:
        mapping = {}
        for col, items in schema.items():
            inner = {}
            for i, element in enumerate(items, start=1):
                group = f"group_{i}"
                if isinstance(element, list):
                    for sub in element:
                        inner[str(sub)] = group
                else:
                    inner[str(element)] = group
            mapping[col] = inner
        return mapping

    mapping = build_mapping(schema)
    print(mapping.items())

    # Also need to do a version if it is to combine column<100
    df2=df.clone()

    # for col, values in list(mapping.items()):  # use list() to allow modification
    #     print(col, values)
    #     valid = []
    #     if values is not None:
    #         for v in values:
    #             print(v)
    #             if v in df2[col].to_list():
    #                 valid.append(v)
    #             else:
    #                 print(f"⚠️ WARNING: value '{v}' not found in column '{col}' - dropping.")
    #             mapping[col] = valid
    #     else:
    #         # valid = df2[col].to_list()
    #         mapping[col] = []

    print(mapping.items())
    for column_key, mp in mapping.items():
        base = pl.col(column_key).cast(pl.String)
        df2 = df2.with_columns(
            base.replace_strict(mp, default=base).alias(f"{column_key}_aggroup")
        )

    ## Now aggrigation
    # first create useful liset for working with data
    float_cols = []
    non_float_cols = []
    for colname in df.columns:
        if colname not in schema.keys():
            if df[colname].dtype in (pl.Float32, pl.Float64): #pl.Int64, pl.Int32
                float_cols.append(colname)
            else:
                non_float_cols.append(colname)

    # Aggregation instruction list
    aggregation_list = [pl.col(colname).mean().alias(colname+"_mean") for colname in stat_list]
    aggregation_list += [pl.col(colname).std().alias(colname+"_std").fill_null(0) for colname in stat_list]
    aggregation_list += [pl.col(colname) for colname in df.columns]

    # Columns list whose list values will have duplicates removed after group_by.
    # Usually not needed, default is False, since you can always clean duplicates later with `.list.unique()`.
    # Keeping the non-cleaned lists can be useful to inspect the full dataset
    if remove_nultiple: 
        unique_list = [pl.col(colname).list.unique().alias(colname) for colname in df.columns if df[colname].dtype not in (pl.Float32, pl.Float64)]
    else:
        unique_list = []

    # we will add sortkey here
    computed_df = (df2
        .group_by([key+"_aggroup" for key in schema.keys()]) 
        .agg(aggregation_list).with_columns(unique_list)
        .drop(cs.contains("_aggroup"))
        )

    mapping, df, df2, computed_df
    return (computed_df,)


@app.cell
def _():
    ### BKP

    ## Method 1 sorting preparatriob
    # for column_key in to_combine.keys():    
    #     df2 = (df2
    #         # Creating additinall column of list that will serve as single sist element source in otherwise
    #         .with_columns(
    #             pl.Series(column_key+"_list", [[x] for x in df2[column_key]])
    #             )
    #         .with_columns(
    #             pl.when(pl.col(column_key).is_in(to_combine[column_key]))
    #             .then(pl.lit(to_combine[column_key]))
    #             .otherwise(pl.col(column_key+"_list"))
    #             .alias(column_key)
    #             )
    #         .drop(column_key+"_list")
    #         )
    return


app._unparsable_cell(
    r"""
     groups = mo.ui.multiselect(
        options=[\"a1\", \"b1\", \"c1\"],   # adjust to your data
        value=[\"a1\",\"b1\"]
    )

    groups
    """,
    name="_"
)


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
