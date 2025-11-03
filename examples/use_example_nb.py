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
    mo.md(
        """
    /// admonition | Doing Analysis.

    After loading data we can start analysis.
    ///
    """
    )
    return


@app.cell
def _(cs, exp1):
    # Add param from cluster_by_proximity to stats_cols be default
    group_schema = {
        "series": {"groups": [], "unite_rest": False},
            }

    exp2 = exp1.aggregate_and_compute(
        params = [("Set Temp [K]", 0.09), ("Peak Field on Sample [mT]", 0.5), ("Set Freq [Hz]", 100000)],
        schema = group_schema,
        extra_stats_cols=["LS336 B [K]", "Set Freq [Hz]"],
        remove_multiple=True,
        # with_aggroups=True
    )
    exp2.data.select(cs.contains(["series"])).unique() #_aggroup
    # exp2.data.columns
    return (exp2,)


@app.cell
def _():
    import numpy as np
    import matplotlib.pyplot as plt


    def _to_np(v):
        # Accepts numpy, pandas, or polars series; falls back to np.array
        if hasattr(v, "to_numpy"):
            return v.to_numpy()
        if hasattr(v, "to_list"):
            # polars
            return np.asarray(v.to_list())
        return np.asarray(v)

    def plot_data2(data, plot_scatter=False, x="", y="", nsig=1.0, ax=None, figsize=(9, 6), LineW=1.5, **kwargs):
        """
        Draw the plot onto `ax` (created if None) and return (fig, ax).
        No plt.show(); no global state.
        """
        # Prepare axes
        created_ax = False
        if ax is None:
            fig, ax = plt.subplots(figsize=figsize)
            created_ax = True
        else:
            fig = ax.figure
            ax.cla()  # clear if reusing

        # Extract columns
        if plot_scatter:
            scatterx = data.explode(f"{x}")
            scattery = data.explode(f"{y}")
            PlXax  = data[f"{x}"]
            PlYax  = data[f"{y}"]
        PlXax2 = data[f"{x}_mean"]
        PlYax2 = data[f"{y}_mean"]
        PlXer2 = data[f"{x}_std"]
        PlYer2 = data[f"{y}_std"]

        # Palettes
        Pal3 = np.array([
            [254, 239, 229],
            [0, 145, 110],
            [0, 115, 189],
            [217, 83, 25],
            [255, 207, 0]
        ]) / 255
        Pal = Pal3

        # Settings
        sc1 = 1

        xlabelN = kwargs.get("xlabel", x)
        ylabelN = kwargs.get("ylabel", y)
        freq = 416.0e6
        plot1name = f"{freq/1e6:.0f} MHz, B test mT Run 0"

        MarkSize = 7 * sc1
        LineW    = LineW * sc1
        FontS    = 18 * sc1
        MarkShape = 'o'
        MarkColor = 'none'
        Lcol = Pal[3]

        # Plot
        ax.errorbar(
            PlXax2, PlYax2, xerr=nsig * PlXer2, yerr=nsig * PlYer2,
            fmt='s', label=plot1name, linewidth=LineW, color=Lcol,
            markeredgecolor='black', markerfacecolor=Pal[2],
            capsize=3, ecolor='black', markersize=6, markeredgewidth=1.0
        )
        if plot_scatter:
            ax.plot(
                PlXax, PlYax, MarkShape, label=plot1name, linewidth=LineW,
                color=Lcol, markeredgecolor=Lcol, markerfacecolor=MarkColor,
                markersize=MarkSize, markeredgewidth=1.0
            )
        font = 'serif'
        # Cosmetics
        ax.set_xlabel(xlabelN, fontsize=FontS, fontname=font)
        ax.set_ylabel(ylabelN, fontsize=FontS, fontname=font)
        ax.tick_params(
            direction="in",      # ticks inward
            top=True, right=True,  # ticks on all sides
            # length=6, width=1.2,   # size and thickness
            which="both",          # apply to both major and minor ticks
        )
        ax.grid(False)
        for spine in ax.spines.values():
            spine.set_linewidth(1)
        ax.tick_params(width=1, labelsize=FontS)
        for label in ax.get_xticklabels() + ax.get_yticklabels():
            label.set_fontsize(FontS)
            label.set_fontname(font)
        # fig.tight_layout()

        return fig, ax
    return (plot_data2,)


@app.cell
def _(exp2, mo):
    # Create individual UI elements for each row
    parameters = mo.ui.array([
        mo.ui.dictionary({
            "parameter": mo.ui.dropdown(options=exp2.data.columns, value=None, label=f"{i}"),
            "value": mo.ui.number(value=0.0, step=0.1, label="Value:"),
            "tolerance": mo.ui.number(value=0.0, step=0.1, label="Tolerance:"),
            "std": mo.ui.slider(0, 5, value=1, label="Std:")
        })
        for i in range(4)
    ])

    # Display the elements in a vertical stack
    ui4 = mo.vstack([
        mo.hstack([elem["parameter"], elem["value"], elem["tolerance"], elem["std"]], gap=0.2)
        for elem in parameters
    ])
    ui4
    return (parameters,)


@app.cell
def _(parameters):
    parameters.value[0]
    return


@app.cell
def _(exp2, parameters, pl):
    sel_param = [
        {
            "parameter": par["parameter"].value,   # widget -> has .value
            "value": par["value"].value,
            "tolerance": par["tolerance"].value,
            "std": par["std"].value,
        }
        for par in parameters
    ]


    T = sel_param[2]["value"]
    Ttol = sel_param[2]["tolerance"]

    to_plot = exp2.data.filter(
        (pl.col("Set Freq [Hz]_mean") < 450e6)
        & (pl.col("LS336 B [K]_mean") < T+Ttol/2)
        & (pl.col("LS336 B [K]_mean") > T-Ttol/2)
        & (pl.col("series").list.contains(1))
    )

    # sel_param
    return (to_plot,)


@app.cell
def _(mo, plot_data2, to_plot):
    ## TODO: What we need?
    # 1. Select if you want to plot scatter
    # 2. Add legend generation
    # 3. Add possibility to name axis
    # 4. 

    # This cell re-runs whenever nsig_slider.value changes
    fig3, ax = plot_data2(
        to_plot,
        x="Peak Field on Sample [mT]",
        y="Surface Resistance [nOhm]",
        xlabel=r"Surface resistance $R_\mathrm{s}$ (n$\Omega$)",
        ylabel=r"Sample temperature $T$ (K)",
        nsig=3,
        LineW=1.0
    )
    # fig3.savefig("plot_output_5.pdf", bbox_inches="tight", pad_inches=1.0)
    mo.mpl.interactive(ax)
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
    return


@app.cell
def _(pl):

    dfd = pl.DataFrame({"a": [[0.9, 1.0, 1.1], [0.5, 0.7], [1.02, 2.0], [1.2, 0.96]]})
    # dfd = pl.DataFrame({"a": [[0.9, 1.0, 1.1], [0.5, 0.7], [1.02, 2.0], [1.2, 0.96]]})



    filtered = dfd.filter(
        pl.col("a")
        .list.filter((pl.element() > 0.95) & (pl.element() < 1.05))
        .list.len() > 0
    )

    filterd2 = dfd.filter(
        pl.col("a")
        .list.contains(1.1)
    )


    # dfd.schema["a"]

    if isinstance(dfd.schema["a"], pl.datatypes.List):
        # Do something if 'a' is a list column
        print("'a' is a list column")
    else:
        # Do something else
        print("'a' is NOT a list column")

    dfd, filtered, filterd2
    return


if __name__ == "__main__":
    app.run()
