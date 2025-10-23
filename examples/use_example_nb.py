import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path
    import polars as pl
    from typing import Iterable, List
    # import marimo as mo

    p = Path(r"G:\PhD_archive\QPR Data\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    # p = Path(r"D:\nextcloud\QPR tests & Operation\2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")
    p
    return Iterable, p, pl


@app.cell
def _(p):
    from qdive import QData as qd

    exp1 = qd.load_and_dress(p, pattern="*Measurements*.txt")


    exp1.data, exp1.metadata
    return (exp1,)


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
    return (result,)


@app.cell
def _(pl, result):
    import matplotlib.pyplot as plt

    def plot_with_errorbars(
        df: pl.DataFrame,
        column_nameX: str,
        column_nameY: str,
        *,
        xlabel: str | None = None,
        ylabel: str | None = None,
        title: str | None = None,
        color: str = "tab:blue",
        marker: str = "o",
        capsize: float = 3.0,
    ):
        """
        Plot a Matplotlib error bar plot using columns with '_avg' and '_std' suffixes.

        Parameters
        ----------
        df : pl.DataFrame
            Aggregated Polars DataFrame containing *_avg and *_std columns.
        column_nameX : str
            Base column name for X (e.g., "Set Temp [K]").
        column_nameY : str
            Base column name for Y (e.g., "Power [W]" or "Q-factor").
        xlabel, ylabel, title : str | None
            Optional axis labels / title overrides.
        color : str
            Matplotlib color name.
        marker : str
            Marker style (default "o").
        capsize : float
            Length of error bar caps.
        """
        # Convert to pandas for Matplotlib
        pdf = df.to_pandas()

        x = pdf[f"{column_nameX}_avg"]
        y = pdf[f"{column_nameY}_avg"]

        # Optional error columns
        xerr = pdf.get(f"{column_nameX}_std", None)
        yerr = pdf.get(f"{column_nameY}_std", None)

        fig, ax = plt.subplots(figsize=(6, 4))
        ax.errorbar(
            x, y,
            xerr=xerr,
            yerr=yerr,
            fmt=marker,
            color=color,
            ecolor="gray",
            elinewidth=1,
            capsize=capsize,
            label=column_nameY
        )

        ax.set_xlabel(xlabel or column_nameX)
        ax.set_ylabel(ylabel or column_nameY)
        if title:
            ax.set_title(title)
        ax.grid(True, ls="--", alpha=0.4)
        ax.legend()
        plt.tight_layout()
        plt.show()


    plot_with_errorbars(
        result,
        column_nameX="Set Temp [K]",
        column_nameY="Surface Resistance [nOhm]",
        xlabel="Set Temperature (K)",
        ylabel="Amplitude (dB)",
        title="Temperature Dependence of Amplitude",
    )
    return


if __name__ == "__main__":
    app.run()
