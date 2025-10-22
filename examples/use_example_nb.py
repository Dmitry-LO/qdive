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
    return (p,)


@app.cell
def _(p):
    from qdive import QData as qd

    exp1 = qd.load_and_dress(p, pattern="*Measurements*.txt")


    exp1.data, exp1.metadata["period"]
    return


if __name__ == "__main__":
    app.run()
