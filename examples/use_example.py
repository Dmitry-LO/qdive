from pathlib import Path
import polars as pl
import polars.selectors as cs
from typing import Iterable, List
import os
import marimo as mo


p = Path("examples/2022-04-04 - test #36 - ARIES B-3.19 Siegen SIS")

from qdive import QData as qd

exp1 = qd.load_and_dress(p, pattern="*Measurements*.txt")


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

from qdive.plotting.plotfunctions import plot_data
# exp2.data["Peak Field on Sample [mT]"]
plot_data(exp2.data, exp2.data, x="Peak Field on Sample [mT]", y="Surface Resistance [nOhm]", LineW = 1)