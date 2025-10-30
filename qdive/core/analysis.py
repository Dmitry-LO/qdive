import polars as pl
import polars.selectors as cs
from pathlib import Path
from typing import Iterable, List, Any
from datetime import date


def cluster_by_proximity(
    df: pl.DataFrame,
    param: str = "Set Temp [K]",
    param_tol: float = 0.09,
) -> pl.DataFrame:
    """
    Group and cluster data points in the given DataFrame based on proximity
    within the specified parameter column.

    The function identifies unique parameter groups in `param` by clustering
    values that differ by less than `param_tol`. At this stage,
    we dont care about other identifiers such as scan or series IDs.

    For example:
        A data point with:
            Set Temp [K] = 2.0, Rs = 10, scan_id = 1
        will be grouped together with another point:
            Set Temp [K] = 2.1, Rs = 100, scan_id = 1,
        since their difference is within `param_tol`.

    The resulting parameter groups will later be used in combination
    with other parameters for further aggregation or analysis.

    Parameters
    ----------
    df : pl.DataFrame
        Input DataFrame containing the data to group.
    param : str, default="Set Temp [K]"
        Name of the column to group by proximity.
    param_tol : float, default=0.1
        Maximum allowed difference between consecutive values in `param`
        to consider them part of the same group.

    Returns
    -------
    pl.DataFrame
        A new DataFrame with an additional _aggroup column indicating
        unique parameter clusters based on proximity.
    """
    grouped_df = (df
        .sort(by=pl.col(param))
        .with_columns(pl.col(param)
            .diff().alias("diff").fill_null(0))
        .with_columns(jump=(pl.col("diff")>param_tol))
        .cast({"jump":pl.UInt8})
        .with_columns((pl.col("jump").cum_sum()+1).alias(param+"_aggroup"))
        .drop(["diff", "jump"])
                 )
    return grouped_df

## TODO: Thorw out non-existing values from schema; think how to make schema more flexible e.g. pass >60 etc 
## or [100] == everything will be gropped in 100 and restr... maybe its done already? 
## Also! Make a separate function aggregate to see how gropping works!
def aggregate_and_compute(
        df: pl.DataFrame,
        schema: dict[str, dict[str, list[Any] | list[list[Any]] | bool]] | None = None,
        stat_list: list[str] | None = None,
        **kwargs
):
    if schema is None:
        schema = {
        "series":         {"groups": []},
            }
    
    if stat_list is None:
        stat_list = [
            "Set Temp [K]",
            "Peak Field on Sample [mT]",
            "Surface Resistance [nOhm]",
        ]

    remove_multiple = kwargs.get("remove_multiple", False)
    with_aggroups = kwargs.get("with_aggroups", False)
    nacked_df_colnames = df.select(~cs.contains("_aggroup")).columns

    # schema.update({name: {"groups": []} for name in df.select(cs.contains("_param_groupe")).columns})
    mapping = build_mapping(schema)
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

    for column_key, mp in mapping.items():
        unite_remaining = schema[column_key].get("unite_rest", False)
        base = pl.col(column_key).cast(pl.String) #default value
        df2 = df2.with_columns(
            base
            .replace_strict(mp, default=base if not unite_remaining else "UNMAPPED") # Replacing values
            # mp is mapping {"100":"group_1"}
            # if oroginal value in column is 100 -> replaced by "group_1"
            # if value is not in map -> will be replaced by base=str(original)
            .alias(f"{column_key}_aggroup") 
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
    aggregation_list += [
        pl.col(colname)
        for colname in df.columns
        if "_aggroup" not in colname
    ]

    # Columns list whose list values will have duplicates removed after group_by.
    # Usually not needed, default is False, since you can always clean duplicates later with `.list.unique()`.
    # Keeping the non-cleaned lists can be useful to inspect the full dataset
    if remove_multiple: 
        unique_list = [pl.col(colname).list.unique().alias(colname) for colname in nacked_df_colnames if colname not in stat_list]
    else:
        unique_list = []
    # we will add sortkey here
    computed_df = (df2
        .group_by([key for key in df2.select(cs.contains("_aggroup")).columns])  #mapping.keys(), key+"_aggroup"
        .agg(aggregation_list)
        .with_columns(unique_list)
        .drop(cs.contains("_aggroup") if not with_aggroups else [])
        )
    return computed_df


def build_mapping(schema: dict) -> dict:
    mapping = {}
    for col, items in schema.items():
        inner = {}
        for i, element in enumerate(items["groups"], start=1):
            group = f"group_{i}"
            if isinstance(element, list):
                for sub in element:
                    inner[str(sub)] = group
            else:
                inner[str(element)] = group
        mapping[col] = inner
    return mapping