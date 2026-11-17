"""Score candidate pairs with Snowpark (a dbt Python model).

dbt runs this inside Snowflake: dbt.ref() returns Snowpark DataFrames, the name
similarity is a Python UDF executed on the warehouse, and the result is saved as a table.
"""
from snowflake.snowpark.functions import col, iff, lit
from snowflake.snowpark.types import FloatType, StringType


def name_similarity(a: str, b: str) -> float:
    import difflib

    if not a or not b:
        return 0.0
    # token-sorted comparison, so "abebe kebede" ~ "kebede abebe"
    a_sorted = " ".join(sorted(a.split()))
    b_sorted = " ".join(sorted(b.split()))
    return difflib.SequenceMatcher(None, a_sorted, b_sorted).ratio()


def model(dbt, session):
    dbt.config(materialized="table", schema="linkage")

    name_sim = session.udf.register(
        name_similarity, return_type=FloatType(), input_types=[StringType(), StringType()]
    )

    recs = dbt.ref("int_records_canonical")
    pairs = dbt.ref("int_candidate_pairs")

    left = recs.select(
        col("record_key").alias("l_key"), col("full_name_norm").alias("l_name"),
        col("phone_9").alias("l_phone"), col("dob").alias("l_dob"), col("id_number").alias("l_id"),
    )
    right = recs.select(
        col("record_key").alias("r_key"), col("full_name_norm").alias("r_name"),
        col("phone_9").alias("r_phone"), col("dob").alias("r_dob"), col("id_number").alias("r_id"),
    )

    scored = (
        pairs.join(left, pairs["left_key"] == left["l_key"])
             .join(right, pairs["right_key"] == right["r_key"])
             .select(
                 col("left_key"), col("right_key"), col("blocking_rules"),
                 name_sim(col("l_name"), col("r_name")).alias("name_score"),
                 iff(col("l_phone") == col("r_phone"), lit(1.0), lit(0.0)).alias("phone_match"),
                 iff(col("l_dob") == col("r_dob"), lit(1.0), lit(0.0)).alias("dob_match"),
                 iff(col("l_id") == col("r_id"), lit(1.0), lit(0.0)).alias("id_match"),
             )
    )

    # Simple weighted score; a learned model (e.g. XGBoost on labelled pairs) can replace this later.
    match_score = (col("name_score") * 0.5 + col("phone_match") * 0.2
                   + col("dob_match") * 0.15 + col("id_match") * 0.15)
    return (scored.with_column("match_score", match_score)
                  .with_column("is_match", col("match_score") >= lit(0.8)))
