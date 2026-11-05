"""Unit tests for column transformations, on tiny in-memory DataFrames."""
import base64

from pyspark.sql import functions as F

from falcon_dwh.facts import parse_request_data
from falcon_dwh.transforms import decode_fid, tf_bool


def _fid(text, pad=True):
    s = base64.urlsafe_b64encode(text.encode()).decode()
    return s if pad else s.rstrip("=")


def _decode(spark, fids):
    df = spark.createDataFrame([(f,) for f in fids], "fid string")
    rid, pos = decode_fid(F.col("fid"))
    return [tuple(r) for r in df.select(rid, pos).collect()]


def test_decode_fid_real_value(spark):
    # real fid from the impressions table
    assert _decode(spark, ["NmM5ZDEzNjYyNTM4ZTdiYTow"]) == [("6c9d13662538e7ba", 0)]


def test_decode_fid_without_padding_and_url_alphabet(spark):
    # "ab?>:12" encodes to a value with '-' / '_' (url alphabet) and needs padding when stripped
    fid = _fid("ab?>~:12", pad=False)
    assert "-" in fid or "_" in fid
    assert _decode(spark, [fid, _fid("ab?>~:12")]) == [("ab?>~", 12), ("ab?>~", 12)]


def test_decode_fid_bad_values_are_null_not_errors(spark):
    bad = [None, "", "!!!notbase64!!!", _fid("no-colon-here"), _fid("abc:x1")]
    assert _decode(spark, bad) == [(None, None)] * len(bad)


def test_tf_bool(spark):
    df = spark.createDataFrame([("t",), ("f",), (None,), ("true",)], "v string")
    assert [r[0] for r in df.select(tf_bool(F.col("v"))).collect()] == [True, False, None, None]


def test_parse_request_data_handles_key_spellings(spark):
    rows = [
        ('{"orderData":{"amount":"33.83","currency":"usd","orderid":"A1"}}',),
        ('{"orderData":{"order_value":"10","currency":"EUR","orderId":"B2"}}',),
        ('{"orderData":{"amount":"n/a","orderID":"C3","cartItems":"[{\\"name\\":\\"x\\"},{\\"name\\":\\"y\\"}]"}}',),
        ('{"userIdentifiers":{}}',),
    ]
    df = spark.createDataFrame(rows, "data string").select(*parse_request_data(F.col("data")))
    df = df.withColumn("order_amount", F.expr("try_cast(trim(order_amount_raw) as double)"))
    got = [(r.order_id, r.order_amount, r.order_currency, r.cart_item_count) for r in df.collect()]
    assert got == [
        ("A1", 33.83, "USD", None),
        ("B2", 10.0, "EUR", None),
        ("C3", None, None, 2),  # non-numeric amount -> null, counted in DQ
        (None, None, None, None),
    ]
