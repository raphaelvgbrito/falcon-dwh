"""Small column-level transformations, kept pure so they can be unit tested on tiny DataFrames."""
from pyspark.sql import Column
from pyspark.sql import functions as F

FID_PATTERN = r"^([^:]+):([0-9]+)$"


def decode_fid(fid: Column):
    """fid is base64url("request_id:position"), maybe without '=' padding.

    Returns (request_id, position) columns; both null when the fid is null or does not decode to
    the expected "id:number" form. try_to_binary returns null instead of failing on bad input.
    """
    std = F.translate(fid, "-_", "+/")
    padded = F.rpad(std, (F.ceil(F.length(std) / 4) * 4).cast("int"), "=")
    decoded = F.call_function("try_to_binary", padded, F.lit("base64")).cast("string")
    ok = decoded.rlike(FID_PATTERN)
    request_id = F.when(ok, F.regexp_extract(decoded, FID_PATTERN, 1))
    position = F.when(ok, F.regexp_extract(decoded, FID_PATTERN, 2).cast("int"))
    return request_id, position


def tf_bool(col: Column):
    """Postgres CSV booleans: 't' / 'f' -> true / false; anything else -> null."""
    return F.when(col == "t", True).when(col == "f", False)


def parse_user_agent(ua: Column):
    """(device_type, os, os_major, browser) from a raw user agent, with built-in functions only.

    Rules are ordered: more specific tokens first (Edge/Samsung/in-app before Chrome, Chrome
    before Safari, iPhone before 'Mac OS X' which iPhone UAs also contain). The publisher app
    sends its own format: "app/1.0 app/12.24.30 (env: prod; platform: ios; ...; model: iPhone)".
    Known limit: iPadOS Safari pretends to be a Mac, so those iPads count as desktop.
    """
    has = lambda pattern: ua.rlike(pattern)  # noqa: E731
    is_app = has("^app/")
    device = (
        F.when(ua.isNull() | (F.trim(ua) == ""), "unknown")
        .when(has("(?i)bot|crawler|spider"), "bot")
        .when(has("iPad|model: iPad|Tablet") | (has("Android") & ~has("Mobile") & ~is_app & ~has("Dalvik")), "tablet")
        .when(has("iPhone|iPod|Android|Mobile|platform: (ios|android)|iOS/"), "mobile")
        .when(has("Windows|Macintosh|X11|CrOS"), "desktop")
        .otherwise("other")
    )
    os = (
        F.when(has("platform: ios|iPhone|iPad|iPod|iOS/"), "iOS")
        .when(has("platform: android|Android"), "Android")
        .when(has("CrOS"), "ChromeOS")
        .when(has("Windows"), "Windows")
        .when(has("Macintosh|Mac OS X"), "macOS")
        .when(has("Linux|X11"), "Linux")
        .otherwise("other")
    )
    os_major = F.coalesce(
        F.nullif(F.regexp_extract(ua, r"osVersion: (\d+)", 1), F.lit("")),
        F.nullif(F.regexp_extract(ua, r"(?:iPhone OS|CPU OS) (\d+)", 1), F.lit("")),
        F.nullif(F.regexp_extract(ua, r"Android (\d+)", 1), F.lit("")),
    ).cast("int")
    browser = (
        F.when(ua.isNull(), "unknown")
        .when(is_app, "publisher app")
        .when(has("FBAN|FBAV|FB_IAB"), "Facebook in-app")
        .when(has("Instagram"), "Instagram in-app")
        .when(has("Dalvik|^com\\."), "native app")
        .when(has("wv\\)"), "Android WebView")
        .when(has("Edg/|EdgiOS/|EdgA/"), "Edge")
        .when(has("SamsungBrowser"), "Samsung Internet")
        .when(has("OPR/|Opera"), "Opera")
        .when(has("CriOS|Chrome/"), "Chrome")
        .when(has("FxiOS|Firefox/"), "Firefox")
        .when(has("Version/.*Safari/"), "Safari")
        .when(has("iPhone|iPad"), "iOS WebView")
        .otherwise("other")
    )
    return device, os, os_major, browser


def time_key(ts: Column):
    """Key into dim_time: HHMM of the (UTC) timestamp as an int, e.g. 14:35:12 -> 1435."""
    return (F.hour(ts) * 100 + F.minute(ts)).cast("int")


def utc_ts(col: Column):
    """Parquet timestamps are stored without zone (UTC by convention). With the session time zone
    set to UTC, casting to TIMESTAMP keeps the same wall-clock value."""
    return col.cast("timestamp")
