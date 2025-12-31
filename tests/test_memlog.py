from reproq_django import memlog


def test_parse_interval_seconds():
    assert memlog._parse_interval_seconds("60s") == 60
    assert memlog._parse_interval_seconds("2m") == 120
    assert memlog._parse_interval_seconds("1h") == 3600
    assert memlog._parse_interval_seconds("45") == 45
    assert memlog._parse_interval_seconds("0") is None
    assert memlog._parse_interval_seconds("off") is None
    assert memlog._parse_interval_seconds("nope") is None
