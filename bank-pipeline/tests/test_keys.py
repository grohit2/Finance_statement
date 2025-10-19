from libs.keys import txn_id

def test_txn_id_stability():
    a = txn_id("lokesh","ACC1","2024-01-01", -123.45, "Amazon India")
    b = txn_id("lokesh","ACC1","2024-01-01", -123.45, "Amazon India")
    assert a == b and len(a) == 16
