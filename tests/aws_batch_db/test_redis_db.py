def test_redis(redisdb):
    """Check that it's actually working on aws_batch_db database."""
    redisdb.set("key1", "value1")
    redisdb.set("key2", "value2")
    assert redisdb.get("key1") == b"value1"
    assert redisdb.get("key2") == b"value2"


def test_redis_client(redisdb):
    redis_client = redisdb.client()
    assert "sock" in redis_client.connection.path
