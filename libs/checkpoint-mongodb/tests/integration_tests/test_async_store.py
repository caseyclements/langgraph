import asyncio
from concurrent.futures import ThreadPoolExecutor
import pytest

from langgraph.store.mongodb import MongoDBStore

import os
from datetime import datetime
from typing import Generator

import pytest
from pymongo import MongoClient

from langgraph.store.base import Item, TTLConfig, PutOp, GetOp, ListNamespacesOp, MatchCondition
from langgraph.store.mongodb import MongoDBStore

MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "langgraph-test")
COLLECTION_NAME = "long_term_memory"


@pytest.fixture
def ttl():
    return 3600

@pytest.fixture
def store(ttl) -> Generator:
    """Create a simple store following that in base's test_list_namespaces_basic"""
    client = MongoClient(MONGODB_URI)
    collection = client[DB_NAME][COLLECTION_NAME]
    collection.delete_many({})
    collection.drop_indexes()

    mdbstore = MongoDBStore(
        collection,
        ttl_config=TTLConfig(default_ttl=ttl, refresh_on_read=True),
    )

    yield mdbstore

    if client:
        client.close()


async def test_large_batches(store: MongoDBStore) -> None:
    N = 100  # less important that we are performant here
    M = 10

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for m in range(M):
            for i in range(N):
                futures += [
                    executor.submit(
                        store.put,
                        ("test", "foo", "bar", "baz", str(m % 2)),
                        f"key{i}",
                        value={"foo": "bar" + str(i)},
                    ),
                    executor.submit(
                        store.get,
                        ("test", "foo", "bar", "baz", str(m % 2)),
                        f"key{i}",
                    ),
                    executor.submit(
                        store.list_namespaces,
                        prefix=None,
                        max_depth=m + 1,
                    ),
                    executor.submit(
                        store.search,
                        ("test",),
                    ),
                    executor.submit(
                        store.put,
                        ("test", "foo", "bar", "baz", str(m % 2)),
                        f"key{i}",
                        value={"foo": "bar" + str(i)},
                    ),
                    executor.submit(
                        store.put,
                        ("test", "foo", "bar", "baz", str(m % 2)),
                        f"key{i}",
                        None,
                    ),
                ]

        results = await asyncio.gather(
            *(asyncio.wrap_future(future) for future in futures)
        )
    assert len(results) == M * N * 6