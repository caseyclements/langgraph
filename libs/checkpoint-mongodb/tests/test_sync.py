from typing import Any, Dict

import pytest
from bson.errors import InvalidDocument
from langchain_core.runnables import RunnableConfig
from pymongo import MongoClient

from langgraph.checkpoint.base import (
    CheckpointMetadata,
    create_checkpoint,
    empty_checkpoint,
)
from langgraph.checkpoint.mongodb import MongoDBSaver

# Setup:
# docker pull mongodb/mongodb-atlas-local:latest
# docker run --name mongodb -d -p 27017:27017 mongodb/mongodb-community-serve
MONGODB_URI = "mongodb://localhost:27017"
DB_NAME = "langgraph-test"

# TODO: Test
#   - before
#   - limit
#   - values in checkpoint
#   - Adding non-trivial additional checkpoints
#   - Add non-trivial metadata: from langchain_core.messages import HumanMessage


@pytest.fixture(scope="session")
def input_data() -> dict:
    """Setup and store conveniently in a single dictionary."""
    inputs: Dict[str, Any] = {}

    inputs["config_1"] = RunnableConfig(
        configurable=dict(thread_id="thread-1", thread_ts="1", checkpoint_ns="")
    )  # config_1 tests deprecated thread_ts

    inputs["config_2"] = RunnableConfig(
        configurable=dict(thread_id="thread-2", checkpoint_id="2", checkpoint_ns="")
    )

    inputs["config_3"] = RunnableConfig(
        configurable=dict(
            thread_id="thread-2", checkpoint_id="2-inner", checkpoint_ns="inner"
        )
    )

    inputs["chkpnt_1"] = empty_checkpoint()
    inputs["chkpnt_2"] = create_checkpoint(inputs["chkpnt_1"], {}, 1)
    inputs["chkpnt_3"] = empty_checkpoint()

    inputs["metadata_1"] = CheckpointMetadata(
        source="input", step=2, writes={}, score=1
    )
    inputs["metadata_2"] = CheckpointMetadata(
        source="loop", step=1, writes={"foo": "bar"}, score=None
    )
    inputs["metadata_3"] = CheckpointMetadata()

    return inputs


def test_search(input_data: Dict[str, Any]) -> None:
    # Clear collections if they exist
    client: MongoClient = MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    for clxn_name in db.list_collection_names():
        db.drop_collection(clxn_name)

    with MongoDBSaver.from_conn_string(MONGODB_URI, DB_NAME) as saver:
        # save checkpoints
        saver.put(
            input_data["config_1"],
            input_data["chkpnt_1"],
            input_data["metadata_1"],
            {},
        )
        saver.put(
            input_data["config_2"],
            input_data["chkpnt_2"],
            input_data["metadata_2"],
            {},
        )
        saver.put(
            input_data["config_3"],
            input_data["chkpnt_3"],
            input_data["metadata_3"],
            {},
        )

        # call method / assertions
        query_1 = {"source": "input"}  # search by 1 key
        query_2 = {
            "step": 1,
            "writes": {"foo": "bar"},
        }  # search by multiple keys
        query_3: dict[str, Any] = {}  # search by no keys, return all checkpoints
        query_4 = {"source": "update", "step": 1}  # no match

        search_results_1 = list(saver.list(None, filter=query_1))
        assert len(search_results_1) == 1
        assert search_results_1[0].metadata == input_data["metadata_1"]

        search_results_2 = list(saver.list(None, filter=query_2))
        assert len(search_results_2) == 1
        assert search_results_2[0].metadata == input_data["metadata_2"]

        search_results_3 = list(saver.list(None, filter=query_3))
        assert len(search_results_3) == 3

        search_results_4 = list(saver.list(None, filter=query_4))
        assert len(search_results_4) == 0

        # search by config (defaults to checkpoints across all namespaces)
        search_results_5 = list(saver.list({"configurable": {"thread_id": "thread-2"}}))
        assert len(search_results_5) == 2
        assert {
            search_results_5[0].config["configurable"]["checkpoint_ns"],
            search_results_5[1].config["configurable"]["checkpoint_ns"],
        } == {"", "inner"}


def test_null_chars(input_data: Dict[str, Any]) -> None:
    """In MongoDB string *values* can be any valid UTF-8 including nulls.
    *Field names*, however, cannot contain nulls characters."""
    with MongoDBSaver.from_conn_string(MONGODB_URI, DB_NAME) as saver:
        null_str = "\x00abc"  # string containing null character

        # 1. null string in field *value*
        null_value_cfg = saver.put(
            input_data["config_1"],
            input_data["chkpnt_1"],
            {"my_key": null_str},
            {},
        )
        assert saver.get_tuple(null_value_cfg).metadata["my_key"] == null_str  # type: ignore
        assert (
            list(saver.list(None, filter={"my_key": null_str}))[0].metadata["my_key"]  # type: ignore
            == null_str
        )

        # 2. null string in field *name*
        with pytest.raises(InvalidDocument):
            saver.put(
                input_data["config_1"],
                input_data["chkpnt_1"],
                {null_str: "my_value"},  # type: ignore
                {},
            )