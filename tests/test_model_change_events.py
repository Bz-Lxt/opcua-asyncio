import asyncio
from contextlib import suppress
from typing import NamedTuple

import pytest

from asyncua import Client, Server, ua

from .conftest import find_free_port


class EventCollector:
    def __init__(self) -> None:
        self.events: asyncio.Queue = asyncio.Queue()
        self.datachanges: asyncio.Queue = asyncio.Queue()

    def event_notification(self, event) -> None:
        self.events.put_nowait(event)

    def datachange_notification(self, node, val, data) -> None:
        self.datachanges.put_nowait(val)

    async def next_event(self):
        return await asyncio.wait_for(self.events.get(), timeout=3)

    async def assert_no_event(self) -> None:
        await asyncio.sleep(0.3)
        assert self.events.empty()


class Harness(NamedTuple):
    srv: Server
    clt: Client
    idx: int
    handler: EventCollector
    folder: object
    plain: object


@pytest.fixture
async def model_change_server():
    port = find_free_port()
    srv = Server()
    await srv.init()
    srv.set_endpoint(f"opc.tcp://127.0.0.1:{port}")
    idx = await srv.register_namespace("urn:test:modelchanges")
    await srv.start()
    clt = Client(f"opc.tcp://admin@127.0.0.1:{port}", timeout=10)
    await clt.connect()
    yield srv, clt, idx
    await clt.disconnect()
    await srv.stop()


@pytest.fixture
async def subscribed(model_change_server):
    srv, clt, idx = model_change_server
    objects = clt.nodes.objects
    folder = await objects.add_folder(idx, "VersionedFolder")
    await folder.add_property(0, "NodeVersion", "1")
    plain = await objects.add_folder(idx, "UnversionedFolder")

    handler = EventCollector()
    sub = await clt.create_subscription(100, handler)
    evtype = clt.get_node(ua.ObjectIds.GeneralModelChangeEventType)
    await sub.subscribe_events(ua.ObjectIds.Server, evtype)
    await asyncio.sleep(0.1)

    yield Harness(srv=srv, clt=clt, idx=idx, handler=handler, folder=folder, plain=plain)

    with suppress(Exception):
        await sub.delete()


def changes_by_node(event) -> dict[ua.NodeId, ua.ModelChangeStructureDataType]:
    return {change.Affected: change for change in event.Changes}


def assert_general_model_change_event(event) -> None:
    assert event.EventType == ua.NodeId(ua.ObjectIds.GeneralModelChangeEventType)
    assert isinstance(event.Changes, list)
    for change in event.Changes:
        assert isinstance(change, ua.ModelChangeStructureDataType)


def assert_ordered(event) -> None:
    affected = [change.Affected.to_string() for change in event.Changes]
    assert affected == sorted(affected)


async def test_add_node_emits_model_change_event(subscribed: Harness):
    child = await subscribed.folder.add_object(subscribed.idx, "AddedObject")

    event = await subscribed.handler.next_event()
    assert_general_model_change_event(event)
    assert_ordered(event)
    changes = changes_by_node(event)

    assert set(changes) == {subscribed.folder.nodeid, child.nodeid}
    assert changes[child.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.NodeAdded)
    assert changes[child.nodeid].AffectedType == ua.NodeId(ua.ObjectIds.BaseObjectType)
    assert changes[subscribed.folder.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.ReferenceAdded)
    assert changes[subscribed.folder.nodeid].AffectedType == ua.NodeId(ua.ObjectIds.FolderType)


async def test_add_variable_type_definition_in_change(subscribed: Harness):
    var = await subscribed.folder.add_variable(subscribed.idx, "AddedVariable", 1)

    event = await subscribed.handler.next_event()
    changes = changes_by_node(event)
    assert changes[var.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.NodeAdded)
    assert changes[var.nodeid].AffectedType == ua.NodeId(ua.ObjectIds.BaseDataVariableType)


async def test_delete_node_emits_model_change_event(subscribed: Harness):
    child = await subscribed.folder.add_object(subscribed.idx, "SoonDeleted")
    await subscribed.handler.next_event()

    await subscribed.clt.delete_nodes([child])

    event = await subscribed.handler.next_event()
    assert_ordered(event)
    changes = changes_by_node(event)
    assert set(changes) == {subscribed.folder.nodeid, child.nodeid}
    assert changes[child.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.NodeDeleted)
    assert changes[child.nodeid].AffectedType == ua.NodeId(ua.ObjectIds.BaseObjectType)
    assert changes[subscribed.folder.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.ReferenceDeleted)


async def test_add_and_delete_references_emit_events(subscribed: Harness):
    other = await subscribed.plain.add_object(subscribed.idx, "ReferencedObject")

    await subscribed.folder.add_reference(other, reftype=ua.ObjectIds.Organizes, forward=True)
    event = await subscribed.handler.next_event()
    assert_ordered(event)
    changes = changes_by_node(event)
    assert set(changes) == {subscribed.folder.nodeid, other.nodeid}
    assert changes[subscribed.folder.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.ReferenceAdded)
    assert changes[other.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.ReferenceAdded)

    await subscribed.folder.delete_reference(other, reftype=ua.ObjectIds.Organizes, forward=True)
    event = await subscribed.handler.next_event()
    changes = changes_by_node(event)
    assert changes[subscribed.folder.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.ReferenceDeleted)
    assert changes[other.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.ReferenceDeleted)


async def test_data_type_change_emits_event(subscribed: Harness):
    var = await subscribed.folder.add_variable(subscribed.idx, "TypedVariable", 1)
    await var.add_property(0, "NodeVersion", "1")
    await subscribed.handler.next_event()
    await subscribed.handler.next_event()

    await var.write_attribute(
        ua.AttributeIds.DataType,
        ua.DataValue(ua.Variant(ua.NodeId(ua.ObjectIds.String), ua.VariantType.NodeId)),
    )

    event = await subscribed.handler.next_event()
    changes = changes_by_node(event)
    assert set(changes) == {var.nodeid}
    assert changes[var.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.DataTypeChanged)


async def test_unversioned_nodes_stay_silent(subscribed: Harness):
    await subscribed.plain.add_object(subscribed.idx, "SilentObject")
    await subscribed.plain.add_folder(subscribed.idx, "SilentFolder")
    linked = await subscribed.plain.add_object(subscribed.idx, "SilentLinked")
    await subscribed.plain.add_reference(linked, reftype=ua.ObjectIds.Organizes, forward=True)
    await subscribed.handler.assert_no_event()


async def test_node_version_bumped_once_and_datachange_sent(subscribed: Harness):
    server_folder = subscribed.srv.get_node(subscribed.folder.nodeid)
    version_node = await subscribed.folder.get_child(["0:NodeVersion"])
    sub = await subscribed.clt.create_subscription(100, subscribed.handler)
    await sub.subscribe_data_change(version_node)
    await asyncio.sleep(0.2)
    while not subscribed.handler.datachanges.empty():
        subscribed.handler.datachanges.get_nowait()
    baseline = int(await version_node.read_value())

    with subscribed.srv.model_change_transaction():
        await server_folder.add_object(subscribed.idx, "BatchedA")
        await server_folder.add_object(subscribed.idx, "BatchedB")

    event = await subscribed.handler.next_event()
    changes = changes_by_node(event)
    assert len(changes) == 3
    assert changes[subscribed.folder.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.ReferenceAdded)
    for nodeid, change in changes.items():
        if nodeid != subscribed.folder.nodeid:
            assert change.Verb == int(ua.ModelChangeStructureVerbMask.NodeAdded)

    expected = str(baseline + 1)
    assert await version_node.read_value() == expected
    bumped = await asyncio.wait_for(subscribed.handler.datachanges.get(), timeout=3)
    assert bumped == expected
    await sub.delete()


async def test_explicit_transaction_compresses_to_single_event(subscribed: Harness):
    server_folder = subscribed.srv.get_node(subscribed.folder.nodeid)
    with subscribed.srv.model_change_transaction():
        first = await server_folder.add_object(subscribed.idx, "TxA")
        second = await server_folder.add_object(subscribed.idx, "TxB")
        third = await server_folder.add_object(subscribed.idx, "TxC")

    event = await subscribed.handler.next_event()
    changes = changes_by_node(event)
    assert_ordered(event)
    assert set(changes) == {
        subscribed.folder.nodeid,
        first.nodeid,
        second.nodeid,
        third.nodeid,
    }
    added = [
        change.Affected for change in event.Changes if change.Verb & int(ua.ModelChangeStructureVerbMask.NodeAdded)
    ]
    assert sorted(added, key=lambda nid: nid.to_string()) == added
    await subscribed.handler.assert_no_event()


async def test_non_numeric_node_version_is_kept(subscribed: Harness):
    versioned = await subscribed.folder.add_object(subscribed.idx, "TextVersioned")
    version_prop = await versioned.add_property(0, "NodeVersion", "draft")
    await subscribed.handler.next_event()
    await subscribed.handler.next_event()

    await versioned.add_object(subscribed.idx, "Child")
    await subscribed.handler.next_event()

    assert await version_prop.read_value() == "draft"


async def test_concurrent_modifications_each_get_consistent_event(subscribed: Harness):
    count = 10
    version_node = await subscribed.folder.get_child(["0:NodeVersion"])
    await asyncio.sleep(0.1)
    baseline = int(await version_node.read_value())

    children = await asyncio.gather(
        *[subscribed.folder.add_object(subscribed.idx, f"Concurrent{i}") for i in range(count)]
    )
    child_ids = {node.nodeid for node in children}

    seen_added: set[ua.NodeId] = set()
    for _ in range(count):
        event = await subscribed.handler.next_event()
        changes = changes_by_node(event)
        assert subscribed.folder.nodeid in changes
        assert changes[subscribed.folder.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.ReferenceAdded)
        added = {
            nodeid for nodeid, change in changes.items() if change.Verb & int(ua.ModelChangeStructureVerbMask.NodeAdded)
        }
        assert len(added) == 1
        assert added <= child_ids
        seen_added.update(added)

    assert seen_added == child_ids
    assert await version_node.read_value() == str(baseline + count)
    await subscribed.handler.assert_no_event()


async def test_delete_node_clearing_references(subscribed: Harness):
    child = await subscribed.folder.add_object(subscribed.idx, "DeletedWithReferences")
    await subscribed.handler.next_event()

    await child.delete(delete_references=True)

    event = await subscribed.handler.next_event()
    changes = changes_by_node(event)
    assert changes[child.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.NodeDeleted)
    assert changes[subscribed.folder.nodeid].Verb == int(ua.ModelChangeStructureVerbMask.ReferenceDeleted)


async def test_delete_node_keeping_references_is_silent(subscribed: Harness):
    child = await subscribed.folder.add_object(subscribed.idx, "DeletedKeepingReferences")
    await subscribed.handler.next_event()

    await child.delete(delete_references=False)
    await subscribed.handler.assert_no_event()
