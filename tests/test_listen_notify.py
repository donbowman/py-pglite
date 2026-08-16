"""Tests for PostgreSQL LISTEN/NOTIFY support through py-pglite.

These tests mirror the official PGlite NOTIFY example
(https://pglite.dev/examples/notify), which LISTENs on a channel and then
NOTIFYs payloads to it. They verify that clients connecting to a py-pglite
managed PGlite instance can subscribe to a channel and receive notifications.
"""

import asyncio

import pytest

from py_pglite import PGliteManager


try:
    import psycopg
except ImportError:
    psycopg = None

try:
    import asyncpg
except ImportError:
    asyncpg = None


pytestmark = pytest.mark.skipif(
    psycopg is None and asyncpg is None,
    reason="requires psycopg or asyncpg",
)


class TestListenNotifyPsycopg:
    """Test LISTEN/NOTIFY using the psycopg driver."""

    @staticmethod
    def _notification(conn, timeout=5.0):
        """Return the next notification as (channel, payload), or None."""
        return next(conn.notifies(timeout=timeout), None)

    def test_listen_and_notify_same_connection(self):
        """LISTEN then NOTIFY on the same connection delivers the payload.

        Mirrors the PGlite NOTIFY example, which listens on a channel and
        then sends a NOTIFY with a payload on the same instance.
        """
        with PGliteManager() as manager:
            with psycopg.connect(manager.get_dsn(), autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("LISTEN test")
                with conn.cursor() as cur:
                    cur.execute("NOTIFY test, 'Hello, world!'")

                notification = self._notification(conn)
                assert notification is not None
                assert notification.channel == "test"
                assert notification.payload == "Hello, world!"

    def test_listen_and_notify_multiple_payloads(self):
        """Multiple NOTIFYs on a channel are received in order."""
        with PGliteManager() as manager:
            with psycopg.connect(manager.get_dsn(), autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("LISTEN test")
                with conn.cursor() as cur:
                    cur.execute("NOTIFY test, 'Hello, world!'")
                with conn.cursor() as cur:
                    cur.execute("NOTIFY test, 'Hello, world again!'")

                notifications = []
                for n in conn.notifies(timeout=5.0):
                    notifications.append((n.channel, n.payload))
                    if len(notifications) == 2:
                        break

                assert notifications == [
                    ("test", "Hello, world!"),
                    ("test", "Hello, world again!"),
                ]

    def test_unlisten_stops_notifications(self):
        """After UNLISTEN a channel no further notifications are received.

        Mirrors the unsubscribe step of the PGlite NOTIFY example, after
        which a NOTIFY is sent but must not be received.
        """
        with PGliteManager() as manager:
            with psycopg.connect(manager.get_dsn(), autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("LISTEN test")
                with conn.cursor() as cur:
                    cur.execute("UNLISTEN test")
                with conn.cursor() as cur:
                    cur.execute("NOTIFY test, 'Will not be received!'")

                assert self._notification(conn, timeout=1.0) is None

    def test_notify_without_listen_is_ignored(self):
        """NOTIFY on a channel nobody is listening to is ignored."""
        with PGliteManager() as manager:
            with psycopg.connect(manager.get_dsn(), autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("NOTIFY test, 'no listeners'")

                assert self._notification(conn, timeout=1.0) is None

    def test_listen_notify_special_characters(self):
        """Channel and payload handling matches PostgreSQL semantics.

        Unquoted channel names are folded to lowercase and payloads can
        contain special characters.
        """
        with PGliteManager() as manager:
            with psycopg.connect(manager.get_dsn(), autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("LISTEN MyChannel")
                with conn.cursor() as cur:
                    cur.execute("NOTIFY mychannel, 'paYloAd&with special! chars'")

                notification = self._notification(conn)
                assert notification is not None
                assert notification.channel == "mychannel"
                assert notification.payload == "paYloAd&with special! chars"


class TestListenNotifyAsyncpg:
    """Test LISTEN/NOTIFY using the asyncpg driver."""

    @staticmethod
    async def _connect(config):
        if config.use_tcp:
            return await asyncpg.connect(
                host=config.tcp_host,
                port=config.tcp_port,
                user="postgres",
                password="postgres",
                database="postgres",
                ssl=False,
                server_settings={},
            )
        socket_dir = config.socket_path.rsplit("/", 1)[0]
        return await asyncpg.connect(
            host=socket_dir,
            user="postgres",
            password="postgres",
            database="postgres",
            server_settings={},
        )

    def test_listen_and_notify_asyncpg(self):
        """asyncpg add_listener receives NOTIFY payloads from the session."""
        if asyncpg is None:
            pytest.skip("asyncpg not available")

        async def run():
            received = []
            ready = asyncio.Event()

            with PGliteManager() as manager:
                conn = await asyncio.wait_for(
                    self._connect(manager.config), timeout=10.0
                )
                try:

                    def on_notification(connection, pid, channel, payload):
                        received.append((channel, payload))
                        ready.set()

                    await conn.add_listener("testchan", on_notification)
                    await conn.execute("NOTIFY testchan, 'Hello, world!'")

                    await asyncio.wait_for(ready.wait(), timeout=5.0)

                    assert received == [("testchan", "Hello, world!")]
                finally:
                    try:
                        await asyncio.wait_for(conn.close(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass

        asyncio.run(run())

    def test_remove_listener_stops_notifications_asyncpg(self):
        """asyncpg remove_listener stops receiving NOTIFY payloads."""
        if asyncpg is None:
            pytest.skip("asyncpg not available")

        async def run():
            received = []
            fired = asyncio.Event()

            with PGliteManager() as manager:
                conn = await asyncio.wait_for(
                    self._connect(manager.config), timeout=10.0
                )
                try:

                    def on_notification(connection, pid, channel, payload):
                        received.append((channel, payload))
                        fired.set()

                    await conn.add_listener("testchan", on_notification)
                    await conn.remove_listener("testchan", on_notification)
                    await conn.execute("NOTIFY testchan, 'should not arrive'")

                    # Give the protocol a moment to (incorrectly) deliver.
                    try:
                        await asyncio.wait_for(fired.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        pass

                    assert received == []
                finally:
                    try:
                        await asyncio.wait_for(conn.close(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass

        asyncio.run(run())
