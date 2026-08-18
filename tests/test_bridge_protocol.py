from __future__ import annotations

import unittest

from astrbot_plugin_gateway_universal.bridge_protocol import (
    build_chat_id,
    build_websocket_url,
    resolve_delivery_temp_dir,
    safe_filename,
)


class BridgeProtocolTest(unittest.TestCase):
    def test_builds_profile_scoped_websocket_url(self):
        self.assertEqual(
            build_websocket_url("http://hermes:8643", "default"),
            "ws://hermes:8643/p/default/astrbot/ws",
        )

    def test_keeps_existing_profile_prefix(self):
        self.assertEqual(
            build_websocket_url("https://example.test/p/custom", "ignored"),
            "wss://example.test/p/custom/astrbot/ws",
        )

    def test_group_session_is_shared_by_group(self):
        first = build_chat_id("aiocqhttp", "10001", "20001")
        second = build_chat_id("aiocqhttp", "10002", "20001")
        self.assertEqual(first, second)
        self.assertEqual(first, "astrbot:aiocqhttp:group:20001")

    def test_private_session_is_per_user(self):
        self.assertNotEqual(
            build_chat_id("aiocqhttp", "10001"),
            build_chat_id("aiocqhttp", "10002"),
        )

    def test_filename_drops_paths_and_unsafe_characters(self):
        self.assertEqual(safe_filename("../bad:name.txt"), "bad_name.txt")

    def test_resolve_delivery_temp_dir(self):
        temp_dir = resolve_delivery_temp_dir()
        self.assertTrue(temp_dir.exists())
        self.assertTrue(temp_dir.is_dir())


if __name__ == "__main__":
    unittest.main()

