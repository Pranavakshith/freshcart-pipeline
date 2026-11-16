"""
Unit tests for RabbitMQ topic exchange dispatcher and driver consumer (Part D).
Author: Pranavakshith (23BCS054)
"""

import sys
import os
import json
import unittest
from unittest.mock import patch, MagicMock

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from stream import dispatcher
from stream import driver


class TestDispatcher(unittest.TestCase):
    """Test suite verifying dispatcher routing key generation and message payloads."""

    def test_build_message_good(self):
        """Standard dispatches (non-10th) must include 'zone' and follow city.zone.type format."""
        rk, msg = dispatcher.build_message(1)
        self.assertIn("zone", msg)
        self.assertNotIn("malformed", msg)
        self.assertIn(msg["city"], dispatcher.CITIES)
        self.assertIn(msg["zone"], dispatcher.ZONES[msg["city"]])
        self.assertIn(msg["type"], dispatcher.TYPES)
        self.assertEqual(rk, f"{msg['city']}.{msg['zone']}.{msg['type']}")

    def test_build_message_bad(self):
        """Every 10th message must be intentionally malformed (missing 'zone') for D3 testing."""
        rk, msg = dispatcher.build_message(10)
        self.assertNotIn("zone", msg)
        self.assertTrue(msg.get("malformed", False))
        # 2-token routing key: city.type
        self.assertEqual(len(rk.split(".")), 2)

    def test_dispatcher_publishes_messages(self):
        """Verify publishing loop emits correct messages and stops on KeyboardInterrupt."""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_conn.channel.return_value = mock_channel

        published = []

        def fake_publish(exchange, routing_key, body):
            published.append((routing_key, json.loads(body)))
            if len(published) == 11:
                raise KeyboardInterrupt()

        mock_channel.basic_publish.side_effect = fake_publish

        with patch('stream.dispatcher.get_connection', return_value=mock_conn):
            with patch('stream.dispatcher.time.sleep'):
                dispatcher.run_dispatcher(max_count=11)

        self.assertEqual(len(published), 11)
        # Message 10 must be malformed
        rk_10, msg_10 = published[9]
        self.assertNotIn("zone", msg_10)
        self.assertTrue(msg_10.get("malformed", False))
        # Message 1 must be well-formed
        rk_1, msg_1 = published[0]
        self.assertIn("zone", msg_1)


class TestDriver(unittest.TestCase):
    """Test suite verifying driver consumer message evaluation and ACK/NACK protocol."""

    def test_driver_callback_ack(self):
        """Valid order messages with non-empty 'zone' must receive basic_ack."""
        mock_ch = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 101
        mock_method.routing_key = "bangalore.koramangala.ride"

        body = json.dumps({
            "id": "REQ-0001",
            "city": "bangalore",
            "zone": "koramangala",
            "type": "ride",
            "ts": 1234567890.0
        }).encode("utf-8")

        driver.process_message("Ravi", mock_ch, mock_method, body)
        mock_ch.basic_ack.assert_called_once_with(delivery_tag=101)
        mock_ch.basic_nack.assert_not_called()

    def test_driver_callback_nack_missing_zone(self):
        """Orders missing 'zone' must be rejected with basic_nack(requeue=False)."""
        mock_ch = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 102
        mock_method.routing_key = "bangalore.food"

        body = json.dumps({
            "id": "REQ-0010",
            "city": "bangalore",
            "type": "food",
            "ts": 1234567890.0,
            "malformed": True
        }).encode("utf-8")

        driver.process_message("Meena", mock_ch, mock_method, body)
        mock_ch.basic_nack.assert_called_once_with(delivery_tag=102, requeue=False)
        mock_ch.basic_ack.assert_not_called()

    def test_driver_callback_nack_corrupt_json(self):
        """Unparseable / non-JSON byte payloads must be rejected with basic_nack(requeue=False)."""
        mock_ch = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 103
        mock_method.routing_key = "chennai.tnagar.food"

        corrupted_body = b"NOT_VALID_JSON_BINARY"

        driver.process_message("Karthik", mock_ch, mock_method, corrupted_body)
        mock_ch.basic_nack.assert_called_once_with(delivery_tag=103, requeue=False)
        mock_ch.basic_ack.assert_not_called()


if __name__ == '__main__':
    unittest.main()
