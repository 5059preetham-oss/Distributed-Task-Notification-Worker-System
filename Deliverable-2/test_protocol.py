import unittest

from protocol import encode_message, read_messages


class ProtocolTests(unittest.TestCase):
    def test_fragmented_and_coalesced_frames(self) -> None:
        payload = encode_message({"type": "REGISTER", "worker_id": "w1"})
        payload += encode_message({"type": "HEARTBEAT"})
        buffer, messages = read_messages(b"", payload[:7])
        self.assertEqual(messages, [])
        buffer, messages = read_messages(buffer, payload[7:])
        self.assertEqual(
            messages,
            [{"type": "REGISTER", "worker_id": "w1"}, {"type": "HEARTBEAT"}],
        )
        self.assertEqual(buffer, b"")

    def test_incomplete_frame_is_retained(self) -> None:
        buffer, messages = read_messages(b"", b'{"type":"RESULT"}')
        self.assertEqual(messages, [])
        self.assertEqual(buffer, b'{"type":"RESULT"}')


if __name__ == "__main__":
    unittest.main()
