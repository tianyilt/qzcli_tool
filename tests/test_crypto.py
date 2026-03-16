import unittest

from qzcli.crypto import encrypt_password


class CryptoTests(unittest.TestCase):
    def test_encrypt_password_preserves_leading_zero(self):
        encrypted = encrypt_password("n")

        self.assertEqual(256, len(encrypted))
        self.assertTrue(encrypted.startswith("0"))
        self.assertEqual(
            "09711f4e0dbf11cdd9a2f391feffc66b236727b2e9e24d9b480bafb8fab55986"
            "dab3c0aaa05f404241b96ff8ad44f454f3f0121c4a1399b25039327aac49c4cc"
            "ae653a916b81e8f129d16381c1cc1ea40d0d5e05a75ad2ff8f38de60edd51ac5"
            "cda7449eae6fdce4f1275dfcaed5f66905f368a16151cbf795b404bc7f0c7803",
            encrypted,
        )


if __name__ == "__main__":
    unittest.main()
