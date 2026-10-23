import os
import unittest
from unittest.mock import patch

import fetch_data


class StockUniverseTests(unittest.TestCase):
    def test_staged_update_rejects_cached_universe_after_network_error(self):
        with patch.dict(os.environ, {"STOCK_REQUIRE_FRESH_UNIVERSE": "1"}):
            with patch.object(fetch_data.requests, "get", side_effect=TimeoutError("offline")):
                with self.assertRaisesRegex(RuntimeError, "交易所清單"):
                    fetch_data.get_tw_stock_list()


if __name__ == "__main__":
    unittest.main()
