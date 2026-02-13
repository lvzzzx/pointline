from __future__ import annotations

import pytest

from pointline.vendors.tardis import (
    get_tardis_parser,
    parse_tardis_incremental_l2,
    parse_tardis_trades,
)


def test_dispatch_maps_incremental_book_aliases() -> None:
    assert get_tardis_parser("trades") is parse_tardis_trades
    assert get_tardis_parser("incremental_book_L2") is parse_tardis_incremental_l2
    assert get_tardis_parser("incremental_book_l2") is parse_tardis_incremental_l2


def test_dispatch_rejects_unknown_data_type() -> None:
    with pytest.raises(ValueError, match="Unsupported Tardis data_type"):
        get_tardis_parser("book_snapshot_25")
