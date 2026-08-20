from datetime import datetime, timezone

import pytest

from geesampler.models import SampleRecord
from geesampler.recipes.mtbs import positive_pairs, post_id_date


def test_mtbs_post_id_date_and_pair_chronology():
    event = SampleRecord(
        "event",
        {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
        datetime(2020, 7, 1, tzinfo=timezone.utc),
        {"Event_ID": "event", "Post_ID": "LC08_044033_20200817"},
    )
    pairs = list(positive_pairs([event]))
    assert [item.properties["Phase"] for item in pairs] == ["pre", "post"]
    assert pairs[0].date < pairs[1].date
    assert post_id_date("LC08_044033_20200817").date().isoformat() == "2020-08-17"
    assert post_id_date("802403920180112").date().isoformat() == "2018-01-12"


def test_mtbs_post_id_requires_a_date():
    with pytest.raises(ValueError):
        post_id_date("missing")
