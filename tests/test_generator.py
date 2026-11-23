"""Tests for the synthetic generator."""

from neural_lakehouse.generator.sessions import DEFECTS, generate_uploads


def test_same_seed_gives_same_uploads():
    a = generate_uploads(seed=7)
    b = generate_uploads(seed=7)
    assert [u.metadata for u in a] == [u.metadata for u in b]


def test_number_of_bad_uploads_and_all_defect_types_used():
    uploads = generate_uploads(n=60, bad_ratio=0.15)
    defects = [u.defect for u in uploads if u.defect]
    assert len(defects) == 9  # round(60 * 0.15)
    assert set(defects) == set(DEFECTS)


def test_duplicate_repeats_an_earlier_session_id():
    uploads = generate_uploads()
    position = next(i for i, u in enumerate(uploads) if u.defect == "duplicate")
    earlier_ids = {u.metadata["session_id"] for u in uploads[:position]}
    assert uploads[position].metadata["session_id"] in earlier_ids
