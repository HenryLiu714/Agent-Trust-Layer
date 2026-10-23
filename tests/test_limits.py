"""The relationships between irimi's size bounds, which `irimi.core.limits` states one line each."""

from irimi.core import limits


def test_a_body_is_decoded_as_far_as_it_is_redacted_and_stored_no_larger():
    assert limits.MAX_DECODED_BODY == limits.MAX_REDACTED_BODY
    assert limits.MAX_REDACTED_BODY >= limits.MAX_STORED_BODY >= limits.MAX_BODY_BYTES


def test_the_store_queue_holds_more_than_one_stored_body():
    assert limits.MAX_QUEUED_BYTES > limits.MAX_STORED_BODY
    assert limits.MAX_QUEUED_BYTES > limits.MAX_CONTROL_BODY


def test_a_captured_trigger_fits_a_control_post_with_room_for_the_rest():
    assert limits.MAX_CAPTURED_BYTES + limits.MAX_NAME < limits.MAX_CONTROL_BODY


def test_the_sdk_reads_any_answer_irimi_can_send():
    assert limits.MAX_ANSWER > limits.MAX_CAPTURED_BYTES


def test_a_posted_error_fits_a_control_post():
    assert limits.MAX_POSTED_MESSAGE + limits.MAX_POSTED_TYPE < limits.MAX_CONTROL_BODY


def test_posted_and_queued_errors_hold_more_than_a_stored_label():
    assert limits.MAX_POSTED_MESSAGE > limits.MAX_ERROR_MESSAGE
    assert limits.MAX_POSTED_TYPE > limits.MAX_ERROR_TYPE
    assert limits.MAX_QUEUED_ERROR > limits.MAX_ERROR_MESSAGE


def test_every_size_is_a_whole_number_of_kib():
    sizes = (
        limits.MAX_DECODED_BODY,
        limits.MAX_REDACTED_BODY,
        limits.MAX_STORED_BODY,
        limits.MAX_QUEUED_BYTES,
        limits.MAX_BODY_BYTES,
        limits.MAX_CONTROL_BODY,
        limits.MAX_CAPTURED_BYTES,
        limits.MAX_ANSWER,
    )
    assert all(size % limits.KiB == 0 for size in sizes)
