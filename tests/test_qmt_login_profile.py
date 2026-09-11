"""Only static vendor resources and synthetic pixels; never capture a UI."""

from hashlib import sha256
from pathlib import Path
import struct
import zlib

import pytest

from integrations import qmt_login_profile as profile


def _canvas(width=624, height=443, color=(235, 242, 255)):
    r, g, b = color
    return bytearray(bytes((b, g, r, 0)) * width * height)


def _fill(canvas, rect, color, width=624):
    r, g, b = color
    row = bytes((b, g, r, 0)) * rect.width
    for y in range(rect.top, rect.bottom):
        offset = (y * width + rect.left) * 4
        canvas[offset:offset + len(row)] = row


def _frame(canvas, width=624, height=443):
    return profile.Frame(width, height, bytes(canvas))


@pytest.fixture
def login_pixels(monkeypatch):
    """Isolate layout checks using a synthetic, fully hashed vendor region.

    The actual fixed vendor digests are independently checked below against
    the installed static PNG. No actual login-window pixels are used here.
    """
    canvas = _canvas()
    _fill(canvas, profile.BANNER, (20, 50, 100))
    monkeypatch.setattr(
        profile, "VENDOR_BANNER_CORE_RGB_SHA256",
        sha256(bytes((20, 50, 100)) * 599 * 130).hexdigest(),
    )
    for rect in (profile.ACCOUNT, profile.PASSWORD):
        _fill(canvas, rect, (255, 255, 255))
    for rect in (profile.LOGIN_BUTTON, profile.OFFLINE_BUTTON):
        _fill(canvas, rect, (14, 122, 239))
    _fill(canvas, profile.COMBINED_MODE, (13, 107, 223))
    for rect in (profile.TRADE_MODE, profile.QUOTES_MODE):
        _fill(canvas, rect, (221, 221, 221))
    _fill(canvas, profile.INDEPENDENT_TRADE, (14, 122, 239))
    _fill(canvas, profile.Rect(362, 310, 372, 320), (235, 242, 255))
    return canvas


def _validate(canvas, **kwargs):
    return profile.validate_login_profile(
        _frame(canvas), executable_version=kwargs.get("executable_version", "2.1.19.0"),
        dpi=kwargs.get("dpi", 96),
    )


def test_known_geometry_and_no_pixel_repr(login_pixels):
    result = _validate(login_pixels)
    assert result.account == profile.Rect(192, 237, 432, 268)
    assert result.password == profile.Rect(192, 272, 432, 303)
    assert result.login_button == profile.Rect(192, 331, 288, 362)
    assert profile.COMBINED_MODE == profile.Rect(192, 205, 271, 232)
    assert profile.SELECTED_MODE_RGB == (13, 107, 223)
    assert "bgra" not in repr(_frame(login_pixels))
    assert _frame(login_pixels).rgb(192, 237) == (255, 255, 255)


@pytest.mark.parametrize("rect,color,code", [
    (profile.Rect(50, 60, 51, 61), (0, 0, 0), "BANNER_INVALID"),
    (profile.Rect(192, 245, 193, 246), (200, 200, 200), "INPUT_INVALID"),
    (profile.Rect(193, 250, 194, 251), (0, 0, 0), "INPUT_INVALID"),
    (profile.Rect(191, 245, 192, 246), (255, 255, 255), "LAYOUT_INVALID"),
    (profile.Rect(194, 333, 195, 334), (221, 221, 221), "BUTTON_INVALID"),
    (profile.Rect(339, 333, 340, 334), (221, 221, 221), "BUTTON_INVALID"),
    (profile.COMBINED_MODE, (221, 221, 221), "MODE_INVALID"),
    (profile.COMBINED_MODE, (14, 122, 239), "MODE_INVALID"),
    (profile.TRADE_MODE, (14, 122, 239), "MODE_INVALID"),
    (profile.QUOTES_MODE, (14, 122, 239), "MODE_INVALID"),
    (profile.Rect(365, 314, 366, 315), (0, 0, 0), "CHECKBOX_INVALID"),
    (profile.Rect(450, 280, 550, 310), (255, 255, 255), "EXTRA_INPUT"),
    (profile.Rect(455, 305, 487, 317), (255, 255, 255), "EXTRA_INPUT"),
    (profile.Rect(450, 280, 451, 281), (0, 0, 0), "LAYOUT_INVALID"),
    (profile.Rect(20, 280, 21, 281), (14, 122, 239), "LAYOUT_INVALID"),
    (profile.Rect(300, 340, 301, 341), (0, 0, 0), "LAYOUT_INVALID"),
])
def test_structural_changes_fail_closed(login_pixels, rect, color, code):
    _fill(login_pixels, rect, color)
    with pytest.raises(ValueError, match="^QMT_PROFILE_" + code + "$"):
        _validate(login_pixels)


def test_value_and_icon_regions_are_masked(login_pixels):
    for rect in (profile.Rect(196, 241, 408, 263), profile.Rect(196, 276, 408, 298)):
        _fill(login_pixels, rect, (0, 0, 0))
    _fill(login_pixels, profile.Rect(408, 246, 427, 260), (14, 122, 239))
    _fill(login_pixels, profile.Rect(408, 281, 425, 295), (14, 122, 239))
    assert _validate(login_pixels) == profile.PROFILE


def test_both_documented_input_border_styles(login_pixels):
    for rect in (profile.ACCOUNT, profile.PASSWORD):
        _fill(login_pixels, rect, (14, 122, 239))
        _fill(login_pixels, profile.Rect(rect.left + 1, rect.top + 1, rect.right - 1, rect.bottom - 1), (255, 255, 255))
    assert _validate(login_pixels) == profile.PROFILE


@pytest.mark.parametrize("kwargs,code", [
    ({"dpi": 120}, "DPI_INVALID"), ({"dpi": True}, "DPI_INVALID"),
    ({"executable_version": "2.1.19.1"}, "VERSION_INVALID"),
])
def test_version_and_dpi_are_exact(login_pixels, kwargs, code):
    with pytest.raises(ValueError, match="^QMT_PROFILE_" + code + "$"):
        _validate(login_pixels, **kwargs)


@pytest.mark.parametrize("width,height,data", [
    (0, 1, b""), (True, 1, bytes(4)), (1, 1, bytes(3)),
    (1, 1, bytearray(4)), (4097, 1, b""), (4096, 4096, b""),
])
def test_bad_frames_do_not_echo_contents(width, height, data):
    with pytest.raises(ValueError, match="^QMT_PROFILE_FRAME_INVALID$"):
        profile.Frame(width, height, data)


def test_wrong_window_size_and_pixel_coordinates():
    frame = profile.Frame(1, 1, b"\x10\x20\x30\x00")
    assert frame.rgb(0, 0) == (48, 32, 16)
    for xy in [(-1, 0), (1, 0), (False, 0)]:
        with pytest.raises(ValueError, match="^QMT_PROFILE_PIXEL_INVALID$"):
            frame.rgb(*xy)
    with pytest.raises(ValueError, match="^QMT_PROFILE_SIZE_INVALID$"):
        profile.validate_login_profile(frame, executable_version="2.1.19.0", dpi=96)


def _caret_frames(rect=profile.Rect(20, 20, 21, 32), states=(0, 1, 0, 1, 0), color=(0, 0, 0)):
    frames = []
    for state in states:
        canvas = _canvas(80, 60, (255, 255, 255))
        if state:
            _fill(canvas, rect, color, 80)
        frames.append(_frame(canvas, 80, 60))
    return frames


CARET_ROI = profile.Rect(10, 10, 70, 50)


@pytest.mark.parametrize("width,height", [(1, 8), (1, 12), (2, 24)])
def test_two_complete_blink_cycles(width, height):
    caret = profile.Rect(20, 20, 20 + width, 20 + height)
    # 35 frames at 80 ms, each state lasting approximately 480 ms.
    frames = _caret_frames(caret, tuple((index // 6) % 2 for index in range(35)))
    assert profile.blink_caret(frames, CARET_ROI) == caret
    assert profile.blink_caret(list(reversed(frames)), CARET_ROI) == caret


@pytest.mark.parametrize("states,code", [
    ((0, 0, 0, 0, 0), "NOT_FOUND"),
    ((1, 1, 1, 1, 1), "NOT_FOUND"),
    ((0, 0, 1, 1, 0), "CYCLES_INSUFFICIENT"),
    ((0, 1, 0, 1, 1), "CYCLES_INSUFFICIENT"),
])
def test_absent_focus_and_incomplete_cycles(states, code):
    with pytest.raises(ValueError, match="^QMT_CARET_" + code + "$"):
        profile.blink_caret(_caret_frames(states=states), CARET_ROI)


@pytest.mark.parametrize("rect", [
    profile.Rect(20, 20, 23, 32), profile.Rect(20, 20, 21, 27),
    profile.Rect(20, 20, 21, 45), profile.Rect(20, 20, 32, 21),
])
def test_non_caret_animation_rejected(rect):
    with pytest.raises(ValueError, match="^QMT_CARET_SHAPE_INVALID$"):
        profile.blink_caret(_caret_frames(rect), CARET_ROI)


def test_second_caret_motion_noise_and_third_state_rejected():
    for change in ("second", "moving", "hole", "noise", "third"):
        frames = _caret_frames()
        canvas = bytearray(frames[3].bgra)
        if change == "second":
            _fill(canvas, profile.Rect(30, 20, 31, 32), (0, 0, 0), 80)
        elif change == "moving":
            _fill(canvas, profile.Rect(20, 20, 21, 32), (255, 255, 255), 80)
            _fill(canvas, profile.Rect(21, 20, 22, 32), (0, 0, 0), 80)
        elif change == "hole":
            _fill(canvas, profile.Rect(20, 25, 21, 26), (255, 255, 255), 80)
        elif change == "noise":
            _fill(canvas, profile.Rect(15, 15, 16, 16), (254, 255, 255), 80)
        else:
            _fill(canvas, profile.Rect(20, 20, 21, 32), (80, 80, 80), 80)
        frames[3] = _frame(canvas, 80, 60)
        with pytest.raises(ValueError, match="^QMT_CARET_(UNSTABLE|SHAPE_INVALID)$"):
            profile.blink_caret(frames, CARET_ROI)


def test_alpha_and_pixels_outside_roi_do_not_establish_focus():
    frames = _caret_frames()
    canvas = bytearray(frames[2].bgra)
    canvas[3::4] = bytes([255]) * (80 * 60)
    _fill(canvas, profile.Rect(0, 0, 5, 5), (0, 0, 0), 80)
    frames[2] = _frame(canvas, 80, 60)
    assert profile.blink_caret(frames, CARET_ROI) == profile.Rect(20, 20, 21, 32)
    with pytest.raises(ValueError, match="^QMT_CARET_UNSTABLE$"):
        profile.blink_caret(_caret_frames(color=(240, 240, 240)), CARET_ROI)


@pytest.mark.parametrize("which", ["short", "long", "dimensions", "outside", "budget", "generator"])
def test_bounded_caret_inputs(which):
    frames, roi = _caret_frames(), CARET_ROI
    if which == "short":
        frames = frames[:4]
    elif which == "long":
        frames = frames * 13
    elif which == "dimensions":
        frames[2] = profile.Frame(1, 1, bytes(4))
    elif which == "outside":
        roi = profile.Rect(10, 10, 81, 50)
    elif which == "budget":
        roi = profile.Rect(0, 0, 2000, 2000)
    else:
        frames = iter(frames)
    with pytest.raises(ValueError, match="^QMT_CARET_SEQUENCE_INVALID$"):
        profile.blink_caret(frames, roi)


def _static_png_rgb(data):
    """Small test-only decoder for the vendor's non-interlaced 8-bit PNGs."""
    assert data[:8] == b"\x89PNG\r\n\x1a\n"
    cursor, compressed, palette = 8, bytearray(), b""
    while cursor < len(data):
        length = struct.unpack_from(">I", data, cursor)[0]
        name = data[cursor + 4:cursor + 8]
        chunk = data[cursor + 8:cursor + 8 + length]
        if name == b"IHDR":
            width, height, depth, kind, compression, filtering, interlace = struct.unpack(">IIBBBBB", chunk)
            assert depth == 8 and kind in (2, 3, 6)
            assert (compression, filtering, interlace) == (0, 0, 0)
        elif name == b"IDAT":
            compressed.extend(chunk)
        elif name == b"PLTE":
            palette = chunk
        cursor += length + 12
    channels = {2: 3, 3: 1, 6: 4}[kind]
    stride = width * channels
    raw, previous, output = zlib.decompress(compressed), bytearray(stride), bytearray()
    assert len(raw) == height * (stride + 1)
    for y in range(height):
        mode = raw[y * (stride + 1)]
        row = bytearray(raw[y * (stride + 1) + 1:(y + 1) * (stride + 1)])
        for x in range(stride):
            a, b, c = (row[x - channels] if x >= channels else 0), previous[x], (previous[x - channels] if x >= channels else 0)
            p = a + b - c
            pa, pb, pc = abs(p - a), abs(p - b), abs(p - c)
            predictor = a if pa <= pb and pa <= pc else b if pb <= pc else c
            addition = (0, a, b, (a + b) // 2, predictor)[mode]
            row[x] = (row[x] + addition) & 255
        for x in range(width):
            output.extend(palette[row[x] * 3:row[x] * 3 + 3] if kind == 3 else row[x * channels:x * channels + 3])
        previous = row
    return width, height, bytes(output)


def test_installed_static_vendor_asset_constants():
    root = Path("D:/国金证券QMT交易端/resource")
    path = root / "tc_img_login_deploy.png"
    if not path.is_file():
        pytest.skip("Static vendor installation is not present; no UI is accessed")
    raw = path.read_bytes()
    profile.validate_vendor_png(raw)
    width, height, rgb = _static_png_rgb(raw)
    assert (width, height) == (599, 185)
    assert sha256(rgb).hexdigest() == profile.VENDOR_BANNER_RGB_SHA256
    assert sha256(rgb[32 * 599 * 3:162 * 599 * 3]).hexdigest() == profile.VENDOR_BANNER_CORE_RGB_SHA256
    allowed = set()
    for name, indices in (("check", (0, 1, 3)), ("uncheck", (2,))):
        width, height, rgb = _static_png_rgb((root / "theme/new" / f"tc_img_login_checkbox_{name}.png").read_bytes())
        assert (width, height) == (48, 12)
        for index in range(4):
            tile = b"".join(rgb[(y * 48 + index * 12) * 3:(y * 48 + index * 12 + 12) * 3] for y in range(12))
            digest = sha256(tile).hexdigest()
            if index in indices:
                allowed.add(digest)
            else:
                assert digest not in profile.UNCHECKED_RGB_SHA256
    assert allowed == profile.UNCHECKED_RGB_SHA256


def test_vendor_file_mutations_and_empty_checkbox_are_exact():
    for value in (b"", b"x" * 140005, bytearray(140005)):
        with pytest.raises(ValueError, match="^QMT_PROFILE_VENDOR_INVALID$"):
            profile.validate_vendor_png(value)
    canvas = _canvas(12, 12, (14, 122, 239))
    _fill(canvas, profile.Rect(1, 1, 11, 11), (235, 242, 255), 12)
    assert profile._rgb_digest(_frame(canvas, 12, 12), profile.Rect(0, 0, 12, 12)) in profile.UNCHECKED_RGB_SHA256
