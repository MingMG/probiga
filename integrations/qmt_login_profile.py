"""Pure image checks for the fixed QMT 2.1.19.0, 96-DPI login form.

The constants below come from the installed vendor resources and the observed
window geometry. Acceptance against a naturally occurring login window is still
required; a synthetic test is not evidence that a live window matches. This
module neither captures images nor reads, recognizes, or emits control values.
Process ownership, foreground/focus ownership and capture timing belong to the
Windows driver. A matching image alone does not establish those facts.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256


@dataclass(frozen=True, repr=False)
class Frame:
    width: int
    height: int
    bgra: bytes

    def __post_init__(self) -> None:
        if (
            type(self.width) is not int
            or type(self.height) is not int
            or not 1 <= self.width <= 4096
            or not 1 <= self.height <= 4096
            or self.width * self.height > 8_388_608
            or type(self.bgra) is not bytes
            or len(self.bgra) != self.width * self.height * 4
        ):
            raise ValueError("QMT_PROFILE_FRAME_INVALID")

    def rgb(self, x: int, y: int) -> tuple[int, int, int]:
        if (
            type(x) is not int or type(y) is not int
            or not 0 <= x < self.width or not 0 <= y < self.height
        ):
            raise ValueError("QMT_PROFILE_PIXEL_INVALID")
        offset = (y * self.width + x) * 4
        return self.bgra[offset + 2], self.bgra[offset + 1], self.bgra[offset]


@dataclass(frozen=True)
class Rect:
    """Pixel rectangle with exclusive right and bottom coordinates."""

    left: int
    top: int
    right: int
    bottom: int

    def __post_init__(self) -> None:
        if (
            any(type(value) is not int for value in (
                self.left, self.top, self.right, self.bottom
            ))
            or self.left < 0 or self.top < 0
            or self.right <= self.left or self.bottom <= self.top
        ):
            raise ValueError("QMT_PROFILE_RECT_INVALID")

    @property
    def width(self) -> int:
        return self.right - self.left

    @property
    def height(self) -> int:
        return self.bottom - self.top

    def contains(self, x: int, y: int) -> bool:
        return self.left <= x < self.right and self.top <= y < self.bottom


@dataclass(frozen=True)
class LoginProfile:
    account: Rect
    password: Rect
    login_button: Rect


ACCOUNT = Rect(192, 237, 432, 268)
PASSWORD = Rect(192, 272, 432, 303)
LOGIN_BUTTON = Rect(192, 331, 288, 362)
OFFLINE_BUTTON = Rect(337, 331, 432, 362)
BANNER = Rect(13, 13, 612, 198)
# The title-bar close control overlays the top of the resource. Compare the
# entire stable center rather than sparse pixels or the overlaid title area.
BANNER_CORE = Rect(13, 45, 612, 175)
COMBINED_MODE = Rect(192, 205, 271, 232)
TRADE_MODE = Rect(271, 205, 351, 232)
QUOTES_MODE = Rect(351, 205, 431, 232)
INDEPENDENT_TRADE = Rect(361, 309, 373, 321)
PROFILE = LoginProfile(ACCOUNT, PASSWORD, LOGIN_BUTTON)

# resource/tc_img_login_deploy.png: exact file and decoded RGB pixel digests.
VENDOR_PNG_SHA256 = "872dc92861f096d12ece99383426c8c74a61c0e4702ccc3aafa927a8822fe8c0"
VENDOR_BANNER_RGB_SHA256 = "7c53a985c85e4dba77fbe37381c80b316de16c1b954dab81c2e75df53b9fea6e"
VENDOR_BANNER_CORE_RGB_SHA256 = "2f4d2f5f338f2970a3f96b6f0ac789915a5490347f1c25b8bbcc2da73b807b11"
# Visually empty 12 x 12 square states from both vendor checkbox sprites.
# Sprite filenames are misleading: several "uncheck" tiles contain a tick.
UNCHECKED_RGB_SHA256 = frozenset({
    "28ee742817ae451e4471159aa0f7e8073224feae211d80f69f4a29411c8be0cc",
    "040cd0ac5e8cdfc425da11cdd53e18d4e01a0fe48018c3fd6d59497ac9478fe0",
    "7d3181f95e095b017c338542d35a0fdd4b8fd751518289a0b127710034ccb5e9",
})
# Opaque center of theme/new/tc_img_login_background.png (54 x 54 nine-slice).
BACKGROUND_RGB = (235, 242, 255)
WHITE_RGB = (255, 255, 255)
INPUT_BORDER_RGB = (14, 122, 239)
BUTTON_RGB = frozenset({(14, 122, 239), (48, 147, 252), (13, 107, 223)})
# The redacted observed layout shows the combined-mode tab in its pressed
# state. Use the vendor's exact clr_login_btn_press, never JPEG sample RGB.
SELECTED_MODE_RGB = (13, 107, 223)
DISABLED_RGB = (221, 221, 221)


def validate_vendor_png(png: bytes) -> None:
    """Check the static vendor asset without parsing untrusted PNG contents."""
    if (
        type(png) is not bytes or len(png) != 140005
        or sha256(png).hexdigest() != VENDOR_PNG_SHA256
    ):
        raise ValueError("QMT_PROFILE_VENDOR_INVALID")


def _rgb_digest(frame: Frame, rect: Rect) -> str:
    digest = sha256()
    for y in range(rect.top, rect.bottom):
        start = (y * frame.width + rect.left) * 4
        row = frame.bgra[start:start + rect.width * 4]
        rgb = bytearray(rect.width * 3)
        rgb[0::3], rgb[1::3], rgb[2::3] = row[2::4], row[1::4], row[0::4]
        digest.update(rgb)
    return digest.hexdigest()


def _perimeter(rect: Rect):
    for x in range(rect.left, rect.right):
        yield x, rect.top
        yield x, rect.bottom - 1
    for y in range(rect.top + 1, rect.bottom - 1):
        yield rect.left, y
        yield rect.right - 1, y


def _solid_perimeter(frame: Frame, rect: Rect, color: tuple[int, int, int]) -> bool:
    return all(frame.rgb(x, y) == color for x, y in _perimeter(rect))


def _check_input(frame: Frame, rect: Rect) -> None:
    # Login-specific styles permit a white border; the generic style uses blue.
    # Neither text nor the account drop-down / password keyboard icon is read.
    edge = frame.rgb(rect.left, rect.top)
    if edge not in (WHITE_RGB, INPUT_BORDER_RGB) or not _solid_perimeter(frame, rect, edge):
        raise ValueError("QMT_PROFILE_INPUT_INVALID")
    inset = Rect(rect.left + 1, rect.top + 1, rect.right - 1, rect.bottom - 1)
    if not _solid_perimeter(frame, inset, WHITE_RGB):
        raise ValueError("QMT_PROFILE_INPUT_INVALID")


def _check_guard(frame: Frame, rect: Rect) -> None:
    for margin in (1, 2):
        expanded = Rect(
            rect.left - margin, rect.top - margin,
            rect.right + margin, rect.bottom + margin,
        )
        if not _solid_perimeter(frame, expanded, BACKGROUND_RGB):
            raise ValueError("QMT_PROFILE_LAYOUT_INVALID")


def _check_extra_input(frame: Frame) -> None:
    # Find any additional 32 x 12 white area in the form body. Labels and small
    # checkboxes are allowed; an extra account, password or challenge field is
    # not. Existing value regions are masked before this structural scan.
    heights = [0] * 599
    for y in range(BANNER.bottom, 431):
        run = 0
        for offset, x in enumerate(range(13, 612)):
            masked = any(rect.contains(x, y) for rect in (ACCOUNT, PASSWORD, LOGIN_BUTTON, OFFLINE_BUTTON))
            heights[offset] = (
                heights[offset] + 1
                if not masked and frame.rgb(x, y) == WHITE_RGB else 0
            )
            run = run + 1 if heights[offset] >= 12 else 0
            if run >= 32:
                raise ValueError("QMT_PROFILE_EXTRA_INPUT")


def _check_blank_layout(frame: Frame) -> None:
    # Fixed spaces outside the form, its left labels and the bottom link row.
    # Treat a new prompt, rectangle or overlay here as an unknown login page.
    for rect in (
        Rect(13, 198, 120, 399),
        Rect(440, 198, 612, 399),
        Rect(290, 331, 335, 362),
    ):
        for y in range(rect.top, rect.bottom):
            for x in range(rect.left, rect.right):
                if frame.rgb(x, y) != BACKGROUND_RGB:
                    raise ValueError("QMT_PROFILE_LAYOUT_INVALID")


def _check_button(frame: Frame, rect: Rect) -> None:
    inset = Rect(rect.left + 2, rect.top + 2, rect.right - 2, rect.bottom - 2)
    color = frame.rgb(inset.left, inset.top)
    if color not in BUTTON_RGB or not _solid_perimeter(frame, inset, color):
        raise ValueError("QMT_PROFILE_BUTTON_INVALID")


def _check_mode(frame: Frame) -> None:
    for rect, expected in (
        (COMBINED_MODE, SELECTED_MODE_RGB),
        (TRADE_MODE, DISABLED_RGB),
        (QUOTES_MODE, DISABLED_RGB),
    ):
        inset = Rect(rect.left + 2, rect.top + 2, rect.right - 2, rect.bottom - 2)
        if not _solid_perimeter(frame, inset, expected):
            raise ValueError("QMT_PROFILE_MODE_INVALID")
    if _rgb_digest(frame, INDEPENDENT_TRADE) not in UNCHECKED_RGB_SHA256:
        raise ValueError("QMT_PROFILE_CHECKBOX_INVALID")


def validate_login_profile(
    frame: Frame, *, executable_version: str, dpi: int
) -> LoginProfile:
    """Validate structural pixels; fail closed with fixed, non-secret codes."""
    if type(frame) is not Frame or (frame.width, frame.height) != (624, 443):
        raise ValueError("QMT_PROFILE_SIZE_INVALID")
    if type(executable_version) is not str or executable_version != "2.1.19.0":
        raise ValueError("QMT_PROFILE_VERSION_INVALID")
    if type(dpi) is not int or dpi != 96:
        raise ValueError("QMT_PROFILE_DPI_INVALID")
    if _rgb_digest(frame, BANNER_CORE) != VENDOR_BANNER_CORE_RGB_SHA256:
        raise ValueError("QMT_PROFILE_BANNER_INVALID")
    for rect in (ACCOUNT, PASSWORD):
        _check_input(frame, rect)
        _check_guard(frame, rect)
    for rect in (LOGIN_BUTTON, OFFLINE_BUTTON):
        _check_button(frame, rect)
        _check_guard(frame, rect)
    _check_mode(frame)
    _check_extra_input(frame)
    _check_blank_layout(frame)
    return PROFILE


def blink_caret(frames: Sequence[Frame], roi: Rect) -> Rect:
    """Locate one solid blinking caret in a fixed ROI, ignoring DIB alpha.

    Require at least A-B-A-B-A (two complete cycles), exactly two RGB states,
    a 1-2 by 8-24 pixel solid vertical bar and no other changed ROI pixels.
    The driver must keep the same foreground window and sample every 80 ms;
    images alone cannot prove their capture time or operating-system focus.
    """
    if (
        not isinstance(frames, Sequence) or not 5 <= len(frames) <= 64
        or type(roi) is not Rect or roi.width * roi.height * len(frames) > 4_000_000
    ):
        raise ValueError("QMT_CARET_SEQUENCE_INVALID")
    first = frames[0]
    if type(first) is not Frame or roi.right > first.width or roi.bottom > first.height:
        raise ValueError("QMT_CARET_SEQUENCE_INVALID")
    if any(type(frame) is not Frame or (frame.width, frame.height) != (first.width, first.height) for frame in frames):
        raise ValueError("QMT_CARET_SEQUENCE_INVALID")
    changed: set[tuple[int, int]] = set()
    for frame in frames[1:]:
        for y in range(roi.top, roi.bottom):
            for x in range(roi.left, roi.right):
                before, after = first.rgb(x, y), frame.rgb(x, y)
                if before == after:
                    continue
                if sum(abs(a - b) for a, b in zip(before, after)) < 90:
                    raise ValueError("QMT_CARET_UNSTABLE")
                changed.add((x, y))
                if len(changed) > 48:
                    raise ValueError("QMT_CARET_SHAPE_INVALID")
    if not changed:
        raise ValueError("QMT_CARET_NOT_FOUND")
    left, right = min(x for x, _ in changed), max(x for x, _ in changed) + 1
    top, bottom = min(y for _, y in changed), max(y for _, y in changed) + 1
    caret = Rect(left, top, right, bottom)
    if not (1 <= caret.width <= 2 and 8 <= caret.height <= 24 and len(changed) == caret.width * caret.height):
        raise ValueError("QMT_CARET_SHAPE_INVALID")
    states: list[tuple[int, int, int]] = []
    transitions = 0
    previous = None
    for frame in frames:
        colors = {frame.rgb(x, y) for x, y in changed}
        if len(colors) != 1:
            raise ValueError("QMT_CARET_UNSTABLE")
        state = colors.pop()
        if state not in states:
            states.append(state)
            if len(states) > 2:
                raise ValueError("QMT_CARET_UNSTABLE")
        if previous is not None and state != previous:
            transitions += 1
        previous = state
    if len(states) != 2 or transitions < 4:
        raise ValueError("QMT_CARET_CYCLES_INSUFFICIENT")
    return caret
