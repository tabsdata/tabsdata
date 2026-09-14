#
# Copyright 2026 Tabsdata Inc.
#

"""EBCDIC fixed-length record fixtures shared by the connector itest suites.

Every encoding is built from its mainframe definition rather than round-tripped
through one of our own encoders, so a test proves the reader agrees with the
format and not merely with itself.
"""

from __future__ import annotations


def copybook(*lines: str) -> str:
    """A copybook in fixed-format COBOL: columns 1-6 are sequence numbers and
    the code area starts at column 8, which is what the parser expects."""
    return "".join(f"       {line}\n" for line in lines)


def ebc(text: str, width: int | None = None) -> bytes:
    """`text` as EBCDIC, space-padded to `width`."""
    return (text if width is None else text.ljust(width)).encode("cp037")


def zoned(value: int, digits: int, signed: bool = True) -> bytes:
    """`USAGE DISPLAY`: one byte per digit, the sign overpunched into the high
    nibble of the last (`C` positive, `D` negative)."""
    body = bytearray(f"{abs(value):0{digits}d}".encode("cp037"))
    if signed:
        body[-1] = (body[-1] & 0x0F) | (0xD0 if value < 0 else 0xC0)
    return bytes(body)


def zoned_leading_sign(value: int, digits: int) -> bytes:
    """`SIGN LEADING SEPARATE`: the sign is its own byte in front."""
    return ebc(("-" if value < 0 else "+") + f"{abs(value):0{digits}d}")


def packed(value: int, digits: int) -> bytes:
    """`COMP-3`: two digits per byte, the final low nibble the sign."""
    text = f"{abs(value):0{digits}d}"
    if len(text) % 2 == 0:
        text = "0" + text
    nibbles = [int(c) for c in text] + [0x0D if value < 0 else 0x0C]
    return bytes((nibbles[i] << 4) | nibbles[i + 1] for i in range(0, len(nibbles), 2))


def binary(value: int, digits: int) -> bytes:
    """`COMP`: big-endian two's complement, width from the declared digits."""
    width = 2 if digits <= 4 else 4 if digits <= 9 else 8
    return value.to_bytes(width, "big", signed=True)


def hfp(value: float, width: int) -> bytes:
    """IBM hex float: sign, 7-bit excess-64 base-16 exponent, then a fraction
    normalised into [1/16, 1)."""
    fraction_bits = 24 if width == 4 else 56
    if value == 0:
        return bytes(width)
    magnitude, exponent = abs(value), 0
    while magnitude >= 1.0:
        magnitude /= 16.0
        exponent += 1
    while magnitude < 1.0 / 16.0:
        magnitude *= 16.0
        exponent -= 1
    raw = ((0x80 if value < 0 else 0) | (exponent + 64)) << fraction_bits
    raw |= round(magnitude * (1 << fraction_bits))
    return raw.to_bytes(width, "big")


def write(path, records: list[bytes]):
    """Write `records` back to back. They must be one fixed length -- a file
    that is not a whole number of records is exactly what the reader refuses."""
    lengths = {len(r) for r in records}
    assert len(lengths) == 1, f"records must be one fixed length, got {lengths}"
    path.write_bytes(b"".join(records))
    return path


# A two-field layout and its matching record, which is all most cases need.
CUSTOMER = copybook(
    "01  CUSTOMER.",
    "    05  NAME  PIC X(6).",
    "    05  COUNT PIC 9(4).",
)


def customer(name: str, count: int) -> bytes:
    return ebc(name, 6) + zoned(count, 4, signed=False)
