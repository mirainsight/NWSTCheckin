"""Deterministic UI accent palette from a calendar date string (``YYYY-MM-DD``).

CHECK IN and NWST Health use **today's MYT date** as the seed when Theme Override does not
supply a primary color, so the theme refreshes each calendar day (not once per week)."""

from __future__ import annotations

import colorsys
import hashlib
import random


def normalize_primary_hex(hex_str: str | None) -> str | None:
    h = (hex_str or "").strip()
    if not h:
        return None
    if not h.startswith("#"):
        h = "#" + h
    if len(h) != 7:
        return None
    try:
        int(h[1:], 16)
    except ValueError:
        return None
    return h.lower()


def theme_from_primary_hex(primary_hex: str) -> dict[str, str]:
    """Build the same shape as ``generate_colors_for_date`` from a fixed primary."""
    p = normalize_primary_hex(primary_hex)
    if not p:
        raise ValueError("Invalid primary hex")
    r = int(p[1:3], 16) / 255.0
    g = int(p[3:5], 16) / 255.0
    b = int(p[5:7], 16) / 255.0
    h, light, sat = colorsys.rgb_to_hls(r, g, b)
    rgb_light = colorsys.hls_to_rgb(h, min(light + 0.2, 0.9), sat)
    light_color = "#{:02x}{:02x}{:02x}".format(
        int(rgb_light[0] * 255),
        int(rgb_light[1] * 255),
        int(rgb_light[2] * 255),
    )
    return {
        "primary": p,
        "light": light_color,
        "background": "#000000",
        "accent": p,
    }


def generate_colors_for_date(date_str: str) -> dict[str, str]:
    """Palette deterministic for ``date_str`` (consistent for that calendar day).

    Args:
        date_str: ``YYYY-MM-DD``

    Returns:
        ``primary``, ``light``, ``background``, ``accent`` hex strings.
    """
    seed = int(hashlib.md5(date_str.encode()).hexdigest(), 16)
    rng = random.Random(seed)
    hue = rng.random()
    saturation = rng.uniform(0.7, 1.0)
    lightness = rng.uniform(0.45, 0.65)

    rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
    primary_color = "#{:02x}{:02x}{:02x}".format(
        int(rgb[0] * 255),
        int(rgb[1] * 255),
        int(rgb[2] * 255),
    )

    rgb_light = colorsys.hls_to_rgb(hue, min(lightness + 0.2, 0.9), saturation)
    light_color = "#{:02x}{:02x}{:02x}".format(
        int(rgb_light[0] * 255),
        int(rgb_light[1] * 255),
        int(rgb_light[2] * 255),
    )

    return {
        "primary": primary_color,
        "light": light_color,
        "background": "#000000",
        "accent": primary_color,
    }
