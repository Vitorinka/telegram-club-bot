import os
import re
import unicodedata
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ASSET_ROOT = Path(__file__).resolve().parent / "assets"
CERTIFICATE_TEMPLATE_PATHS = {
    "gift_1m": ASSET_ROOT / "certificates" / "gift_1_month.png",
    "gift_6m": ASSET_ROOT / "certificates" / "gift_6_months.png",
    "gift_12m": ASSET_ROOT / "certificates" / "gift_12_months.png",
}
CERTIFICATE_FONT_PATH = ASSET_ROOT / "fonts" / "EBGaramond-VariableFont_wght.ttf"
CERTIFICATE_NAME_BOX = (300, 340, 1236, 610)
CERTIFICATE_NAME_COLOR = (0, 119, 143)
CERTIFICATE_NAME_MAX_LENGTH = 50
CERTIFICATE_NAME_FONT_SIZE = 112
CERTIFICATE_NAME_MIN_FONT_SIZE = 52
CERTIFICATE_NAME_TOO_LONG_TEXT = (
    "Подпись слишком длинная для сертификата. "
    "Попробуйте указать имя или более короткий вариант имени и фамилии."
)


class CertificateNameError(ValueError):
    pass


def certificate_template_path(tariff_code):
    path = CERTIFICATE_TEMPLATE_PATHS.get(tariff_code)
    if path is None:
        raise ValueError("unknown_gift_certificate_tariff")
    return path


def normalize_certificate_name(value):
    if value is None:
        raise CertificateNameError("empty_certificate_name")
    value = str(value)
    if any(char in "\r\n" or unicodedata.category(char).startswith("C") for char in value):
        raise CertificateNameError("invalid_certificate_name")
    normalized = re.sub(r"\s+", " ", value.strip())
    if not normalized:
        raise CertificateNameError("empty_certificate_name")
    if len(normalized) > CERTIFICATE_NAME_MAX_LENGTH:
        raise CertificateNameError("certificate_name_too_long")
    if not any(char.isalpha() for char in normalized):
        raise CertificateNameError("invalid_certificate_name")
    if not all(char.isalpha() or char in " -" for char in normalized):
        raise CertificateNameError("invalid_certificate_name")
    return normalized


def _font_for_name(name):
    left, top, right, bottom = CERTIFICATE_NAME_BOX
    max_width = right - left
    max_height = bottom - top
    for size in range(CERTIFICATE_NAME_FONT_SIZE, CERTIFICATE_NAME_MIN_FONT_SIZE - 1, -2):
        font = ImageFont.truetype(str(CERTIFICATE_FONT_PATH), size=size)
        box = font.getbbox(name)
        if box[2] - box[0] <= max_width and box[3] - box[1] <= max_height:
            return font
    raise CertificateNameError("certificate_name_does_not_fit")


def validate_certificate_name_fits(value):
    normalized = normalize_certificate_name(value)
    _font_for_name(normalized)
    return normalized


def certificate_assets_status():
    available = [path for path in CERTIFICATE_TEMPLATE_PATHS.values() if path.is_file()]
    return {
        "template_count": len(available),
        "required_template_count": len(CERTIFICATE_TEMPLATE_PATHS),
        "font_available": CERTIFICATE_FONT_PATH.is_file(),
    }


def render_gift_certificate(tariff_code, certificate_name, output_path):
    template_path = certificate_template_path(tariff_code)
    normalized = normalize_certificate_name(certificate_name) if certificate_name else None
    with Image.open(template_path) as source:
        image = source.convert("RGB")
    if normalized:
        font = _font_for_name(normalized)
        draw = ImageDraw.Draw(image)
        left, top, right, bottom = CERTIFICATE_NAME_BOX
        draw.text(
            ((left + right) / 2, (top + bottom) / 2),
            normalized,
            font=font,
            fill=CERTIFICATE_NAME_COLOR,
            anchor="mm",
            align="center",
        )
    output_path = Path(output_path)
    image.save(output_path, format="PNG", optimize=True)
    return output_path


def remove_generated_certificate(path):
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
