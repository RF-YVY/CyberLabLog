"""Create exact-size label PDFs as normal local HTTP files, without browser blobs."""
import base64
from io import BytesIO
import math
from pathlib import Path
import time
import uuid

from PIL import Image
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen.canvas import Canvas

from paths import data_dir


def preview_path(token: str) -> Path:
    if len(token) != 32 or any(char not in "0123456789abcdef" for char in token):
        raise ValueError("Invalid label preview identifier")
    return data_dir() / "label_print_previews" / f"{token}.pdf"


def create_print_pdf(images: list[str], width: float, height: float) -> str:
    if not all(math.isfinite(value) and 1 <= value <= 12 for value in (width, height)):
        raise ValueError("Label dimensions must be between 1 and 12 inches")
    if not 1 <= len(images) <= 500:
        raise ValueError("Choose between 1 and 500 labels")
    if sum(len(image) for image in images) > 80 * 1024 * 1024:
        raise ValueError("Label images are too large; print a smaller batch")
    output = BytesIO()
    page = (width * 72, height * 72)
    pdf = Canvas(output, pagesize=page, pageCompression=1)
    pdf.setTitle("Evidence Labels")
    for image in images:
        if not image.startswith("data:image/png;base64,") or len(image) > 8 * 1024 * 1024:
            raise ValueError("Label images must be PNG data")
        try:
            raw = base64.b64decode(image.split(",", 1)[1], validate=True)
            with Image.open(BytesIO(raw)) as source:
                if source.format != "PNG" or source.width * source.height > 16000000:
                    raise ValueError("Invalid label image size")
                source.load()
                rgb = source.convert("RGBA")
                white = Image.new("RGB", rgb.size, "white")
                white.paste(rgb, mask=rgb.getchannel("A"))
                pdf.drawImage(ImageReader(white), 0, 0, width=page[0], height=page[1])
            pdf.showPage()
        except (OSError, ValueError) as exc:
            raise ValueError("Could not read a label image") from exc
    pdf.save()
    token = uuid.uuid4().hex
    path = preview_path(token)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(output.getvalue())
    # Only remove old generated PDFs from this dedicated preview directory.
    cutoff = time.time() - 86400
    for old in path.parent.glob("*.pdf"):
        try:
            if old.stat().st_mtime < cutoff:
                old.unlink()
        except OSError:
            pass
    return token
