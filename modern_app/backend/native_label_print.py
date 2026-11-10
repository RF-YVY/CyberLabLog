"""Print label images directly through Windows, using the label's feed geometry."""
import base64
from io import BytesIO
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from PIL import Image


def _powershell(script, job=None, timeout=30):
    if sys.platform != "win32":
        raise ValueError("Direct label printing is available on Windows; use Open Print PDF instead")
    executable = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    environment = os.environ.copy()
    if job is not None:
        environment["CYBERLAB_LABEL_JOB"] = str(job)
    prefix = "[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(); $ErrorActionPreference = 'Stop'; "
    command = base64.b64encode((prefix + script).encode("utf-16le")).decode("ascii")
    try:
        result = subprocess.run([str(executable), "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", command], env=environment, capture_output=True, encoding="utf-8", errors="replace", creationflags=subprocess.CREATE_NO_WINDOW, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise ValueError("The Windows printer did not respond. Check the printer and its queue before retrying.") from exc
    if result.returncode:
        raise ValueError("Windows printing failed: " + (result.stderr.strip() or "Check the printer and label stock")[-1500:])
    return json.loads(result.stdout.lstrip("\ufeff"))


def list_printers():
    if sys.platform != "win32":
        return {"printers": [], "recommended": "", "direct_print_available": False}
    result = _powershell(r"""
Add-Type -AssemblyName System.Drawing
$names = @([System.Drawing.Printing.PrinterSettings]::InstalledPrinters | Sort-Object)
$default = (New-Object System.Drawing.Printing.PrinterSettings).PrinterName
@{printers=$names; default=$default} | ConvertTo-Json -Compress
""")
    names = result["printers"]
    recommended = next((name for name in names if "dymo" in name.lower() and "550" in name), next((name for name in names if "dymo" in name.lower()), result.get("default", "")))
    return {"printers": names, "recommended": recommended, "direct_print_available": True}


def native_geometry(width, height, printer):
    if not all(math.isfinite(value) and 1 <= value <= 12 for value in [width, height]):
        raise ValueError("Label dimensions must be between 1 and 12 inches")
    rotate = "dymo" in printer.lower() and width > height
    across, feed = (height, width) if rotate else (width, height)
    return {"paper_width": round(across * 100), "paper_height": round(feed * 100), "rotate": rotate}


def print_images(images, width, height, printer):
    if not printer or not printer.strip():
        raise ValueError("Select an installed label printer, then try again")
    geometry = native_geometry(width, height, printer)
    if not 1 <= len(images) <= 500 or sum(len(image) for image in images) > 80 * 1024 * 1024:
        raise ValueError("Choose a smaller batch of labels")
    with tempfile.TemporaryDirectory(prefix="cyberlab-native-labels-") as directory:
        paths = []
        for index, data in enumerate(images):
            if not data.startswith("data:image/png;base64,") or len(data) > 8 * 1024 * 1024:
                raise ValueError("Label images must be PNG data")
            try:
                with Image.open(BytesIO(base64.b64decode(data.split(",", 1)[1], validate=True))) as image:
                    if image.format != "PNG" or image.width * image.height > 16000000:
                        raise ValueError("Invalid label image")
                    image.load()
                    label = image.convert("RGBA")
                    if geometry["rotate"]:
                        label = label.transpose(Image.Transpose.ROTATE_90)
                    white = Image.new("RGB", label.size, "white")
                    white.paste(label, mask=label.getchannel("A"))
                    path = Path(directory) / f"label-{index}.png"
                    white.save(path, dpi=(300, 300)); paths.append(str(path))
            except (OSError, ValueError) as exc:
                raise ValueError("Could not read a label image") from exc
        job = Path(directory) / "job.json"
        job.write_text(json.dumps({**geometry, "printer": printer, "images": paths}), encoding="utf-8")
        return _powershell(PRINT_SCRIPT, job=job, timeout=120)


LAYOUT_SCRIPT = r"""
function Get-LabelRectangle($iw, $ih, $pageWidth, $pageHeight, $printable) {
    $physical = New-Object System.Drawing.RectangleF(0,0,$pageWidth,$pageHeight)
    $safe = [System.Drawing.RectangleF]::Intersect($physical, $printable)
    # A half-millimeter allowance avoids edge loss from driver rounding/feed tolerance.
    $safe.Inflate(-2.0,-2.0)
    if ($safe.Width -le 0 -or $safe.Height -le 0) { throw 'The printer reported an invalid printable label area' }
    $scale = [Math]::Min($safe.Width / $iw, $safe.Height / $ih)
    $width = $iw * $scale; $height = $ih * $scale
    return New-Object System.Drawing.RectangleF(($safe.X + ($safe.Width - $width)/2), ($safe.Y + ($safe.Height - $height)/2), $width, $height)
}
"""


PRINT_SCRIPT = "Add-Type -AssemblyName System.Drawing\n" + LAYOUT_SCRIPT + r"""
$job = Get-Content -LiteralPath $env:CYBERLAB_LABEL_JOB -Raw -Encoding UTF8 | ConvertFrom-Json
$doc = New-Object System.Drawing.Printing.PrintDocument
$doc.PrinterSettings.PrinterName = $job.printer
if (-not $doc.PrinterSettings.IsValid) { throw 'The selected printer is not available' }
$doc.DocumentName = 'CyberLab Evidence Labels'
$doc.PrintController = New-Object System.Drawing.Printing.StandardPrintController
# Dymo paper is short-edge across the head, long-edge in the feed direction.
# Match actual dimensions; do not substitute a different "Shipping" stock.
$paper = $null
$candidates = @($doc.PrinterSettings.PaperSizes | Sort-Object @{Expression={if ($_.PaperName -like '*30256*') {0} else {1}}})
foreach ($candidate in $candidates) {
    if ([Math]::Abs($candidate.Width - $job.paper_width) -le 4 -and [Math]::Abs($candidate.Height - $job.paper_height) -le 4) { $paper = $candidate; break }
}
if ($null -eq $paper) { $paper = New-Object System.Drawing.Printing.PaperSize('CyberLab label', [int]$job.paper_width, [int]$job.paper_height) }
$doc.DefaultPageSettings.PaperSize = $paper
$doc.DefaultPageSettings.Landscape = $false
$doc.DefaultPageSettings.Margins = New-Object System.Drawing.Printing.Margins(0,0,0,0)
$doc.OriginAtMargins = $false
$script:pageIndex = 0
$handler = [System.Drawing.Printing.PrintPageEventHandler] {
    param($sender, $event)
    if ([Math]::Abs($event.PageBounds.Width - $job.paper_width) -gt 6 -or [Math]::Abs($event.PageBounds.Height - $job.paper_height) -gt 6) {
        $event.Cancel = $true
        throw 'The printer driver ignored the selected label size. Check its paper settings before retrying.'
    }
    $img = [System.Drawing.Image]::FromFile($job.images[$script:pageIndex])
    try {
        $event.Graphics.TranslateTransform(-$event.PageSettings.HardMarginX, -$event.PageSettings.HardMarginY)
        $event.Graphics.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
        $event.Graphics.PixelOffsetMode = [System.Drawing.Drawing2D.PixelOffsetMode]::HighQuality
        $dest = Get-LabelRectangle $img.Width $img.Height $event.PageBounds.Width $event.PageBounds.Height $event.PageSettings.PrintableArea
        $event.Graphics.DrawImage($img, $dest)
    } finally { $img.Dispose() }
    $script:pageIndex++
    $event.HasMorePages = $script:pageIndex -lt $job.images.Count
}
$doc.add_PrintPage($handler)
try {
    $doc.Print()
    @{success=$true; count=$script:pageIndex; printer=$job.printer; paper=$paper.PaperName} | ConvertTo-Json -Compress
} finally { $doc.remove_PrintPage($handler); $doc.Dispose() }
"""
