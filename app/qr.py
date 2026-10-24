import io
from datetime import datetime, timezone
import qrcode
import qrcode.image.svg


def format_size(bytes_count: int) -> str:
    """Formats raw bytes into a human-readable string."""
    if bytes_count < 1024:
        return f"{bytes_count} B"
    elif bytes_count < 1024 * 1024:
        return f"{bytes_count / 1024:.2f} KB ({bytes_count:,} bytes)"
    elif bytes_count < 1024 * 1024 * 1024:
        return f"{bytes_count / (1024 * 1024):.2f} MB ({bytes_count:,} bytes)"
    else:
        return f"{bytes_count / (1024 * 1024 * 1024):.2f} GB ({bytes_count:,} bytes)"


def generate_ascii_qr(data: str) -> str:
    """Generates an inverted compact terminal ASCII QR code."""
    qr = qrcode.QRCode(border=1)
    qr.add_data(data)
    qr.make(fit=True)
    string_io = io.StringIO()
    qr.print_ascii(out=string_io, invert=True)
    return string_io.getvalue()


def generate_svg_qr(data: str) -> str:
    """Generates an inline SVG string for the given URL or data using SvgPathImage."""
    factory = qrcode.image.svg.SvgPathImage
    qr_img = qrcode.make(data, image_factory=factory, box_size=10, border=2)
    bio = io.BytesIO()
    qr_img.save(bio)
    return bio.getvalue().decode("utf-8")


def format_cli_summary(
    original_name: str,
    file_size_bytes: int,
    sha256_hash: str,
    expires_at: datetime,
    max_downloads: int,
    download_url: str,
    pin: str,
) -> str:
    """Constructs a clean ASCII box summary and includes terminal ASCII QR code."""
    now = datetime.now(timezone.utc)
    delta_mins = max(1, int((expires_at - now).total_seconds() / 60))
    expires_str = expires_at.strftime("%Y-%m-%d %H:%M:%S UTC")

    ascii_qr = generate_ascii_qr(download_url)

    line = "+-------------------------------------------------------------------------+"
    header = (
        f"{line}\n"
        f"|                                QR-DROP                                  |\n"
        f"|      Ephemeral Streaming File-Transfer & S3 Object Storage Engine       |\n"
        f"{line}\n"
    )

    details = (
        f"  File Name     : {original_name}\n"
        f"  File Size     : {format_size(file_size_bytes)}\n"
        f"  SHA-256       : {sha256_hash}\n"
        f"  PIN Code      : {pin}\n"
        f"  Download URL  : {download_url}\n"
        f"  Expires At    : {expires_str} (in {delta_mins} mins)\n"
        f"  Max Downloads : {max_downloads} (Remaining: {max_downloads})\n"
        f"{line}\n\n"
        f"Scan QR code from terminal or mobile camera:\n\n"
        f"{ascii_qr}\n"
        f"To download via CLI:\n"
        f"  curl -O {download_url}\n"
    )

    return header + details
