"""
parse_sip.py - VoLTE pipeline, SIP parser.

Bronze: every SIP packet as captured (duplicates kept)
Silver: deduplicated SIP messages (one row per real message)

Run from the project root:
    python parse_sip.py input_logs/4_sip.pcap
"""
import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import pyshark

OUTPUT_DIR = Path("output_logs")
BRONZE_DIR = OUTPUT_DIR / "bronze"
SILVER_DIR = OUTPUT_DIR / "silver"


def inner_ip(pkt):
    """Tunneled (GTP) packets have an outer and inner IP layer.
    The inner one is the real phone <-> IMS traffic."""
    ip_layers = [layer for layer in pkt.layers if layer.layer_name == "ip"]
    if not ip_layers:
        return None, None
    return ip_layers[-1].src, ip_layers[-1].dst


def extract_sip(pcap_path: Path) -> pd.DataFrame:
    """BRONZE: one row per captured SIP packet, nothing removed."""
    rows = []
    cap = pyshark.FileCapture(str(pcap_path), display_filter="sip", keep_packets=False)
    for pkt in cap:
        sip = pkt.sip
        src, dst = inner_ip(pkt)
        rows.append({
            "source_file": pcap_path.name,
            "frame_number": int(pkt.number),
            "event_time_utc": datetime.fromtimestamp(float(pkt.sniff_timestamp), tz=timezone.utc),
            "src_ip": src,
            "dst_ip": dst,
            "call_id": getattr(sip, "call_id", None),
            "cseq": getattr(sip, "cseq", None),
            "method": getattr(sip, "method", None),
            "status_code": getattr(sip, "status_code", None),
            "ingested_at_utc": datetime.now(timezone.utc),
        })
    cap.close()
    return pd.DataFrame(rows)


def dedupe_sip(bronze: pd.DataFrame) -> pd.DataFrame:
    """SILVER: same Call-ID + CSeq + method/status = one real message
    captured twice. Keep the earliest copy."""
    keys = ["call_id", "cseq", "method", "status_code"]
    return (
        bronze.sort_values("event_time_utc")
              .drop_duplicates(subset=keys, keep="first")
              .reset_index(drop=True)
    )


def main():
    if len(sys.argv) != 2:
        sys.exit("Usage: python parse_sip.py input_logs/<file.pcap>")

    pcap = Path(sys.argv[1])
    if not pcap.exists():
        sys.exit(f"File not found: {pcap}")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    bronze = extract_sip(pcap)
    if bronze.empty:
        sys.exit(f"No SIP packets found in {pcap.name}")
    silver = dedupe_sip(bronze)

    bronze_out = BRONZE_DIR / f"{pcap.stem}_sip.csv"
    silver_out = SILVER_DIR / f"{pcap.stem}_sip.csv"
    bronze.to_csv(bronze_out, index=False)
    silver.to_csv(silver_out, index=False)

    print(f"Bronze rows: {len(bronze):>4}  -> {bronze_out}")
    print(f"Silver rows: {len(silver):>4}  -> {silver_out}\n")
    print(silver[["event_time_utc", "src_ip", "dst_ip", "method", "status_code"]].to_string(index=False))


if __name__ == "__main__":
    main()