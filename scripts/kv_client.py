#!/usr/bin/env python3
"""
Minimal TCP client for the KV store binary protocol (see com.kvstore.network.Protocol).
Follows STATUS_REDIRECT (0x03) to the Raft leader automatically.
"""
from __future__ import annotations

import argparse
import socket
import struct
import sys
from typing import List, Tuple

STATUS_OK = 0x00
STATUS_NOT_FOUND = 0x01
STATUS_ERROR = 0x02
STATUS_REDIRECT = 0x03

OP_PUT = 0x01
OP_GET = 0x02
OP_DELETE = 0x03
OP_RANGE = 0x04
OP_BATCH_PUT = 0x05


def _read_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed while reading response")
        buf.extend(chunk)
    return bytes(buf)


def _read_response(sock: socket.socket) -> Tuple[int, bytes]:
    header = _read_exact(sock, 5)
    status = header[0]
    plen = struct.unpack(">I", header[1:5])[0]
    payload = _read_exact(sock, plen) if plen else b""
    return status, payload


def _encode_put(key: str, value: bytes) -> bytes:
    kb = key.encode("utf-8")
    payload_len = 4 + len(kb) + 4 + len(value)
    return struct.pack(">BI", OP_PUT, payload_len) + struct.pack(">I", len(kb)) + kb + struct.pack(">I", len(value)) + value


def _encode_get(key: str) -> bytes:
    kb = key.encode("utf-8")
    payload_len = 4 + len(kb)
    return struct.pack(">BI", OP_GET, payload_len) + struct.pack(">I", len(kb)) + kb


def _encode_delete(key: str) -> bytes:
    kb = key.encode("utf-8")
    payload_len = 4 + len(kb)
    return struct.pack(">BI", OP_DELETE, payload_len) + struct.pack(">I", len(kb)) + kb


def _encode_range(start_key: str, end_key: str) -> bytes:
    sb = start_key.encode("utf-8")
    eb = end_key.encode("utf-8")
    payload_len = 4 + len(sb) + 4 + len(eb)
    return (
        struct.pack(">BI", OP_RANGE, payload_len)
        + struct.pack(">I", len(sb))
        + sb
        + struct.pack(">I", len(eb))
        + eb
    )


def _encode_batch_put(pairs: List[Tuple[str, bytes]]) -> bytes:
    payload = struct.pack(">I", len(pairs))
    for key, value in pairs:
        kb = key.encode("utf-8")
        payload += struct.pack(">I", len(kb)) + kb + struct.pack(">I", len(value)) + value
    payload_len = len(payload)
    return struct.pack(">BI", OP_BATCH_PUT, payload_len) + payload


def _decode_redirect(payload: bytes) -> Tuple[str, int]:
    hlen = struct.unpack(">I", payload[0:4])[0]
    host = payload[4 : 4 + hlen].decode("utf-8")
    port = struct.unpack(">I", payload[4 + hlen : 8 + hlen])[0]
    return host, port


def _decode_range_payload(payload: bytes) -> List[Tuple[str, bytes]]:
    buf = memoryview(payload)
    off = 0
    (count,) = struct.unpack_from(">I", buf, off)
    off += 4
    out: List[Tuple[str, bytes]] = []
    for _ in range(count):
        kl = struct.unpack_from(">I", buf, off)[0]
        off += 4
        key = bytes(buf[off : off + kl]).decode("utf-8")
        off += kl
        vl = struct.unpack_from(">I", buf, off)[0]
        off += 4
        val = bytes(buf[off : off + vl])
        off += vl
        out.append((key, val))
    return out


def send_with_redirects(host: str, port: int, request: bytes, timeout: float = 30.0, max_redirects: int = 8) -> Tuple[int, bytes]:
    current_host, current_port = host, port
    for _ in range(max_redirects):
        with socket.create_connection((current_host, current_port), timeout=timeout) as sock:
            sock.sendall(request)
            status, payload = _read_response(sock)
        if status == STATUS_REDIRECT:
            current_host, current_port = _decode_redirect(payload)
            continue
        return status, payload
    raise RuntimeError(f"too many redirects (>{max_redirects})")


def _status_name(s: int) -> str:
    return {STATUS_OK: "OK", STATUS_NOT_FOUND: "NOT_FOUND", STATUS_ERROR: "ERROR", STATUS_REDIRECT: "REDIRECT"}.get(s, f"0x{s:02x}")


def main() -> int:
    p = argparse.ArgumentParser(description="KV store TCP demo client (binary protocol + Raft redirect)")
    p.add_argument("--host", default="127.0.0.1", help="Initial KV server host")
    p.add_argument("--port", type=int, default=7777, help="Initial KV client port")
    p.add_argument("--timeout", type=float, default=30.0, help="Socket timeout seconds")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("put", help="PUT key value")
    sp.add_argument("key")
    sp.add_argument("value")

    sg = sub.add_parser("get", help="GET key")
    sg.add_argument("key")

    sd = sub.add_parser("delete", help="DELETE key")
    sd.add_argument("key")

    sr = sub.add_parser("range", help="RANGE start_key end_key (inclusive lexicographic)")
    sr.add_argument("start_key")
    sr.add_argument("end_key")

    sb = sub.add_parser("batch-put", help="BATCH_PUT key=value ...")
    sb.add_argument("pairs", nargs="+", metavar="key=value", help="One or more key=value pairs")

    args = p.parse_args()

    try:
        if args.cmd == "put":
            req = _encode_put(args.key, args.value.encode("utf-8"))
            status, payload = send_with_redirects(args.host, args.port, req, timeout=args.timeout)
            print(f"{_status_name(status)}")
            if status == STATUS_ERROR:
                print(payload.decode("utf-8", errors="replace"), file=sys.stderr)
                return 1
            return 0 if status == STATUS_OK else 1

        if args.cmd == "get":
            req = _encode_get(args.key)
            status, payload = send_with_redirects(args.host, args.port, req, timeout=args.timeout)
            if status == STATUS_OK:
                print(f"{_status_name(status)}")
                # Avoid mixing TextIO and binary writes (ordering can look wrong in demos).
                print(payload.decode("utf-8", errors="replace"))
            else:
                print(f"{_status_name(status)}")
            if status == STATUS_ERROR:
                print(payload.decode("utf-8", errors="replace"), file=sys.stderr)
            return 0 if status == STATUS_OK else 1

        if args.cmd == "delete":
            req = _encode_delete(args.key)
            status, payload = send_with_redirects(args.host, args.port, req, timeout=args.timeout)
            print(f"{_status_name(status)}")
            if status == STATUS_ERROR:
                print(payload.decode("utf-8", errors="replace"), file=sys.stderr)
            return 0 if status == STATUS_OK else 1

        if args.cmd == "range":
            req = _encode_range(args.start_key, args.end_key)
            status, payload = send_with_redirects(args.host, args.port, req, timeout=args.timeout)
            print(f"{_status_name(status)}")
            if status == STATUS_OK:
                for k, v in _decode_range_payload(payload):
                    print(f"  {k} = {v.decode('utf-8', errors='replace')}")
            elif status == STATUS_ERROR:
                print(payload.decode("utf-8", errors="replace"), file=sys.stderr)
            return 0 if status == STATUS_OK else 1

        if args.cmd == "batch-put":
            pairs: List[Tuple[str, bytes]] = []
            for item in args.pairs:
                if "=" not in item:
                    print(f"expected key=value, got: {item}", file=sys.stderr)
                    return 2
                k, v = item.split("=", 1)
                pairs.append((k, v.encode("utf-8")))
            req = _encode_batch_put(pairs)
            status, payload = send_with_redirects(args.host, args.port, req, timeout=args.timeout)
            print(f"{_status_name(status)}")
            if status == STATUS_ERROR:
                print(payload.decode("utf-8", errors="replace"), file=sys.stderr)
            return 0 if status == STATUS_OK else 1

    except (OSError, EOFError, RuntimeError) as e:
        print(str(e), file=sys.stderr)
        return 2

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
