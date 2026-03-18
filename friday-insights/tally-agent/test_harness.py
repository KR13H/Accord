import json
import os
import sqlite3
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import extractor


class MockCloudHandler(BaseHTTPRequestHandler):
    should_fail = False
    received = []

    def do_POST(self):
        n = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(n).decode("utf-8", errors="ignore")
        if self.should_fail:
            self.send_response(503)
            self.end_headers()
            return
        try:
            MockCloudHandler.received.append(json.loads(body))
        except Exception:
            pass
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


class MockTallyHandler(BaseHTTPRequestHandler):
    malformed = False

    def do_POST(self):
        n = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(n).decode("utf-8", errors="ignore")
        if self.malformed:
            payload = "<ENVELOPE><BROKEN>"
        elif "Day Book" in body:
            payload = """
            <ENVELOPE><BODY><DATA>
              <VOUCHER><DATE>20260317</DATE><PARTYLEDGERNAME>Sharma Traders</PARTYLEDGERNAME><BilledQtyAmount>45000.00</BilledQtyAmount></VOUCHER>
            </DATA></BODY></ENVELOPE>
            """.strip()
        else:
            payload = """
            <ENVELOPE><BODY><DATA>
              <BILLFIXED><LEDGERNAME>Global Auto</LEDGERNAME><NAME>INV-12</NAME><AmountPending>18500.00</AmountPending></BILLFIXED>
            </DATA></BODY></ENVELOPE>
            """.strip()
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.end_headers()
        self.wfile.write(payload.encode("utf-8"))

    def log_message(self, *args):
        pass


def start_server(host, port, handler_cls):
    server = HTTPServer((host, port), handler_cls)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


def count_pending(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.execute("SELECT COUNT(*) FROM outbox WHERE status='pending'")
    return c.fetchone()[0]


def run():
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "outbox.db")

    print("\n[1] Tally down simulation")
    code = extractor.run_once(
        tenant_id="t1",
        report_date_yyyymmdd="20260317",
        db_path=db_path,
        tally_url="http://127.0.0.1:9999",
        cloud_url="http://127.0.0.1:9100/ingest",
    )
    print("exit_code:", code, "(expected non-zero due to tally down)")

    print("\n[2] Malformed XML simulation")
    tally = start_server("127.0.0.1", 9001, MockTallyHandler)
    MockTallyHandler.malformed = True
    code = extractor.run_once("t1", "20260317", db_path, "http://127.0.0.1:9001", "http://127.0.0.1:9100/ingest")
    print("exit_code:", code, "(expected extraction failure)")
    MockTallyHandler.malformed = False
    tally.shutdown()

    print("\n[3] Alternate tags simulation (TDL trap)")
    tally = start_server("127.0.0.1", 9002, MockTallyHandler)
    code = extractor.run_once("t1", "20260317", db_path, "http://127.0.0.1:9002", "http://127.0.0.1:9100/ingest")
    print("exit_code:", code, "(expected upload fail if cloud absent, but parse success and outbox write)")
    tally.shutdown()

    print("\n[4] Internet down with SQLite outbox fallback")
    pending = count_pending(db_path)
    print("pending_outbox_rows:", pending, "(expected >= 1)")

    print("\n[5] Replay-safe resend via outbox flush")
    cloud = start_server("127.0.0.1", 9100, MockCloudHandler)
    MockCloudHandler.should_fail = False
    code = extractor.flush_outbox(db_path, "http://127.0.0.1:9100/ingest")
    print("flush_exit_code:", code, "(expected 0)")
    print("cloud_received_payloads:", len(MockCloudHandler.received), "(expected >= 1)")
    cloud.shutdown()

    tmp.cleanup()
    print("\nAll simulations completed.")


if __name__ == "__main__":
    run()
