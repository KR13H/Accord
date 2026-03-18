from http.server import BaseHTTPRequestHandler, HTTPServer

DAYBOOK_XML = """<ENVELOPE><BODY><DATA>
  <VOUCHER><DATE>20260317</DATE><PARTYLEDGERNAME>Sharma Traders</PARTYLEDGERNAME><AMOUNT>45000.00</AMOUNT></VOUCHER>
  <VOUCHER><DATE>20260317</DATE><PARTYLEDGERNAME>Apex Tech</PARTYLEDGERNAME><BilledQtyAmount>32000.00</BilledQtyAmount></VOUCHER>
</DATA></BODY></ENVELOPE>"""

OUTSTANDING_XML = """<ENVELOPE><BODY><DATA>
  <BILLFIXED><LEDGERNAME>Sharma Traders</LEDGERNAME><NAME>INV-1001</NAME><BILLDUEDATE>20260310</BILLDUEDATE><BILLPENDING>45000.00</BILLPENDING></BILLFIXED>
  <BILLFIXED><LEDGERNAME>Global Auto</LEDGERNAME><NAME>INV-1002</NAME><BILLDUEDATE>20260314</BILLDUEDATE><AmountPending>18500.00</AmountPending></BILLFIXED>
</DATA></BODY></ENVELOPE>"""

class Handler(BaseHTTPRequestHandler):
  def do_POST(self):
    n = int(self.headers.get("Content-Length", "0"))
    body = self.rfile.read(n).decode("utf-8", errors="ignore")
    payload = DAYBOOK_XML if "Day Book" in body else OUTSTANDING_XML
    self.send_response(200)
    self.send_header("Content-Type", "application/xml; charset=utf-8")
    self.end_headers()
    self.wfile.write(payload.encode("utf-8"))
  def log_message(self, *args):
    pass

HTTPServer(("127.0.0.1", 9000), Handler).serve_forever()
