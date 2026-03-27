import argparse
import hashlib
import hmac
import json
import math
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET


TALLY_URL = "http://127.0.0.1:9000"
DB_PATH = "extractor_outbox.db"
CLOUD_URL = "https://example.com/ingest"
HTTP_TIMEOUT_SEC = 15
HMAC_SECRET = "DEMO_SECRET_KEY_123"
BALANCE_TOLERANCE = Decimal("0.00")


@dataclass
class ExtractResult:
    ok: bool
    payload: Optional[Dict[str, Any]]
    error: Optional[str]


class QuarantineError(Exception):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def stable_payload_id(tenant_id: str, report_date: str, payload: Dict[str, Any]) -> str:
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    h = hashlib.sha256(canon).hexdigest()[:16]
    return f"{tenant_id}:{report_date}:{h}"


def post_xml(url: str, xml_body: str, timeout: int = HTTP_TIMEOUT_SEC) -> str:
    req = Request(
        url=url,
        data=xml_body.encode("utf-8"),
        headers={"Content-Type": "application/xml; charset=utf-8"},
        method="POST",
    )
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def build_daybook_xml(report_date_yyyymmdd: str) -> str:
    return f"""
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export Data</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>Day Book</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <SVFROMDATE>{report_date_yyyymmdd}</SVFROMDATE>
        <SVTODATE>{report_date_yyyymmdd}</SVTODATE>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      </STATICVARIABLES>
    </DESC>
  </BODY>
</ENVELOPE>
""".strip()


def build_outstanding_xml(report_date_yyyymmdd: str) -> str:
    return f"""
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export Data</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>Bills Receivable</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <SVTODATE>{report_date_yyyymmdd}</SVTODATE>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      </STATICVARIABLES>
    </DESC>
  </BODY>
</ENVELOPE>
""".strip()



def build_stock_summary_xml(report_date_yyyymmdd: str) -> str:
    return f"""
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export Data</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>Stock Summary</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <SVTODATE>{report_date_yyyymmdd}</SVTODATE>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      </STATICVARIABLES>
    </DESC>
  </BODY>
</ENVELOPE>
""".strip()


def normalize_tag(tag: str) -> str:
    if "}" in tag:
        tag = tag.split("}", 1)[1]
    return tag.strip().lower()



def parse_decimal(text: Optional[str]) -> Optional[Decimal]:
    if text is None:
        return None
    cleaned = text.strip().replace(",", "")
    if cleaned == "":
        return None
    try:
        d = Decimal(cleaned)
    except InvalidOperation:
        return None

    if math.isnan(float(d)) or math.isinf(float(d)):
        return None
    return d


def collect_tag_texts(elem: ET.Element) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for child in elem.iter():
        tag = normalize_tag(child.tag)
        txt = (child.text or "").strip()
        if txt:
            out.setdefault(tag, []).append(txt)
    return out


def find_first_text(tag_map: Dict[str, List[str]], preferred_tags: List[str]) -> str:
    for t in preferred_tags:
        vals = tag_map.get(t.lower())
        if vals:
            return vals[0]
    return ""


def find_amount_tolerant(tag_map: Dict[str, List[str]]) -> Decimal:
    preferred = [
        "amount",
        "billpending",
        "amountpending",
        "billedqtyamount",
        "closingbalance",
        "deemedpositiveamount",
        "debitamount",
        "creditamount",
        "value",
    ]
    for t in preferred:
        vals = tag_map.get(t, [])
        for v in vals:
            d = parse_decimal(v)
            if d is not None:
                return d

    num_re = re.compile(r"^-?\d+(\.\d+)?$")
    for vals in tag_map.values():
        for v in vals:
            vv = v.replace(",", "").strip()
            if num_re.match(vv):
                d = parse_decimal(vv)
                if d is not None:
                    return d

    return Decimal("0")


def parse_debit_credit(tag_map: Dict[str, List[str]], amount: Decimal) -> Tuple[Decimal, Decimal]:
    debit = Decimal("0")
    credit = Decimal("0")

    debit_tags = ["debit", "debitamount", "dramount", "dr"]
    credit_tags = ["credit", "creditamount", "cramount", "cr"]

    for t in debit_tags:
        for v in tag_map.get(t, []):
            dv = parse_decimal(v)
            if dv is not None:
                debit += abs(dv)

    for t in credit_tags:
        for v in tag_map.get(t, []):
            cv = parse_decimal(v)
            if cv is not None:
                credit += abs(cv)

    if debit == 0 and credit == 0:
        # Fallback heuristic if explicit tags are absent.
        if amount >= 0:
            debit = amount
        else:
            credit = abs(amount)

    return debit, credit


def parse_daybook_xml(xml_text: str) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_text)
    records: List[Dict[str, Any]] = []

    for elem in root.iter():
        tag = normalize_tag(elem.tag)
        if tag not in ("voucher", "allledgerentries.list", "ledgerentry", "vch"):
            continue

        tag_map = collect_tag_texts(elem)

        date_val = find_first_text(tag_map, ["date", "voucherdate", "effectivedate"])
        party = find_first_text(tag_map, ["partyledgername", "ledgername", "partyname", "name"])
        vtype = find_first_text(tag_map, ["vouchertypename", "vchtype", "type"])
        vnum = find_first_text(tag_map, ["vouchernumber", "vchno", "number"])
        amount = find_amount_tolerant(tag_map)
        debit, credit = parse_debit_credit(tag_map, amount)

        if any([date_val, party, vtype, vnum]) or amount != Decimal("0"):
            records.append(
                {
                    "date": date_val,
                    "voucher_type": vtype,
                    "voucher_number": vnum,
                    "party": party,
                    "amount": str(amount),
                    "debit": str(debit),
                    "credit": str(credit),
                }
            )
    return records


def parse_outstanding_xml(xml_text: str) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_text)
    rows: List[Dict[str, Any]] = []

    for elem in root.iter():
        tag = normalize_tag(elem.tag)
        if tag not in ("billfixed", "bill", "outstanding", "billwise", "ledgeroutstanding"):
            continue

        tag_map = collect_tag_texts(elem)
        party = find_first_text(tag_map, ["ledgername", "partyledgername", "partyname", "name"])
        bill_no = find_first_text(tag_map, ["name", "billname", "billno", "billnumber"])
        due_date = find_first_text(tag_map, ["billduedate", "duedate", "date"])
        pending = find_amount_tolerant(tag_map)

        if any([party, bill_no, due_date]) or pending != Decimal("0"):
            rows.append(
                {
                    "party": party,
                    "bill_no": bill_no,
                    "due_date": due_date,
                    "pending_amount": str(pending),
                }
            )

    return rows


def parse_stock_xml(xml_text: str) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_text)
    items: List[Dict[str, Any]] = []

    for elem in root.iter():
        tag = normalize_tag(elem.tag)
        if tag not in ("stockitem", "stockgroupsummary", "stocksummary", "stockentry"):
            continue

        tag_map = collect_tag_texts(elem)
        name = (elem.get("NAME") or elem.get("name", "")).strip()
        if not name:
            name = find_first_text(tag_map, ["name", "stockitemname", "itemname"])

        qty_text = find_first_text(
            tag_map,
            ["closingbalance", "closingstock", "closingbaseunits", "closingqty", "quantity", "qty"],
        )
        value_text = find_first_text(
            tag_map,
            ["closingvalue", "closingbalancevalue", "closingstockvalue", "value", "amount"],
        )

        qty = parse_decimal(qty_text) if qty_text else Decimal("0")
        value = parse_decimal(value_text) if value_text else Decimal("0")

        if name or (qty is not None and qty != Decimal("0")):
            items.append(
                {
                    "item_name": name,
                    "closing_qty": str(qty or Decimal("0")),
                    "closing_value": str(value or Decimal("0")),
                }
            )

    return items


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS outbox (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            report_date TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            retries INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def save_outbox(conn: sqlite3.Connection, row_id: str, tenant_id: str, report_date: str, payload: Dict[str, Any]) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT OR REPLACE INTO outbox (id, tenant_id, report_date, payload_json, status, retries, last_error, created_at, updated_at)
        VALUES (?, ?, ?, ?, COALESCE((SELECT status FROM outbox WHERE id=?), 'pending'),
                COALESCE((SELECT retries FROM outbox WHERE id=?), 0),
                COALESCE((SELECT last_error FROM outbox WHERE id=?), NULL),
                COALESCE((SELECT created_at FROM outbox WHERE id=?), ?), ?)
        """,
        (
            row_id,
            tenant_id,
            report_date,
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
            row_id,
            row_id,
            row_id,
            row_id,
            now,
            now,
        ),
    )
    conn.commit()


def mark_sent(conn: sqlite3.Connection, row_id: str) -> None:
    conn.execute(
        "UPDATE outbox SET status='sent', updated_at=? WHERE id=?",
        (utc_now_iso(), row_id),
    )
    conn.commit()


def mark_failed(conn: sqlite3.Connection, row_id: str, err: str) -> None:
    conn.execute(
        "UPDATE outbox SET status='pending', retries=retries+1, last_error=?, updated_at=? WHERE id=?",
        (err[:800], utc_now_iso(), row_id),
    )
    conn.commit()


def pending_rows(conn: sqlite3.Connection, limit: int = 100) -> List[Tuple[str, str]]:
    cur = conn.execute(
        "SELECT id, payload_json FROM outbox WHERE status='pending' ORDER BY created_at ASC LIMIT ?",
        (limit,),
    )
    return [(r[0], r[1]) for r in cur.fetchall()]


def canonical_json(data: Dict[str, Any]) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def build_signed_envelope(payload: Dict[str, Any], secret: str) -> Dict[str, Any]:
    ts = utc_epoch()
    nonce = str(uuid.uuid4())
    payload_hash = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

    signing_material = f"{ts}.{nonce}.{payload_hash}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), signing_material, hashlib.sha256).hexdigest()

    return {
        "timestamp": ts,
        "nonce": nonce,
        "payload_hash": payload_hash,
        "signature": signature,
        "payload": payload,
    }


def upload_payload(cloud_url: str, payload: Dict[str, Any], timeout: int = HTTP_TIMEOUT_SEC) -> None:
    envelope = build_signed_envelope(payload, HMAC_SECRET)
    body = canonical_json(envelope).encode("utf-8")
    req = Request(
        url=cloud_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-FI-Timestamp": str(envelope["timestamp"]),
            "X-FI-Nonce": envelope["nonce"],
            "X-FI-Signature": envelope["signature"],
        },
        method="POST",
    )
    with urlopen(req, timeout=timeout) as resp:
        code = getattr(resp, "status", 200)
        if code < 200 or code >= 300:
            raise RuntimeError(f"Cloud upload failed with status={code}")


def dual_algorithm_validate(daybook: List[Dict[str, Any]]) -> Dict[str, str]:
    if not daybook:
        raise QuarantineError("Quarantine Error: empty daybook, refusing to send")

    # Algorithm A: direct summation from parsed debit/credit fields.
    debits_a = Decimal("0")
    credits_a = Decimal("0")
    for i, row in enumerate(daybook):
        d = parse_decimal(str(row.get("debit", "")))
        c = parse_decimal(str(row.get("credit", "")))
        if d is None or c is None:
            raise QuarantineError(f"Quarantine Error: NaN/corrupt debit/credit at row={i}")
        if d < 0 or c < 0:
            raise QuarantineError(f"Quarantine Error: negative debit/credit at row={i}")
        debits_a += d
        credits_a += c

    # Algorithm B: independent recomputation using amount sign semantics.
    debits_b = Decimal("0")
    credits_b = Decimal("0")
    for i, row in enumerate(daybook):
        amt = parse_decimal(str(row.get("amount", "")))
        if amt is None:
            raise QuarantineError(f"Quarantine Error: NaN/corrupt amount at row={i}")
        if amt >= 0:
            debits_b += amt
        else:
            credits_b += abs(amt)

    # Cross-check algorithm consistency.
    if abs(debits_a - debits_b) > BALANCE_TOLERANCE:
        raise QuarantineError("Quarantine Error: dual algorithm mismatch on total debits")
    if abs(credits_a - credits_b) > BALANCE_TOLERANCE:
        raise QuarantineError("Quarantine Error: dual algorithm mismatch on total credits")

    # Final accounting invariant.
    if abs(debits_a - credits_a) > BALANCE_TOLERANCE:
        raise QuarantineError(
            f"Quarantine Error: books do not balance (debits={debits_a} credits={credits_a})"
        )

    return {
        "total_debits": str(debits_a),
        "total_credits": str(credits_a),
        "tolerance": str(BALANCE_TOLERANCE),
        "validated_at_utc": utc_now_iso(),
    }


def extract(report_date_yyyymmdd: str, tally_url: str = TALLY_URL) -> ExtractResult:
    try:
        daybook_xml = post_xml(tally_url, build_daybook_xml(report_date_yyyymmdd))
        outstanding_xml = post_xml(tally_url, build_outstanding_xml(report_date_yyyymmdd))

        daybook = parse_daybook_xml(daybook_xml)
        outstanding = parse_outstanding_xml(outstanding_xml)
        # Inventory fetch is non-fatal: service businesses may not maintain stock in Tally.
        try:
            stock_xml = post_xml(tally_url, build_stock_summary_xml(report_date_yyyymmdd))
            inventory = parse_stock_xml(stock_xml)
        except (HTTPError, URLError, TimeoutError, ET.ParseError):
            inventory = []

        validation = dual_algorithm_validate(daybook)

        payload = {
            "schema_version": "2.0",
            "extracted_at_utc": utc_now_iso(),
            "report_date": report_date_yyyymmdd,
            "daybook": daybook,
            "outstanding_receivables": outstanding,
            "inventory_summary": inventory,
            "validation": validation,
            "meta": {
                "source": "tally_prime_xml_port_9000",
                "host": "localhost",
            },
        }
        return ExtractResult(ok=True, payload=payload, error=None)

    except QuarantineError as e:
        return ExtractResult(ok=False, payload=None, error=str(e))
    except (HTTPError, URLError, TimeoutError) as e:
        return ExtractResult(ok=False, payload=None, error=f"Tally unreachable: {e}")
    except ET.ParseError as e:
        return ExtractResult(ok=False, payload=None, error=f"Malformed XML from Tally: {e}")
    except Exception as e:
        return ExtractResult(ok=False, payload=None, error=f"Unexpected extraction failure: {e}")


def run_once(tenant_id: str, report_date_yyyymmdd: str, db_path: str, tally_url: str, cloud_url: str) -> int:
    conn = sqlite3.connect(db_path)
    init_db(conn)

    result = extract(report_date_yyyymmdd, tally_url=tally_url)
    if not result.ok:
        print(f"[ERROR] {result.error}")
        return 2

    payload = result.payload
    row_id = stable_payload_id(tenant_id, report_date_yyyymmdd, payload)

    # Save to outbox only after passing dual-algorithm validation.
    save_outbox(conn, row_id, tenant_id, report_date_yyyymmdd, payload)
    print(f"[INFO] saved payload to outbox id={row_id}")

    try:
        upload_payload(cloud_url, payload)
        mark_sent(conn, row_id)
        print(f"[INFO] upload success id={row_id}")
        return 0
    except Exception as e:
        mark_failed(conn, row_id, str(e))
        print(f"[WARN] upload failed; kept in outbox id={row_id}: {e}")
        return 1


def flush_outbox(db_path: str, cloud_url: str) -> int:
    conn = sqlite3.connect(db_path)
    init_db(conn)
    rows = pending_rows(conn, limit=500)
    if not rows:
        print("[INFO] no pending outbox rows")
        return 0

    ok_count = 0
    fail_count = 0
    for row_id, payload_json in rows:
        try:
            payload = json.loads(payload_json)
            upload_payload(cloud_url, payload)
            mark_sent(conn, row_id)
            ok_count += 1
        except Exception as e:
            mark_failed(conn, row_id, str(e))
            fail_count += 1

    print(f"[INFO] outbox flush complete: sent={ok_count} failed={fail_count}")
    return 0 if fail_count == 0 else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Friday Insights local Tally extractor (Hardened V2)")
    p.add_argument("--tenant-id", default="tenant_demo")
    p.add_argument("--report-date", default=datetime.now().strftime("%Y%m%d"))
    p.add_argument("--db-path", default=DB_PATH)
    p.add_argument("--tally-url", default=TALLY_URL)
    p.add_argument("--cloud-url", default=CLOUD_URL)
    p.add_argument("--mode", choices=["run", "flush", "loop"], default="run")
    p.add_argument("--interval-sec", type=int, default=1800, help="loop interval for catch-up mode")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.mode == "run":
        return run_once(args.tenant_id, args.report_date, args.db_path, args.tally_url, args.cloud_url)

    if args.mode == "flush":
        return flush_outbox(args.db_path, args.cloud_url)

    while True:
        _ = run_once(args.tenant_id, args.report_date, args.db_path, args.tally_url, args.cloud_url)
        _ = flush_outbox(args.db_path, args.cloud_url)
        time.sleep(max(30, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
