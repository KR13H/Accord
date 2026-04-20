const ESC = 0x1b;
const GS = 0x1d;

let connectedDevice = null;
let outEndpointNumber = null;
let claimedInterfaceNumber = null;

function encoder() {
  return new TextEncoder();
}

function toBytes(text) {
  return Array.from(encoder().encode(`${text}\n`));
}

async function ensurePrinterConnection() {
  if (!navigator.usb) {
    throw new Error("WebUSB is not supported in this browser");
  }

  if (!connectedDevice) {
    connectedDevice = await navigator.usb.requestDevice({ filters: [] });
  }

  if (!connectedDevice.opened) {
    await connectedDevice.open();
  }

  if (connectedDevice.configuration === null) {
    await connectedDevice.selectConfiguration(1);
  }

  if (claimedInterfaceNumber === null || outEndpointNumber === null) {
    const iface = connectedDevice.configuration.interfaces.find((item) =>
      item.alternates.some((alt) => alt.endpoints.some((ep) => ep.direction === "out"))
    );
    if (!iface) {
      throw new Error("No compatible OUT endpoint found on thermal printer");
    }

    claimedInterfaceNumber = iface.interfaceNumber;
    await connectedDevice.claimInterface(claimedInterfaceNumber);

    const alt = iface.alternates.find((item) => item.endpoints.some((ep) => ep.direction === "out"));
    const endpoint = alt?.endpoints.find((ep) => ep.direction === "out");
    if (!endpoint) {
      throw new Error("Failed to resolve printer OUT endpoint");
    }
    outEndpointNumber = endpoint.endpointNumber;
  }
}

function sanitizeLine(value, maxLen = 32) {
  const line = String(value ?? "").replace(/[\r\n]+/g, " ").trim();
  if (line.length <= maxLen) {
    return line;
  }
  return `${line.slice(0, maxLen - 1)}…`;
}

function splitColumns(left, right, width = 32) {
  const l = sanitizeLine(left, width);
  const r = sanitizeLine(right, width);
  const gap = Math.max(1, width - l.length - r.length);
  return `${l}${" ".repeat(gap)}${r}`;
}

function buildEscPosPayload(transaction, items) {
  const tx = transaction || {};
  const lines = [];
  lines.push(...toBytes("ACCORD POS"));
  lines.push(...toBytes("------------------------------"));

  const printableItems = Array.isArray(items) ? items : [];
  if (printableItems.length === 0) {
    lines.push(...toBytes(splitColumns("Sale", `INR ${Number(tx.amount || 0).toFixed(2)}`)));
  } else {
    for (const item of printableItems) {
      const qty = Number(item.quantity || 1);
      const unitPrice = Number(item.unit_price || item.price || 0);
      const subtotal = qty * unitPrice;
      const name = item.localized_name || item.item_name || item.name || "Item";
      lines.push(...toBytes(sanitizeLine(name, 32)));
      lines.push(...toBytes(splitColumns(`${qty} x ${unitPrice.toFixed(2)}`, subtotal.toFixed(2))));
    }
  }

  lines.push(...toBytes("------------------------------"));
  lines.push(...toBytes(splitColumns("TOTAL", `INR ${Number(tx.amount || 0).toFixed(2)}`)));
  lines.push(...toBytes(`Payment: ${sanitizeLine(tx.payment_method || "Cash")}`));
  lines.push(...toBytes(`Time: ${new Date().toLocaleString("en-IN")}`));
  lines.push(...toBytes("Thank you for shopping!"));

  return new Uint8Array([
    ESC,
    0x40,
    ESC,
    0x61,
    0x01,
    ...lines,
    ESC,
    0x61,
    0x00,
    0x0a,
    0x0a,
    0x0a,
    GS,
    0x56,
    0x00,
  ]);
}

export async function printReceipt(transaction, items) {
  await ensurePrinterConnection();
  if (outEndpointNumber === null) {
    throw new Error("Printer endpoint is not initialized");
  }

  const payload = buildEscPosPayload(transaction, items);
  await connectedDevice.transferOut(outEndpointNumber, payload);
  return { status: "printed" };
}

export function browserPrintReceipt(transaction, items) {
  const tx = transaction || {};
  const printableItems = Array.isArray(items) ? items : [];

  const htmlItems = printableItems
    .map((item) => {
      const qty = Number(item.quantity || 1);
      const unitPrice = Number(item.unit_price || item.price || 0);
      const subtotal = qty * unitPrice;
      const name = sanitizeLine(item.localized_name || item.item_name || item.name || "Item", 48);
      return `<tr><td>${name}</td><td style="text-align:center;">${qty}</td><td style="text-align:right;">${subtotal.toFixed(2)}</td></tr>`;
    })
    .join("");

  const popup = window.open("", "_blank", "width=360,height=640");
  if (!popup) {
    throw new Error("Could not open print window");
  }

  popup.document.write(`
    <html>
      <head>
        <title>Receipt</title>
        <style>
          body { font-family: monospace; width: 58mm; margin: 0 auto; padding: 8px; }
          table { width: 100%; border-collapse: collapse; font-size: 12px; }
          td { padding: 2px 0; }
          .total { border-top: 1px dashed #000; margin-top: 6px; padding-top: 6px; font-weight: 700; }
        </style>
      </head>
      <body>
        <div style="text-align:center; font-weight:700;">ACCORD POS</div>
        <div style="text-align:center; margin-bottom:6px;">${new Date().toLocaleString("en-IN")}</div>
        <table>${htmlItems || `<tr><td>Sale</td><td></td><td style="text-align:right;">${Number(tx.amount || 0).toFixed(2)}</td></tr>`}</table>
        <div class="total">TOTAL: INR ${Number(tx.amount || 0).toFixed(2)}</div>
        <div>Payment: ${sanitizeLine(tx.payment_method || "Cash", 24)}</div>
      </body>
    </html>
  `);
  popup.document.close();
  popup.focus();
  popup.print();
}
