describe("SME Chaos Suite", () => {
  beforeEach(() => {
    cy.visit("/");
  });

  it("blocks cashier from owner views", () => {
    cy.window().then((win) => {
      win.localStorage.setItem("smeRole", "cashier");
    });
    cy.visit("/sme-dashboard");
    cy.get("body").then(($body) => {
      const bodyText = $body.text();
      expect(bodyText.includes("403") || bodyText.includes("Unauthorized") || bodyText.includes("Unable to load")).to.eq(true);
    });
  });

  it("queues offline POS sales and flushes when online", () => {
    cy.intercept("POST", "/api/v1/sme/transactions", {
      statusCode: 201,
      body: { status: "ok", transaction: { id: 1 } },
    }).as("postTx");

    cy.visit("/sme-pos");
    cy.window().then((win) => {
      Object.defineProperty(win.navigator, "onLine", {
        configurable: true,
        get: () => false,
      });
    });

    for (let i = 0; i < 5; i += 1) {
      cy.contains("button", "1").click();
      cy.contains("button", "Record Cash Sale").click();
      cy.contains("Offline: Sale Saved Locally.");
      cy.contains("button", "Clear").click();
    }

    cy.window().then((win) => {
      Object.defineProperty(win.navigator, "onLine", {
        configurable: true,
        get: () => true,
      });
      win.dispatchEvent(new Event("online"));
    });

    cy.get("@postTx.all").should("have.length.at.least", 1);
  });

  it("renders UPI modal QR element", () => {
    cy.visit("/sme-pos");
    cy.contains("button", "2").click();
    cy.contains("button", "Record UPI Sale").click();
    cy.get("canvas").should("exist");
    cy.contains("button", "Payment Received").should("exist");
  });
});