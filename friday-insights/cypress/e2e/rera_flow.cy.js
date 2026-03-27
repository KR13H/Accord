describe("RERA Booking Critical Path", () => {
  const adminHeaders = {
    "X-Role": "admin",
    "X-Admin-Id": "1",
  };

  function parseCount(text) {
    const cleaned = String(text).replace(/,/g, "").trim();
    const match = cleaned.match(/\d+/);
    return Number(match ? match[0] : "0");
  }

  let baselineCount = 0;
  let createdBookingId = "";

  beforeEach(() => {
    baselineCount = 0;
    createdBookingId = "";

    cy.request({
      method: "GET",
      url: "/api/v1/dashboard/summary",
      headers: adminHeaders,
    }).then((res) => {
      baselineCount = Number(res.body?.total_bookings || 0);
    });
  });

  afterEach(() => {
    if (!createdBookingId) {
      return;
    }

    cy.request({
      method: "DELETE",
      url: `/api/v1/bookings/${createdBookingId}`,
      headers: adminHeaders,
      failOnStatusCode: false,
    });
  });

  it("logs in, creates booking, and verifies total bookings increment", () => {
    const seed = Date.now();

    cy.visit("/signup");

    cy.get('input[placeholder="Aarav Mehta"]').clear().type("Cypress Manager");
    cy.get('input[placeholder="you@company.com"]').clear().type(`cypress+${seed}@accord.test`);
    cy.contains("button", "Continue with Email").click();

    cy.visit("/dashboard");
    cy.url().should("include", "/dashboard");

    cy.intercept("POST", "**/api/v1/bookings").as("createBooking");
    cy.intercept("GET", "**/api/v1/dashboard/summary*").as("getSummary");

    cy.get('[data-cy="booking-project-id"], input[placeholder="Project ID"]').first().clear().type("PRJ-NORTH-01");
    cy.get('[data-cy="booking-customer-name"], input[placeholder="Customer Name"]').first().clear().type(`Cypress Buyer ${seed}`);
    cy.get('[data-cy="booking-unit-code"], input[placeholder="Unit Code"]').first().clear().type(`UNIT-${seed}`);
    cy.get('[data-cy="booking-total-consideration"], input[placeholder="Total Consideration"]').first().clear().type("6200000");

    cy.get('[data-cy="booking-submit"], button').contains(/Create Booking/i).click();

    cy.wait("@createBooking").then(({ request, response }) => {
      expect(response?.statusCode).to.eq(201);
      createdBookingId = String(request.body.booking_id || "");
      expect(createdBookingId).to.match(/^BK-WEB-/);
    });

    // Dashboard summary fetches on page load; reload gives a deterministic sync point.
    cy.reload();
    cy.wait("@getSummary");

    cy.contains("div", /Total Bookings|कुल बुकिंग|ਕੁੱਲ ਬੁਕਿੰਗਾਂ|کل بکنگز/i, { timeout: 15000 })
      .parents("div.rounded-xl")
      .first()
      .find("div.text-xl")
      .should(($el) => {
        const widgetCount = parseCount($el.text());
        expect(widgetCount).to.equal(baselineCount + 1);
      });
  });
});
