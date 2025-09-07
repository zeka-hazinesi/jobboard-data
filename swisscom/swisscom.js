// swisscom.playwright.js
import { chromium } from "playwright";
import fs from "fs";

const START_URL =
  "https://swisscom.wd103.myworkdayjobs.com/de-DE/SwisscomExternalCareers";
const API_URL =
  "https://swisscom.wd103.myworkdayjobs.com/wday/cxs/swisscom/SwisscomExternalCareers/jobs";
const LIMIT = 100;

function flatten(p) {
  return {
    id: p?.id ?? null,
    title: p?.title ?? null,
    locations:
      p?.locations
        ?.map((l) => l?.city || l?.descriptor || l)
        ?.filter(Boolean) ?? [],
    jobFamily: p?.jobFamily?.descriptor ?? null,
    timeType: p?.timeType ?? null,
    postedOn: p?.postedOn ?? p?.startDate ?? null,
    url: p?.externalPath
      ? `https://swisscom.wd103.myworkdayjobs.com${p.externalPath}`
      : null,
  };
}

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    locale: "de-DE",
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
  });
  const page = await context.newPage();

  try {
    // 1) Load careers page to obtain valid cookies and the CSRF token cookie
    await page.goto(START_URL, { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle");

    // 2) Extract CSRF token from cookie jar
    const cookies = await context.cookies();
    const csrfCookie = cookies.find((c) => c.name === "CALYPSO_CSRF_TOKEN");
    if (!csrfCookie?.value) {
      throw new Error(
        "Could not read CALYPSO_CSRF_TOKEN cookie. Are you being blocked or offline?"
      );
    }
    const csrf = csrfCookie.value;

    // 3) Helper to call the Workday API using the SAME context (shares cookies)
    async function fetchPage(offset) {
      const res = await context.request.post(API_URL, {
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          "x-calypso-csrf-token": csrf,
          origin: "https://swisscom.wd103.myworkdayjobs.com",
          referer: START_URL,
        },
        data: {
          appliedFacets: {},
          limit: LIMIT,
          offset,
          searchText: "",
        },
        timeout: 30000,
      });

      if (!res.ok()) {
        const text = await res.text();
        throw new Error(`HTTP ${res.status()} ${res.statusText()}\n${text}`);
      }
      return res.json();
    }

    // 4) Fetch all pages (100 at a time)
    let offset = 0;
    const all = [];
    while (true) {
      const pageJson = await fetchPage(offset);
      const items = pageJson?.jobPostings ?? [];
      console.log(`Fetched ${items.length} jobs at offset ${offset}`);
      all.push(...items);
      if (items.length < LIMIT) break;
      offset += LIMIT;
      // be polite
      await new Promise((r) => setTimeout(r, 400));
    }

    // 5) Save
    fs.writeFileSync("swisscom_jobs_raw.json", JSON.stringify(all, null, 2));
    fs.writeFileSync(
      "swisscom_jobs_flat.json",
      JSON.stringify(all.map(flatten), null, 2)
    );
    console.log(`Done. Saved ${all.length} postings.`);
  } catch (err) {
    console.error("Playwright scrape failed:", err.message);
    process.exitCode = 1;
  } finally {
    await browser.close();
  }
})();
