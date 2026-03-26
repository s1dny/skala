"""
Scraper for 27crags.com — fetches crags, routes, and ascents.

Strategy:
  1. Fetch /crags page → embedded JSON array of all crags
  2. For each target crag, fetch /crags/{slug}/routelist → parse route table
  3. For each route with ascents, fetch route page + /more endpoint → parse ticks
  4. Climber list is built organically from ascent data

Uses httpx (network requests) first; falls back to playwright if blocked.
"""

import json
import re
import time
import random

import httpx
from bs4 import BeautifulSoup
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn
from rich.panel import Panel

from skala.db import (
    get_connection,
    upsert_climber,
    upsert_route,
    insert_ascent,
    get_progress,
    set_progress,
)

BASE_URL = "https://27crags.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

TICK_TYPE_MAP = {
    "onsight": "onsight",
    "on sight": "onsight",
    "flash": "flash",
    "red point": "redpoint",
    "redpoint": "redpoint",
    "toprope": "toprope",
    "top rope": "toprope",
    "solo": "solo",
    "attempt": "attempt",
    "tick": "tick",
}

console = Console()


def _parse_grade_code(code: str) -> str | None:
    """Parse 27crags grade code like '5006B' → '6B', '7007A' → '7A'.

    The code is a numeric sort key followed by the display grade.
    Strip leading digits that form the sort prefix.
    """
    if not code:
        return None
    # The pattern is: 3-digit sort prefix + display grade
    # e.g. 700 + "7A", 500 + "6B", 400 + "6A"
    match = re.match(r'^\d{3}(.+)$', code)
    if match:
        return match.group(1)
    # If it already looks like a grade, return as-is
    if re.match(r'^[0-9VvBb]', code):
        return code
    return code


def _polite_sleep(low: float = 1.0, high: float = 3.0):
    time.sleep(random.uniform(low, high))


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

def _make_client() -> httpx.Client:
    return httpx.Client(
        headers=HEADERS,
        follow_redirects=True,
        timeout=30.0,
    )


def _get(client: httpx.Client, url: str) -> httpx.Response:
    """GET with basic retry."""
    for attempt in range(3):
        try:
            resp = client.get(url)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                console.print(f"  [yellow]Rate limited, waiting {wait}s...[/yellow]")
                time.sleep(wait)
                continue
            console.print(f"  [red]HTTP {resp.status_code}[/red] for {url}")
            return resp
        except httpx.RequestError as e:
            console.print(f"  [red]Request error (attempt {attempt + 1}):[/red] {e}")
            time.sleep(5)
    raise RuntimeError(f"Failed to fetch {url} after 3 attempts")


# ---------------------------------------------------------------------------
# Crags
# ---------------------------------------------------------------------------

def fetch_all_crags(client: httpx.Client) -> list[dict]:
    """Fetch the full crag list from the /crags page (embedded JSON)."""
    resp = _get(client, f"{BASE_URL}/crags")
    # The crags page embeds a JSON array in a script or data attribute
    # Look for the JSON array of crag objects
    text = resp.text

    # The crags page embeds JSON like: "crags":[{"id":1,"name":"...","boulder_count":...}, ...]
    match = re.search(r'"crags"\s*:\s*(\[\s*\{.*?\}\s*\])', text, re.DOTALL)
    if match:
        try:
            crags = json.loads(match.group(1))
            # Add param_id from the HTML links if not present
            if crags and "param_id" not in crags[0]:
                _enrich_crags_with_slugs(crags, text)
            console.print(f"  Found [green]{len(crags)}[/green] crags from embedded JSON")
            return crags
        except json.JSONDecodeError:
            pass

    # Fallback: parse the page HTML for crag links
    console.print("  [yellow]No embedded JSON found, parsing crag links from HTML...[/yellow]")
    soup = BeautifulSoup(text, "html.parser")
    crags = []
    for link in soup.select('a[href*="/crags/"]'):
        href = link.get("href", "")
        slug_match = re.match(r'/crags/([a-z0-9-]+)$', href)
        if slug_match:
            slug = slug_match.group(1)
            name = link.get_text(strip=True)
            if name and slug not in ("new",):
                crags.append({"param_id": slug, "name": name})

    # Deduplicate
    seen = set()
    unique = []
    for c in crags:
        if c["param_id"] not in seen:
            seen.add(c["param_id"])
            unique.append(c)

    console.print(f"  Found [green]{len(unique)}[/green] crags from HTML links")
    return unique


def _name_to_slug(name: str) -> str:
    """Derive a URL slug from a crag name: 'Magic Wood' → 'magic-wood'."""
    import unicodedata
    # Transliterate unicode to ASCII (ö→o, ä→a, etc.)
    slug = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    slug = slug.lower().strip()
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    return slug


def _enrich_crags_with_slugs(crags: list[dict], page_html: str):
    """Add param_id (URL slug) to crags from the embedded JSON.

    Strategy: derive slug from name. Numeric IDs work as URLs but often
    show a parent area with fewer routes. Text slugs get the full route list.
    Falls back to numeric ID if the name produces an empty slug.
    """
    for crag in crags:
        name = crag.get("name", "")
        slug = _name_to_slug(name)
        crag["param_id"] = slug if slug else str(crag.get("id", ""))
        crag["_id_fallback"] = str(crag.get("id", ""))


def list_crags(min_boulders: int = 50, limit: int = 50) -> list[dict]:
    """Discover crags with boulder routes. Returns sorted by boulder count."""
    client = _make_client()
    all_crags = fetch_all_crags(client)
    client.close()
    filtered = filter_crags(all_crags, min_boulders)
    return filtered[:limit]


def filter_crags(crags: list[dict], min_boulders: int = 50) -> list[dict]:
    """Filter crags to those with enough boulder routes."""
    filtered = []
    for c in crags:
        boulder_count = c.get("boulder_count", 0)
        if boulder_count and int(boulder_count) >= min_boulders:
            filtered.append(c)
    # Sort by boulder count descending
    filtered.sort(key=lambda c: int(c.get("boulder_count", 0)), reverse=True)
    return filtered


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def scrape_routelist(client: httpx.Client, crag_slug: str) -> list[dict]:
    """Scrape the route list for a crag."""
    url = f"{BASE_URL}/crags/{crag_slug}/routelist"
    resp = _get(client, url)
    if resp.status_code != 200:
        console.print(f"  [red]Failed to fetch routelist for {crag_slug}[/red]")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    routes = []

    # Route list table: columns are [name/info, grade_code, type, ascents, rating, ...]
    for row in soup.select("tr"):
        link = row.select_one('a[href*="/routes/"]')
        if not link:
            continue

        href = link.get("href", "")
        route_slug_match = re.search(r'/crags/[^/]+/routes/([^/?#]+)', href)
        if not route_slug_match:
            continue

        route_slug = route_slug_match.group(1)
        route_name = link.get_text(strip=True)

        tds = row.select("td.hidden-xs")
        grade = None
        route_type = None
        ascent_count = 0

        if len(tds) >= 3:
            # td[0] = grade code (e.g. "5006B"), td[1] = type, td[2] = ascent count
            grade_code = tds[0].get_text(strip=True)
            # Extract display grade: strip leading numeric prefix (sort key)
            grade = _parse_grade_code(grade_code)
            route_type = tds[1].get_text(strip=True)
            try:
                ascent_count = int(tds[2].get_text(strip=True))
            except ValueError:
                pass

        routes.append({
            "slug": route_slug,
            "name": route_name,
            "grade": grade,
            "type": route_type,
            "crag_slug": crag_slug,
            "ascent_count": ascent_count,
        })

    return routes


# ---------------------------------------------------------------------------
# Ascents
# ---------------------------------------------------------------------------

def _parse_ascent_html(html: str, crag_slug: str, route_slug: str, route_name: str, grade: str | None) -> list[dict]:
    """Parse ascent rows from HTML (works for both route page and /more endpoint)."""
    soup = BeautifulSoup(html, "html.parser")
    ascents = []

    # Look for ascent rows — multiple possible structures
    rows = soup.select(".result-row, .ascent-row, .tick-row, tr.ascent, .ascent")
    if not rows:
        # Try broader: any element with climber links and ascent type info
        rows = soup.select("[class*='ascent'], [class*='tick']")
    if not rows:
        # Broadest: look for links to climber profiles within structured containers
        rows = soup.select("li, tr, .row")

    for row in rows:
        # Find climber
        climber_link = row.select_one('a[href*="/climbers/"]')
        if not climber_link:
            continue

        href = climber_link.get("href", "")
        username_match = re.search(r'/climbers/([^/?#]+)', href)
        if not username_match:
            continue
        username = username_match.group(1)

        # Find tick type
        tick_type = None
        type_el = row.select_one(".ascent-type, .tick-type, [class*='ascent-type']")
        if type_el:
            raw = type_el.get_text(strip=True).lower()
            tick_type = TICK_TYPE_MAP.get(raw)

        if not tick_type:
            # Check title attributes
            for el in row.select("[title]"):
                title = el.get("title", "").strip().lower()
                if title in TICK_TYPE_MAP:
                    tick_type = TICK_TYPE_MAP[title]
                    break

        if not tick_type:
            # Check text content for known tick types
            text = row.get_text(" ", strip=True).lower()
            for key in TICK_TYPE_MAP:
                if key in text:
                    tick_type = TICK_TYPE_MAP[key]
                    break

        # Find date
        date = None
        date_el = row.select_one(".date, time, [class*='date']")
        if date_el:
            date = date_el.get_text(strip=True)
            # Normalize date format
            if not re.match(r'\d{4}-\d{2}-\d{2}', date):
                date = None

        route_id = f"27c:{crag_slug}/{route_slug}"

        ascents.append({
            "username": username,
            "route_id": route_id,
            "route_name": route_name,
            "grade": grade,
            "tick_type": tick_type,
            "date": date,
        })

    return ascents


def scrape_route_ascents(client: httpx.Client, crag_slug: str, route_slug: str,
                         route_name: str, grade: str | None) -> list[dict]:
    """Scrape all ascents for a single route."""
    url = f"{BASE_URL}/crags/{crag_slug}/routes/{route_slug}"
    resp = _get(client, url)
    if resp.status_code != 200:
        return []

    ascents = _parse_ascent_html(resp.text, crag_slug, route_slug, route_name, grade)

    # Try the /more endpoint for additional ascents
    more_url = f"{url}/more"
    try:
        more_resp = _get(client, more_url)
        if more_resp.status_code == 200:
            try:
                data = more_resp.json()
                ticks_html = data.get("ticks", "")
                if ticks_html:
                    more_ascents = _parse_ascent_html(ticks_html, crag_slug, route_slug, route_name, grade)
                    # Merge, avoiding duplicates
                    existing = {(a["username"], a["date"]) for a in ascents}
                    for a in more_ascents:
                        if (a["username"], a["date"]) not in existing:
                            ascents.append(a)
            except (json.JSONDecodeError, ValueError):
                pass
    except RuntimeError:
        pass

    return ascents


# ---------------------------------------------------------------------------
# Playwright fallback
# ---------------------------------------------------------------------------

def _scrape_with_playwright(crag_slugs: list[str], conn, debug: bool):
    """Fallback: use headless browser if HTTP requests fail."""
    from playwright.sync_api import sync_playwright

    console.print("[yellow]Falling back to Playwright browser...[/yellow]")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 720},
            locale="en-US",
        )
        page = context.new_page()

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            crag_task = progress.add_task("Crags (playwright)", total=len(crag_slugs))

            for crag_slug in crag_slugs:
                progress_key = f"crag:{crag_slug}"
                if get_progress(conn, progress_key) == "done":
                    progress.update(crag_task, advance=1)
                    continue

                progress.update(crag_task, description=f"[playwright] {crag_slug}")

                # Fetch route list
                page.goto(f"{BASE_URL}/crags/{crag_slug}/routelist", wait_until="domcontentloaded")
                page.wait_for_timeout(3000)
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")

                routes = []
                for row in soup.select("tr"):
                    link = row.select_one('a[href*="/routes/"]')
                    if not link:
                        continue
                    href = link.get("href", "")
                    slug_match = re.search(r'/crags/[^/]+/routes/([^/?#]+)', href)
                    if slug_match:
                        route_slug = slug_match.group(1)
                        route_name = link.get_text(strip=True)
                        grade = None
                        grade_el = row.select_one(".grade, td:nth-child(2)")
                        if grade_el:
                            gt = grade_el.get_text(strip=True)
                            if re.match(r'^[0-9VvBb]', gt):
                                grade = gt
                        routes.append((route_slug, route_name, grade))

                route_task = progress.add_task(f"  Routes in {crag_slug}", total=len(routes))

                for route_slug, route_name, grade in routes:
                    route_id = f"27c:{crag_slug}/{route_slug}"
                    upsert_route(conn, route_id, route_name, grade)

                    page.goto(f"{BASE_URL}/crags/{crag_slug}/routes/{route_slug}",
                              wait_until="domcontentloaded")
                    page.wait_for_timeout(2000)
                    route_html = page.content()

                    ascents = _parse_ascent_html(route_html, crag_slug, route_slug, route_name, grade)
                    for a in ascents:
                        upsert_climber(conn, a["username"])
                        insert_ascent(conn, a["username"], a["route_id"],
                                      a["route_name"], a["grade"], a["tick_type"], a["date"])

                    progress.update(route_task, advance=1)
                    _polite_sleep(1.0, 2.0)

                conn.commit()
                set_progress(conn, progress_key, "done")
                progress.update(crag_task, advance=1)

        browser.close()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape(
    crag_slugs: list[str] | None = None,
    min_boulders: int = 50,
    max_crags: int = 10,
    debug: bool = False,
):
    """Main scrape function.

    Args:
        crag_slugs: Specific crag slugs to scrape. If None, auto-discover.
        min_boulders: Minimum boulder count to include a crag (for auto-discover).
        max_crags: Maximum number of crags to scrape (for auto-discover).
        debug: Print extra debug info.
    """
    conn = get_connection()
    client = _make_client()
    use_playwright = False

    # Step 1: Determine which crags to scrape
    if not crag_slugs:
        with console.status("[bold cyan]Discovering crags..."):
            try:
                all_crags = fetch_all_crags(client)
                if all_crags and all_crags[0].get("boulder_count"):
                    target_crags = filter_crags(all_crags, min_boulders)[:max_crags]
                else:
                    target_crags = all_crags[:max_crags]
                crag_slugs = [c["param_id"] for c in target_crags]
                console.print(f"  Selected [green]{len(crag_slugs)}[/green] crags: {', '.join(crag_slugs[:5])}...")
            except Exception as e:
                console.print(f"[red]Failed to discover crags via HTTP:[/red] {e}")
                client.close()
                return
    else:
        console.print(f"Scraping [green]{len(crag_slugs)}[/green] specified crags")

    # Step 2: For each crag, scrape routes and ascents
    total_routes = 0
    total_ascents = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        crag_task = progress.add_task("Scraping crags", total=len(crag_slugs))

        for i, crag_slug in enumerate(crag_slugs):
            progress_key = f"crag:{crag_slug}"
            if get_progress(conn, progress_key) == "done":
                progress.update(crag_task, description=f"[dim]{crag_slug} — skipped (done)[/dim]", advance=1)
                continue

            progress.update(crag_task, description=f"Crag [cyan]{crag_slug}[/cyan]")

            # Fetch route list
            try:
                routes = scrape_routelist(client, crag_slug)
            except Exception as e:
                console.print(f"  [red]HTTP failed for {crag_slug}:[/red] {e}")
                use_playwright = True
                break

            if not routes:
                progress.update(crag_task, description=f"[dim]{crag_slug} — no routes[/dim]", advance=1)
                set_progress(conn, progress_key, "done")
                _polite_sleep()
                continue

            # Store routes and scrape ascents
            crag_ascent_count = 0
            route_task = progress.add_task(f"  Routes in {crag_slug}", total=len(routes))

            for j, route in enumerate(routes):
                route_id = f"27c:{crag_slug}/{route['slug']}"
                upsert_route(conn, route_id, route["name"], route["grade"])

                try:
                    ascents = scrape_route_ascents(
                        client, crag_slug, route["slug"], route["name"], route["grade"]
                    )
                except Exception as e:
                    console.print(f"    [red]Failed to fetch ascents for {route['name']}:[/red] {e}")
                    use_playwright = True
                    break

                for a in ascents:
                    upsert_climber(conn, a["username"])
                    insert_ascent(
                        conn, a["username"], a["route_id"],
                        a["route_name"], a["grade"], a["tick_type"], a["date"],
                    )
                    crag_ascent_count += 1

                # Be polite — don't hammer the server
                if j % 10 == 9:
                    conn.commit()
                _polite_sleep(0.5, 1.5)
                progress.update(route_task, advance=1)

            # Remove the per-crag route task once done
            progress.remove_task(route_task)

            if use_playwright:
                break

            conn.commit()
            set_progress(conn, progress_key, "done")
            total_routes += len(routes)
            total_ascents += crag_ascent_count
            progress.update(crag_task, advance=1)
            _polite_sleep(2.0, 4.0)

    client.close()

    # Step 3: Playwright fallback for remaining crags
    if use_playwright:
        remaining = [s for s in crag_slugs if get_progress(conn, f"crag:{s}") != "done"]
        if remaining:
            console.print(f"\n[yellow]Falling back to Playwright for {len(remaining)} remaining crags...[/yellow]")
            _scrape_with_playwright(remaining, conn, debug)

    # Summary
    climber_count = conn.execute("SELECT COUNT(*) FROM climbers").fetchone()[0]
    route_count = conn.execute("SELECT COUNT(*) FROM routes").fetchone()[0]
    ascent_count = conn.execute("SELECT COUNT(*) FROM ascents").fetchone()[0]

    console.print()
    console.print(Panel(
        f"[green]{climber_count}[/green] climbers  ·  [green]{route_count}[/green] routes  ·  [green]{ascent_count}[/green] ascents",
        title="[bold]Scraping Complete",
        border_style="green",
    ))

    conn.close()
