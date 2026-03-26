import click

from skala.db import get_connection
from skala.scraper import scrape as run_scrape, list_crags
from skala.elo import calculate_elos


@click.group()
def main():
    """Skala — ELO ratings for rock climbing."""
    pass


@main.command()
@click.option("--crags", default=None, help="Comma-separated crag slugs (e.g. fontainebleau,magic-wood-15945)")
@click.option("--max-crags", default=10, help="Max crags to auto-discover if none specified")
@click.option("--min-boulders", default=50, help="Min boulder count for auto-discovery")
@click.option("--debug", is_flag=True, help="Extra debug output")
def scrape(crags, max_crags, min_boulders, debug):
    """Scrape climbing data from 27crags.com."""
    crag_slugs = None
    if crags:
        crag_slugs = [s.strip() for s in crags.split(",") if s.strip()]

    run_scrape(
        crag_slugs=crag_slugs,
        max_crags=max_crags,
        min_boulders=min_boulders,
        debug=debug,
    )


@main.command()
@click.option("--min-boulders", default=50, help="Min boulder count to show")
@click.option("--limit", default=50, help="Number of crags to show")
def crags(min_boulders, limit):
    """List crags with boulder routes from 27crags.com."""
    results = list_crags(min_boulders=min_boulders, limit=limit)
    print(f"\n{'#':<5}{'Slug':<35}{'Name':<30}{'Boulders':<10}")
    print("-" * 80)
    for i, c in enumerate(results, 1):
        name = c.get("name", "?")[:28]
        slug = c.get("param_id", "?")[:33]
        boulders = c.get("boulder_count", 0)
        print(f"{i:<5}{slug:<35}{name:<30}{boulders}")


@main.command()
def calculate():
    """Run ELO calculations on all scraped ascents."""
    calculate_elos()


@main.command()
@click.option("--type", "entity_type", type=click.Choice(["climbers", "routes"]), default="routes")
@click.option("--limit", default=50, help="Number of results to show")
def rankings(entity_type, limit):
    """Display ELO rankings."""
    conn = get_connection()

    if entity_type == "climbers":
        rows = conn.execute(
            "SELECT username, elo, matches FROM climbers WHERE matches > 0 ORDER BY elo DESC LIMIT ?",
            (limit,),
        ).fetchall()
        print(f"\n{'Rank':<6}{'Climber':<30}{'ELO':<10}{'Matches'}")
        print("-" * 56)
        for i, row in enumerate(rows, 1):
            print(f"{i:<6}{row['username']:<30}{row['elo']:<10.1f}{row['matches']}")
    else:
        rows = conn.execute(
            "SELECT route_id, name, grade, elo, matches FROM routes WHERE matches > 0 ORDER BY elo DESC LIMIT ?",
            (limit,),
        ).fetchall()
        print(f"\n{'Rank':<6}{'Route':<35}{'Grade':<10}{'ELO':<10}{'Matches'}")
        print("-" * 71)
        for i, row in enumerate(rows, 1):
            name = (row["name"] or "Unknown")[:33]
            grade = row["grade"] or "?"
            print(f"{i:<6}{name:<35}{grade:<10}{row['elo']:<10.1f}{row['matches']}")

    conn.close()
