import click
from rich.console import Console
from rich.table import Table

from skala.db import get_connection
from skala.scraper import scrape as run_scrape, list_crags
from skala.elo import calculate_elos

console = Console()


@click.group()
def main():
    """Skala — ELO ratings for rock climbing."""
    pass


@main.command()
@click.option("--crags", default=None, help="Comma-separated crag slugs (e.g. fontainebleau,magic-wood-15945)")
@click.option("--max-crags", default=10, help="Max crags to auto-discover if none specified")
@click.option("--min-boulders", default=50, help="Min boulder count for auto-discovery")
@click.option("--workers", default=6, help="Number of threads for parallel route scraping")
@click.option("--debug", is_flag=True, help="Extra debug output")
def scrape(crags, max_crags, min_boulders, workers, debug):
    """Scrape climbing data from 27crags.com."""
    crag_slugs = None
    if crags:
        crag_slugs = [s.strip() for s in crags.split(",") if s.strip()]

    run_scrape(
        crag_slugs=crag_slugs,
        max_crags=max_crags,
        min_boulders=min_boulders,
        workers=workers,
        debug=debug,
    )


@main.command()
@click.option("--min-boulders", default=50, help="Min boulder count to show")
@click.option("--limit", default=50, help="Number of crags to show")
def crags(min_boulders, limit):
    """List crags with boulder routes from 27crags.com."""
    with console.status("[bold cyan]Fetching crag list..."):
        results = list_crags(min_boulders=min_boulders, limit=limit)

    if not results:
        console.print("[yellow]No crags found matching criteria.[/yellow]")
        return

    table = Table(title=f"Top {len(results)} Boulder Crags", show_lines=False)
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Slug", style="cyan", max_width=35)
    table.add_column("Name", style="bold white", max_width=30)
    table.add_column("Boulders", style="green", justify="right")

    for i, c in enumerate(results, 1):
        name = c.get("name", "?")[:28]
        slug = c.get("param_id", "?")[:33]
        boulders = str(c.get("boulder_count", 0))
        table.add_row(str(i), slug, name, boulders)

    console.print()
    console.print(table)


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
            "SELECT username, elo, rd, matches FROM climbers WHERE matches > 0 ORDER BY elo DESC LIMIT ?",
            (limit,),
        ).fetchall()

        if not rows:
            console.print("[yellow]No climber rankings yet. Run 'skala calculate' first.[/yellow]")
            conn.close()
            return

        table = Table(title=f"Top {len(rows)} Climbers by Rating")
        table.add_column("Rank", style="dim", width=5, justify="right")
        table.add_column("Climber", style="bold cyan", max_width=30)
        table.add_column("Rating", style="bold green", justify="right")
        table.add_column("\u00b1RD", style="dim", justify="right")
        table.add_column("Matches", style="white", justify="right")

        for i, row in enumerate(rows, 1):
            table.add_row(str(i), row["username"], f"{row['elo']:.0f}", f"{row['rd']:.0f}", str(row["matches"]))

    else:
        rows = conn.execute(
            "SELECT route_id, name, grade, elo, rd, matches FROM routes WHERE matches > 0 ORDER BY elo DESC LIMIT ?",
            (limit,),
        ).fetchall()

        if not rows:
            console.print("[yellow]No route rankings yet. Run 'skala calculate' first.[/yellow]")
            conn.close()
            return

        table = Table(title=f"Top {len(rows)} Routes by Rating")
        table.add_column("Rank", style="dim", width=5, justify="right")
        table.add_column("Route", style="bold cyan", max_width=35)
        table.add_column("Grade", style="yellow", width=8, justify="center")
        table.add_column("Rating", style="bold green", justify="right")
        table.add_column("\u00b1RD", style="dim", justify="right")
        table.add_column("Matches", style="white", justify="right")

        for i, row in enumerate(rows, 1):
            name = (row["name"] or "Unknown")[:33]
            grade = row["grade"] or "?"
            table.add_row(str(i), name, grade, f"{row['elo']:.0f}", f"{row['rd']:.0f}", str(row["matches"]))

    console.print()
    console.print(table)
    conn.close()
