import click
from rich.console import Console
from rich.table import Table

from skala.db import get_connection
from skala.scraper import scrape as run_scrape, list_crags
from skala.elo import calculate_elos

console = Console()


def _extract_crag_slug(route_id: str | None) -> str:
    """Extract the crag slug from a route id like '27c:magic-wood/foo'."""
    if not route_id or ":" not in route_id or "/" not in route_id:
        return "?"
    return route_id.split(":", 1)[1].split("/", 1)[0] or "?"


def _normalize_climber_arg(climber: str | None) -> str | None:
    """Validate raw climber CLI input as an exact username."""
    if not climber:
        return None

    username = climber.strip()
    if not username:
        raise click.BadParameter("Climber cannot be empty.", param_hint="--climber")

    if "/" in username or ":" in username:
        raise click.BadParameter("Use the exact climber username only, not a URL.", param_hint="--climber")

    return username


@click.group()
def main():
    """Skala — ELO ratings for rock climbing."""
    pass


@main.command()
@click.option("--crags", help="Number of top crags to auto-discover, or comma-separated slugs")
@click.option("--climber", help="Exact climber username to scrape directly")
@click.option("--sort", type=click.Choice(["boulders", "likes"]), default="boulders", help="Sort order for auto-discovery")
@click.option("--workers", default=6, help="Number of threads for parallel route scraping")
@click.option("--debug", is_flag=True, help="Extra debug output")
def scrape(crags, climber, sort, workers, debug):
    """Scrape climbing data from 27crags.com."""
    climber_slug = _normalize_climber_arg(climber)

    if climber_slug:
        if crags:
            raise click.UsageError("--crags and --climber cannot be used together.")
        run_scrape(climber_slug=climber_slug, sort=sort, workers=workers, debug=debug)
        return

    crags = crags or "10"
    if crags.isdigit():
        run_scrape(max_crags=int(crags), sort=sort, workers=workers, debug=debug)
    else:
        crag_slugs = [s.strip() for s in crags.split(",") if s.strip()]
        run_scrape(crag_slugs=crag_slugs, sort=sort, workers=workers, debug=debug)


@main.command()
@click.option("--limit", default=50, help="Number of crags to show")
@click.option("--sort", type=click.Choice(["boulders", "likes"]), default="boulders", help="Sort crags by this field")
def crags(limit, sort):
    """List crags with boulder routes from 27crags.com."""
    with console.status("[bold cyan]Fetching crag list..."):
        results = list_crags(limit=limit, sort=sort)

    if not results:
        console.print("[yellow]No crags found matching criteria.[/yellow]")
        return

    table = Table(title=f"Top {len(results)} Boulder Crags (sorted by {sort})", show_lines=False)
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Slug", style="cyan", max_width=35)
    table.add_column("Name", style="bold white", max_width=30)
    table.add_column("Boulders", style="green", justify="right")
    table.add_column("Likes", style="magenta", justify="right")

    for i, c in enumerate(results, 1):
        name = c.get("name", "?")[:28]
        slug = c.get("param_id", "?")[:33]
        boulders = str(c.get("boulder_count", 0))
        likes = str(c.get("likes_count", 0))
        table.add_row(str(i), slug, name, boulders, likes)

    console.print()
    console.print(table)


@main.command()
def calculate():
    """Run ELO calculations on all scraped ascents."""
    calculate_elos()


@main.command()
@click.option("--type", "entity_type", type=click.Choice(["climbers", "routes"]))
@click.option("--climber", help="Show the ranking for a single exact climber username")
@click.option("--limit", default=50, help="Number of results to show")
def rankings(entity_type, climber, limit):
    """Display ELO rankings."""
    climber_slug = _normalize_climber_arg(climber)
    entity_type = entity_type or ("climbers" if climber_slug else "routes")

    if climber_slug and entity_type != "climbers":
        raise click.UsageError("--climber can only be used with climber rankings.")

    conn = get_connection()

    if entity_type == "climbers":
        if climber_slug:
            rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY elo DESC) AS rank,
                        username,
                        elo,
                        rd,
                        matches
                    FROM climbers
                    WHERE matches > 0
                )
                SELECT rank, username, elo, rd, matches
                FROM ranked
                WHERE username = ?
                """,
                (climber_slug,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT username, elo, rd, matches FROM climbers WHERE matches > 0 ORDER BY elo DESC LIMIT ?",
                (limit,),
            ).fetchall()

        if not rows:
            if climber_slug:
                console.print(f"[yellow]No ranking found for climber '{climber_slug}'. Run 'skala calculate' after scraping them.[/yellow]")
            else:
                console.print("[yellow]No climber rankings yet. Run 'skala calculate' first.[/yellow]")
            conn.close()
            return

        table = Table(title=f"Ranking for {rows[0]['username']}" if climber_slug else f"Top {len(rows)} Climbers by Rating")
        table.add_column("Rank", style="dim", width=5, justify="right")
        table.add_column("Climber", style="bold cyan", max_width=30)
        table.add_column("Rating", style="bold green", justify="right")
        table.add_column("\u00b1RD", style="dim", justify="right")
        table.add_column("Matches", style="white", justify="right")

        for i, row in enumerate(rows, 1):
            rank = row["rank"] if climber_slug else i
            table.add_row(str(rank), row["username"], f"{row['elo']:.0f}", f"{row['rd']:.0f}", str(row["matches"]))

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
        table.add_column("Crag", style="magenta", max_width=24)
        table.add_column("Grade", style="yellow", width=8, justify="center")
        table.add_column("Rating", style="bold green", justify="right")
        table.add_column("\u00b1RD", style="dim", justify="right")
        table.add_column("Matches", style="white", justify="right")

        for i, row in enumerate(rows, 1):
            name = (row["name"] or "Unknown")[:33]
            crag = _extract_crag_slug(row["route_id"])
            grade = row["grade"] or "?"
            table.add_row(str(i), name, crag, grade, f"{row['elo']:.0f}", f"{row['rd']:.0f}", str(row["matches"]))

    console.print()
    console.print(table)
    conn.close()
