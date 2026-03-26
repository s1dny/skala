"""Glicko-2 rating system for climbers vs routes.

Implements the algorithm from Mark Glickman's paper:
http://www.glicko.net/glicko/glicko2.pdf

Ascents are grouped into daily rating periods. RD decay is scaled
proportionally to elapsed time (calibrated to 30-day reference periods)
so that daily granularity doesn't inflate uncertainty.
"""

import math
from collections import defaultdict
from datetime import date as dt_date

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn
from rich.panel import Panel

from skala.db import get_connection

# Glicko-2 constants
INITIAL_RATING = 1500.0
INITIAL_RD = 350.0
INITIAL_VOL = 0.06
TAU = 0.5  # system constant — constrains volatility change
CONVERGENCE_TOL = 1e-6
GLICKO2_SCALE = 173.7178  # conversion factor between Glicko and Glicko-2 scales
REFERENCE_PERIOD_DAYS = 30.0  # RD decay is calibrated to this period length

CLIMBER_WINS_TICK_TYPES = {"flash", "onsight"}

console = Console()


def _to_g2(rating: float) -> float:
    """Glicko rating -> Glicko-2 mu."""
    return (rating - INITIAL_RATING) / GLICKO2_SCALE


def _to_g2_rd(rd: float) -> float:
    """Glicko RD -> Glicko-2 phi."""
    return rd / GLICKO2_SCALE


def _from_g2(mu: float) -> float:
    """Glicko-2 mu -> Glicko rating."""
    return mu * GLICKO2_SCALE + INITIAL_RATING


def _from_g2_rd(phi: float) -> float:
    """Glicko-2 phi -> Glicko RD."""
    return phi * GLICKO2_SCALE


def _g(phi: float) -> float:
    """Glicko-2 g function — reduces impact of uncertain opponents."""
    return 1.0 / math.sqrt(1.0 + 3.0 * phi * phi / (math.pi * math.pi))


def _E(mu: float, mu_j: float, phi_j: float) -> float:
    """Expected score of entity with rating mu against opponent mu_j."""
    return 1.0 / (1.0 + math.exp(-_g(phi_j) * (mu - mu_j)))


def _update_volatility(sigma: float, phi: float, v: float, delta: float) -> float:
    """Compute new volatility using the Illinois algorithm (Step 5 of Glicko-2)."""
    a = math.log(sigma * sigma)
    tau2 = TAU * TAU

    def f(x: float) -> float:
        ex = math.exp(x)
        d2 = delta * delta
        phi2 = phi * phi
        top = ex * (d2 - phi2 - v - ex)
        bottom = 2.0 * (phi2 + v + ex) ** 2
        return top / bottom - (x - a) / tau2

    # Set initial bounds
    A = a
    if delta * delta > phi * phi + v:
        B = math.log(delta * delta - phi * phi - v)
    else:
        k = 1
        while f(a - k * TAU) < 0:
            k += 1
        B = a - k * TAU

    fA = f(A)
    fB = f(B)

    # Illinois algorithm — bisection variant that converges faster
    for _ in range(50):
        if abs(B - A) < CONVERGENCE_TOL:
            break
        C = A + (A - B) * fA / (fB - fA)
        fC = f(C)
        if fC * fB <= 0:
            A = B
            fA = fB
        else:
            fA /= 2.0
        B = C
        fB = fC

    return math.exp(A / 2.0)


def _apply_rd_decay(phi: float, sigma: float, days_elapsed: float) -> float:
    """Increase RD proportionally to time elapsed since last activity.

    Standard Glicko-2 applies phi* = sqrt(phi² + sigma²) per reference period.
    We scale sigma² by (days / reference_days) so daily periods don't over-inflate.
    """
    if days_elapsed <= 0:
        return phi
    time_scale = days_elapsed / REFERENCE_PERIOD_DAYS
    phi_star = math.sqrt(phi * phi + time_scale * sigma * sigma)
    # Cap at initial RD — can't be more uncertain than knowing nothing
    return min(phi_star, _to_g2_rd(INITIAL_RD))


def _glicko2_update(
    mu: float, phi: float, sigma: float,
    opponents: list[tuple[float, float, float]],
    days_elapsed: float = REFERENCE_PERIOD_DAYS,
) -> tuple[float, float, float]:
    """Run one Glicko-2 rating period update.

    opponents: list of (mu_j, phi_j, score) for each match in the period.
    days_elapsed: days since this entity's last activity (scales RD decay).
    Returns (new_mu, new_phi, new_sigma).
    """
    # Apply time-scaled RD decay
    phi = _apply_rd_decay(phi, sigma, days_elapsed)

    if not opponents:
        return mu, phi, sigma

    # Step 3: compute v (estimated variance of rating based on game outcomes)
    v_inv = 0.0
    for mu_j, phi_j, _ in opponents:
        g_j = _g(phi_j)
        e_j = _E(mu, mu_j, phi_j)
        v_inv += g_j * g_j * e_j * (1.0 - e_j)
    v = 1.0 / v_inv

    # Step 4: compute delta (estimated improvement)
    delta_sum = 0.0
    for mu_j, phi_j, s_j in opponents:
        g_j = _g(phi_j)
        e_j = _E(mu, mu_j, phi_j)
        delta_sum += g_j * (s_j - e_j)
    delta = v * delta_sum

    # Step 5: update volatility
    new_sigma = _update_volatility(sigma, phi, v, delta)

    # Step 6: update RD
    phi_star = math.sqrt(phi * phi + new_sigma * new_sigma)

    # Step 7: update rating and RD
    new_phi = 1.0 / math.sqrt(1.0 / (phi_star * phi_star) + 1.0 / v)
    new_mu = mu + new_phi * new_phi * delta_sum

    return new_mu, new_phi, new_sigma


def calculate_elos():
    conn = get_connection()

    # Load ascents, keeping only the best tick per climber-route pair.
    with console.status("[bold cyan]Loading ascents..."):
        ascents = conn.execute(
            """
            SELECT climber, route_id, tick_type, date FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY climber, route_id
                        ORDER BY
                            CASE tick_type
                                WHEN 'onsight' THEN 0
                                WHEN 'flash'   THEN 1
                                ELSE 2
                            END,
                            date ASC, id ASC
                    ) AS rn
                FROM ascents
            )
            WHERE rn = 1
            ORDER BY date ASC
            """
        ).fetchall()

    if not ascents:
        console.print("[yellow]No ascents found. Run 'skala scrape' first.[/yellow]")
        conn.close()
        return

    # Group ascents into daily rating periods.
    periods: dict[str, list] = defaultdict(list)
    for a in ascents:
        period = a["date"] or "0000-01-01"
        periods[period].append(a)

    # Collect all entity IDs
    climber_ids: set[str] = set()
    route_ids: set[str] = set()
    for a in ascents:
        climber_ids.add(a["climber"])
        route_ids.add(a["route_id"])
    all_entities = climber_ids | route_ids

    # State: (mu, phi, sigma) in Glicko-2 scale
    mu: dict[str, float] = {e: _to_g2(INITIAL_RATING) for e in all_entities}
    phi: dict[str, float] = {e: _to_g2_rd(INITIAL_RD) for e in all_entities}
    sigma: dict[str, float] = {e: INITIAL_VOL for e in all_entities}
    match_counts: dict[str, int] = {e: 0 for e in all_entities}
    last_active: dict[str, dt_date | None] = {e: None for e in all_entities}

    sorted_periods = sorted(periods.keys())

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]Glicko-2 rating"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Rating periods", total=len(sorted_periods))

        for period_key in sorted_periods:
            period_ascents = periods[period_key]
            try:
                period_date = dt_date.fromisoformat(period_key)
            except ValueError:
                period_date = None

            # Build per-entity opponent lists for this period
            period_matches: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
            active: set[str] = set()

            for a in period_ascents:
                climber = a["climber"]
                route = a["route_id"]
                tick_type = a["tick_type"]
                climber_wins = tick_type in CLIMBER_WINS_TICK_TYPES if tick_type else False

                c_score = 1.0 if climber_wins else 0.0
                r_score = 0.0 if climber_wins else 1.0

                period_matches[climber].append((mu[route], phi[route], c_score))
                period_matches[route].append((mu[climber], phi[climber], r_score))
                active.add(climber)
                active.add(route)

                match_counts[climber] += 1
                match_counts[route] += 1

            # Update only active entities — decay is applied lazily based on
            # days since their last activity, not once per calendar day.
            for entity in active:
                if period_date and last_active[entity]:
                    days_elapsed = (period_date - last_active[entity]).days
                else:
                    days_elapsed = REFERENCE_PERIOD_DAYS  # first appearance

                mu[entity], phi[entity], sigma[entity] = _glicko2_update(
                    mu[entity], phi[entity], sigma[entity],
                    period_matches[entity],
                    days_elapsed=days_elapsed,
                )
                last_active[entity] = period_date

            progress.update(task, advance=1)

        # Final RD decay for entities inactive since their last period
        if sorted_periods:
            try:
                final_date = dt_date.fromisoformat(sorted_periods[-1])
            except ValueError:
                final_date = None
            if final_date:
                for entity in all_entities:
                    if last_active[entity] and last_active[entity] < final_date:
                        days_since = (final_date - last_active[entity]).days
                        phi[entity] = _apply_rd_decay(phi[entity], sigma[entity], days_since)

    # Convert to Glicko scale and write to DB
    with console.status("[bold cyan]Writing ratings to database..."):
        conn.execute(f"UPDATE climbers SET elo = {INITIAL_RATING}, rd = {INITIAL_RD}, volatility = {INITIAL_VOL}, matches = 0")
        conn.execute(f"UPDATE routes SET elo = {INITIAL_RATING}, rd = {INITIAL_RD}, volatility = {INITIAL_VOL}, matches = 0")

        for entity in climber_ids:
            conn.execute(
                "UPDATE climbers SET elo = ?, rd = ?, volatility = ?, matches = ? WHERE username = ?",
                (_from_g2(mu[entity]), _from_g2_rd(phi[entity]), sigma[entity], match_counts[entity], entity),
            )
        for entity in route_ids:
            conn.execute(
                "UPDATE routes SET elo = ?, rd = ?, volatility = ?, matches = ? WHERE route_id = ?",
                (_from_g2(mu[entity]), _from_g2_rd(phi[entity]), sigma[entity], match_counts[entity], entity),
            )
        conn.commit()

    conn.close()

    console.print(Panel(
        f"[green]{len(climber_ids)}[/green] climbers  ·  [green]{len(route_ids)}[/green] routes  ·  "
        f"[green]{len(ascents)}[/green] ascents  ·  [green]{len(sorted_periods)}[/green] rating periods",
        title="[bold]Glicko-2 Rating Complete",
        border_style="green",
    ))
