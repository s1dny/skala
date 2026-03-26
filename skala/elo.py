from skala.db import get_connection

STARTING_ELO = 1000.0
K_NEW = 32  # <30 matches
K_ESTABLISHED = 20

CLIMBER_WINS_TICK_TYPES = {"flash", "onsight"}


def _k_factor(matches: int) -> float:
    return K_NEW if matches < 30 else K_ESTABLISHED


def _expected_score(rating_a: float, rating_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((rating_b - rating_a) / 400.0))


def calculate_elos():
    conn = get_connection()

    # Reset all ratings
    conn.execute(f"UPDATE climbers SET elo = {STARTING_ELO}, matches = 0")
    conn.execute(f"UPDATE routes SET elo = {STARTING_ELO}, matches = 0")
    conn.commit()

    # Load all ascents chronologically
    ascents = conn.execute(
        "SELECT climber, route_id, tick_type FROM ascents ORDER BY date ASC, id ASC"
    ).fetchall()

    print(f"Processing {len(ascents)} ascents...")

    # In-memory ratings for speed
    climber_elos: dict[str, float] = {}
    climber_matches: dict[str, int] = {}
    route_elos: dict[str, float] = {}
    route_matches: dict[str, int] = {}

    for ascent in ascents:
        climber = ascent["climber"]
        route_id = ascent["route_id"]
        tick_type = ascent["tick_type"]

        c_elo = climber_elos.get(climber, STARTING_ELO)
        c_matches = climber_matches.get(climber, 0)
        r_elo = route_elos.get(route_id, STARTING_ELO)
        r_matches = route_matches.get(route_id, 0)

        # Climber wins if flash/onsight, route wins otherwise
        climber_wins = tick_type in CLIMBER_WINS_TICK_TYPES if tick_type else False

        e_climber = _expected_score(c_elo, r_elo)
        e_route = 1.0 - e_climber

        k_climber = _k_factor(c_matches)
        k_route = _k_factor(r_matches)

        if climber_wins:
            c_elo += k_climber * (1.0 - e_climber)
            r_elo += k_route * (0.0 - e_route)
        else:
            c_elo += k_climber * (0.0 - e_climber)
            r_elo += k_route * (1.0 - e_route)

        climber_elos[climber] = c_elo
        climber_matches[climber] = c_matches + 1
        route_elos[route_id] = r_elo
        route_matches[route_id] = r_matches + 1

    # Write back to DB
    for username, elo in climber_elos.items():
        conn.execute(
            "UPDATE climbers SET elo = ?, matches = ? WHERE username = ?",
            (elo, climber_matches[username], username),
        )
    for route_id, elo in route_elos.items():
        conn.execute(
            "UPDATE routes SET elo = ?, matches = ? WHERE route_id = ?",
            (elo, route_matches[route_id], route_id),
        )

    conn.commit()
    conn.close()

    print(f"Updated ratings for {len(climber_elos)} climbers and {len(route_elos)} routes")
