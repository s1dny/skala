# Skala

Two-sided ELO rating system for rock climbing. Every ascent is a match between climber and route — flash it, and you win; struggle on it, and the route wins. Over time, both climbers and routes converge on accurate ratings.

Data sourced from [27crags.com](https://27crags.com).

## Install

Requires Python 3.13+.

```
uv sync
```

## Usage

### Discover crags

```
skala crags --min-boulders 100 --limit 20
```

Lists crags from 27crags.com sorted by boulder count, showing the slug you need for scraping.

### Scrape

```
# Scrape specific crags by slug
skala scrape --crags magic-wood,cresciano,fontainebleau

# Auto-discover and scrape top crags by boulder count
skala scrape --max-crags 5 --min-boulders 200
```

Fetches route lists and all logged ascents for each crag. Progress is tracked per-crag, so interrupted scrapes resume where they left off. Uses HTTP requests by default; falls back to a Playwright browser automatically if needed.

### Calculate ELO

```
skala calculate
```

Processes all ascents chronologically and updates ELO ratings for every climber and route.

### View rankings

```
skala rankings --type routes --limit 20
skala rankings --type climbers --limit 20
```

## How ELO works here

Each ascent is treated as a head-to-head match:

- **Flash or onsight** = climber wins (climber gains ELO, route loses)
- **Everything else** (redpoint, toprope, etc.) = route wins (route gains ELO, climber loses)

The logic: almost nobody logs failed attempts, so flash vs. non-flash is the strongest available signal for whether the climber found the route easy or hard.

K-factors: 32 for new entities (<30 matches), 20 for established ones.

## Data

All data is stored in a local SQLite database (`skala.db`). Tables:

- `climbers` — username, ELO, match count
- `routes` — route ID, name, grade, ELO, match count
- `ascents` — climber, route, grade, tick type, date
- `scrape_progress` — tracks which crags have been fully scraped
