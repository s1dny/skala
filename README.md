# Skala

Two-sided ELO rating system for rock climbing. Every ascent is a match between climber and route: flash it, you win; no flash?, and the route wins. Over time, both climbers and routes converge on accurate ratings.

Data sourced from [27crags.com](https://27crags.com).

## Install

Requires Python 3.13+.

```
uv sync
```

## Usage

### Discover crags

```
uv run skala crags --limit 20
uv run skala crags --sort likes
```

Lists crags from 27crags.com, showing the slug you need for scraping. Sort by `boulders` (default) or `likes`.

### Scrape

```
# Scrape specific crags by slug
uv run skala scrape --crags magic-wood,cresciano,fontainebleau

# Scrape one climber directly by exact username
uv run skala scrape --climber pelez

# Scrape every crag the climber has climbed at
uv run skala scrape --climber pelez --full

# Auto-discover and scrape top 10 crags by likes
uv run skala scrape --crags 10 --sort likes
```

Fetches route lists and all logged ascents for each crag. `--crags` accepts a number (top N by sort order) or comma-separated slugs. Progress is tracked per-crag, so interrupted scrapes resume where they left off.

`--climber` bypasses crag discovery and imports the ascents listed on a single climber profile. The user must provide the exact climber username.

`--climber --full` uses the climber's full ascent history to discover every crag they have climbed at, then runs the full crag scraper for those crags.

### Calculate ELO

```
uv run skala calculate
```

Processes all ascents chronologically and updates ELO ratings for every climber and route.

### View rankings

```
uv run skala rankings --type routes --limit 20
uv run skala rankings --type climbers --limit 20
uv run skala rankings --climber pelez
```

## How ELO works here

Each ascent is treated as a head-to-head match:

- **Flash or onsight** = climber wins (climber gains ELO, route loses)
- **Everything else** (redpoint, toprope, etc.) = route wins (route gains ELO, climber loses)

The logic: almost nobody logs failed attempts, so flash vs. non-flash is the strongest available signal for whether the climber found the route easy or hard.

K-factors: 32 for new entities (<30 matches), 20 for established ones.

## Data

All data is stored in a local SQLite database (`uv run skala.db`). Tables:

- `climbers` — username, ELO, match count
- `routes` — route ID, name, grade, ELO, match count
- `ascents` — climber, route, grade, tick type, date
- `scrape_progress` — tracks which crags have been fully scraped
