#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyiceberg[pyiceberg-core]>=0.9.1",
#     "pyarrow>=21.0.0",
#     "s3fs>=2024.1.0",
#     "click>=8.0"
# ]
# ///

"""Baseball-themed data population script for Lakekeeper.

Creates comprehensive baseball test data including teams, players, leagues,
and analytics data to support icectl CLI and TUI testing.

Usage:
    uv run scripts/populate.py
"""

import os
import logging
import random
from datetime import datetime, timedelta, date
from decimal import Decimal

import click
import pyarrow as pa


# Trust homelab CA cert for S3 HTTPS connections (FsspecFileIO uses requests/botocore)
_HOMELAB_CA = os.path.expanduser("~/.homelab-ca.pem")
if os.path.exists(_HOMELAB_CA) and "REQUESTS_CA_BUNDLE" not in os.environ:
    os.environ["REQUESTS_CA_BUNDLE"] = _HOMELAB_CA


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Environment setup
LAKEKEEPER_URI = os.getenv("LAKEKEEPER_URI", "https://lakekeeper.homelab/catalog/")
PROJECT_ID = "00000000-0000-0000-0000-000000000000"


def ensure_warehouse_exists():
    """Ensure the warehouse exists via Lakekeeper management API."""
    logger.info("Checking if warehouse exists...")
    try:
        from pyiceberg.catalog import load_catalog
        load_catalog(
            "prod",
            uri=LAKEKEEPER_URI,
            warehouse=f"{PROJECT_ID}/warehouse",
            **{"rest.ssl-verification": False}
        )
        logger.info("Successfully connected to warehouse")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to warehouse: {e}")
        return False


def create_catalog():
    """Create PyIceberg catalog connection."""
    from pyiceberg.catalog import load_catalog
    return load_catalog(
        "prod",
        uri=LAKEKEEPER_URI,
        warehouse=f"{PROJECT_ID}/warehouse",
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3.access-key-id": "rustfsadmin",
            "s3.secret-access-key": "rustfsadmin123",
            "s3.connect-timeout": 10,
            "s3.request-timeout": 30,
            "rest.ssl-verification": False,
        }
    )


def create_namespaces(catalog):
    """Create baseball namespaces."""
    namespaces = ["teams", "players", "leagues", "analytics"]

    existing = {tuple(ns) for ns in catalog.list_namespaces()}

    for ns_name in namespaces:
        ns_tuple = (ns_name,)
        if ns_tuple not in existing:
            logger.info(f"Creating namespace: {ns_name}")
            catalog.create_namespace(ns_tuple)
        else:
            logger.info(f"Namespace already exists: {ns_name}")


def drop_all_data(catalog):
    """Drop all baseball tables and namespaces."""
    namespaces = ["teams", "players", "leagues", "analytics"]
    existing = {tuple(ns) for ns in catalog.list_namespaces()}
    for ns_name in namespaces:
        ns_tuple = (ns_name,)
        if ns_tuple not in existing:
            logger.info(f"Namespace not found, skipping: {ns_name}")
            continue
        for table_id in catalog.list_tables(ns_name):
            logger.info(f"Dropping table: {table_id}")
            catalog.drop_table(table_id)
        logger.info(f"Dropping namespace: {ns_name}")
        catalog.drop_namespace(ns_tuple)


def generate_team_data():
    """Generate realistic team data."""
    teams = [
        # American League East
        ("NYY", "New York Yankees", "New York", 1903, "Yankee Stadium", 54251, "AL", "East"),
        ("BOS", "Boston Red Sox", "Boston", 1901, "Fenway Park", 37755, "AL", "East"),
        ("TOR", "Toronto Blue Jays", "Toronto", 1977, "Rogers Centre", 49282, "AL", "East"),
        ("TB", "Tampa Bay Rays", "Tampa Bay", 1998, "Tropicana Field", 25000, "AL", "East"),
        ("BAL", "Baltimore Orioles", "Baltimore", 1901, "Oriole Park", 45971, "AL", "East"),

        # American League Central
        ("CLE", "Cleveland Guardians", "Cleveland", 1901, "Progressive Field", 35041, "AL", "Central"),
        ("MIN", "Minnesota Twins", "Minneapolis", 1901, "Target Field", 38544, "AL", "Central"),
        ("CWS", "Chicago White Sox", "Chicago", 1901, "Guaranteed Rate Field", 40615, "AL", "Central"),
        ("DET", "Detroit Tigers", "Detroit", 1901, "Comerica Park", 41083, "AL", "Central"),
        ("KC", "Kansas City Royals", "Kansas City", 1969, "Kauffman Stadium", 37903, "AL", "Central"),

        # American League West
        ("HOU", "Houston Astros", "Houston", 1962, "Minute Maid Park", 41168, "AL", "West"),
        ("SEA", "Seattle Mariners", "Seattle", 1977, "T-Mobile Park", 47929, "AL", "West"),
        ("TEX", "Texas Rangers", "Arlington", 1972, "Globe Life Field", 40300, "AL", "West"),
        ("LAA", "Los Angeles Angels", "Los Angeles", 1961, "Angel Stadium", 45517, "AL", "West"),
        ("OAK", "Oakland Athletics", "Oakland", 1901, "Oakland Coliseum", 46765, "AL", "West"),

        # National League East
        ("ATL", "Atlanta Braves", "Atlanta", 1871, "Truist Park", 41084, "NL", "East"),
        ("PHI", "Philadelphia Phillies", "Philadelphia", 1883, "Citizens Bank Park", 43647, "NL", "East"),
        ("NYM", "New York Mets", "New York", 1962, "Citi Field", 41922, "NL", "East"),
        ("MIA", "Miami Marlins", "Miami", 1993, "loanDepot Park", 37442, "NL", "East"),
        ("WSN", "Washington Nationals", "Washington", 1969, "Nationals Park", 41313, "NL", "East"),

        # National League Central
        ("MIL", "Milwaukee Brewers", "Milwaukee", 1969, "American Family Field", 41900, "NL", "Central"),
        ("STL", "St. Louis Cardinals", "St. Louis", 1882, "Busch Stadium", 45494, "NL", "Central"),
        ("CHC", "Chicago Cubs", "Chicago", 1876, "Wrigley Field", 41649, "NL", "Central"),
        ("CIN", "Cincinnati Reds", "Cincinnati", 1881, "Great American Ball Park", 42319, "NL", "Central"),
        ("PIT", "Pittsburgh Pirates", "Pittsburgh", 1881, "PNC Park", 38362, "NL", "Central"),

        # National League West
        ("LAD", "Los Angeles Dodgers", "Los Angeles", 1883, "Dodger Stadium", 56000, "NL", "West"),
        ("SD", "San Diego Padres", "San Diego", 1969, "Petco Park", 40209, "NL", "West"),
        ("SF", "San Francisco Giants", "San Francisco", 1883, "Oracle Park", 41915, "NL", "West"),
        ("AZ", "Arizona Diamondbacks", "Phoenix", 1998, "Chase Field", 48519, "NL", "West"),
        ("COL", "Colorado Rockies", "Denver", 1993, "Coors Field", 50144, "NL", "West"),
    ]
    return teams


def generate_player_data():
    """Generate realistic player data."""
    first_names = [
        "Mike", "John", "David", "Chris", "Matt", "Alex", "Ryan", "Kevin", "Brian", "Jason",
        "Tyler", "Josh", "Aaron", "Adam", "Andrew", "Anthony", "Brandon", "Carlos", "Daniel", "Eric",
        "Jose", "Justin", "Kyle", "Luis", "Mark", "Michael", "Nick", "Paul", "Rich", "Robert",
        "Scott", "Steve", "Tim", "Tony", "Will", "Zach", "Jose", "Juan", "Miguel", "Fernando"
    ]

    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
        "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
        "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
        "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores"
    ]

    positions = [
        "C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "DH", "SP", "RP", "CL"
    ]

    teams = [team[0] for team in generate_team_data()]

    players = []
    for i in range(750):
        player_id = f"P{i+1:04d}"
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        position = random.choice(positions)
        team_id = random.choice(teams)
        jersey_number = random.randint(1, 99) if random.random() > 0.1 else None
        bats = random.choice(["L", "R", "S"])
        throws = random.choice(["L", "R"])

        # Generate realistic birth date (ages 20-40)
        birth_year = random.randint(1984, 2004)
        birth_date = date(birth_year, random.randint(1, 12), random.randint(1, 28))

        # Generate debut date (after birth, before current date)
        min_debut_year = max(birth_year + 18, 2005)  # At least 18 years old
        max_debut_year = min(2024, birth_year + 35)  # Debut by age 35
        if min_debut_year <= max_debut_year:
            debut_year = random.randint(min_debut_year, max_debut_year)
            debut_date = date(debut_year, random.randint(1, 12), random.randint(1, 28))
        else:
            debut_date = None

        players.append((player_id, name, position, team_id, jersey_number, bats, throws, birth_date, debut_date))

    return players


def generate_batting_stats_data():
    """Generate realistic batting statistics."""
    players = generate_player_data()
    pitcher_positions = {"SP", "RP", "CL"}

    stats = []
    for player_id, name, position, team_id, *_ in players:
        # Skip detailed batting stats for pitchers (they have minimal at-bats)
        if position in pitcher_positions:
            if random.random() < 0.3:  # Only 30% of pitchers have batting stats
                for season in [2022, 2023, 2024]:
                    # Very limited stats for pitchers
                    games = random.randint(1, 5)
                    at_bats = random.randint(1, 10)
                    hits = random.randint(0, at_bats // 3)
                    avg = round(hits / at_bats, 3) if at_bats > 0 else 0.000

                    stats.append((player_id, season, team_id, games, at_bats,
                                random.randint(0, hits), hits, 0, 0, 0, 0, 0, avg))
            continue

        # Generate full batting stats for position players
        for season in [2022, 2023, 2024]:
            if random.random() < 0.85:  # 85% chance player played in season
                games = random.randint(50, 162)
                at_bats = random.randint(games * 2, games * 4)

                # Generate realistic hit distribution
                avg_target = random.uniform(0.220, 0.320)  # Realistic batting average range
                hits = int(at_bats * avg_target)

                # Generate hit types (doubles, triples, home runs)
                doubles = random.randint(hits // 8, hits // 4)
                triples = random.randint(0, max(1, hits // 20))
                home_runs = random.randint(0, max(1, hits // 6))

                # Ensure hit types don't exceed total hits
                extra_base = doubles + triples + home_runs
                if extra_base > hits:
                    doubles = hits // 3
                    triples = 0 if hits < 10 else random.randint(0, 2)
                    home_runs = max(0, hits - doubles - triples) // 2

                runs = random.randint(hits // 3, hits)
                rbis = random.randint(home_runs, hits)
                stolen_bases = random.randint(0, max(1, runs // 4))

                actual_avg = round(hits / at_bats, 3) if at_bats > 0 else 0.000

                stats.append((player_id, season, team_id, games, at_bats, runs,
                             hits, doubles, triples, home_runs, rbis, stolen_bases, actual_avg))

    return stats


def generate_game_logs_data():
    """Generate realistic game log data."""
    teams = [team[0] for team in generate_team_data()]

    games = []
    game_id_counter = 1

    # Generate games for 2022-2024 seasons
    for year in [2022, 2023, 2024]:
        season_start = date(year, 3, 25)  # Approximate season start
        season_end = date(year, 9, 30)   # Approximate season end

        current_date = season_start
        while current_date <= season_end and game_id_counter <= 15000:  # Limit total games
            # Skip some days (rest days, rainouts, etc.)
            if random.random() < 0.3:
                current_date += timedelta(days=1)
                continue

            # Generate 1-8 games per day
            games_today = random.randint(1, 8)

            for _ in range(games_today):
                game_id = f"G{game_id_counter:06d}"

                # Pick random teams (ensure they're different)
                home_team = random.choice(teams)
                away_team = random.choice([t for t in teams if t != home_team])

                # Generate realistic scores (most games are close)
                if random.random() < 0.7:  # 70% of games are within 3 runs
                    home_score = random.randint(2, 8)
                    away_score = home_score + random.randint(-3, 3)
                    away_score = max(0, away_score)
                else:  # 30% are blowouts
                    home_score = random.randint(0, 15)
                    away_score = random.randint(0, 15)

                innings = 9 if random.random() < 0.85 else random.randint(10, 15)  # Extra innings
                attendance = random.randint(15000, 45000) if random.random() > 0.1 else None

                # Weather conditions (optional)
                weather_options = ["Clear", "Cloudy", "Light Rain", "Dome", None]
                weather = random.choice(weather_options) if random.random() > 0.3 else None
                temperature = random.randint(45, 95) if weather and weather != "Dome" else None

                games.append((game_id, current_date, home_team, away_team, home_score,
                             away_score, innings, attendance, weather, temperature))

                game_id_counter += 1

                if game_id_counter > 15000:  # Limit total games
                    break

            current_date += timedelta(days=1)

    return games


def generate_performance_metrics_data():
    """Generate performance metrics data."""
    games = generate_game_logs_data()
    players = generate_player_data()

    metrics = []
    metric_id_counter = 1

    # Sample subset of games and players for metrics (to keep reasonable size)
    sample_games = random.sample(games, min(5000, len(games)))
    sample_players = random.sample(players, min(300, len(players)))

    for game_id, game_date, home_team, away_team, *_ in sample_games:
        # Get players from teams playing in this game
        game_players = [p for p in sample_players if p[3] in [home_team, away_team]]

        for player_id, name, position, *_ in game_players[:10]:  # Limit players per game
            # Generate multiple metrics per player per game
            num_metrics = random.randint(1, 5)

            for _ in range(num_metrics):
                metric_id = f"M{metric_id_counter:08d}"

                # Game timestamp (during the 3-hour game window)
                game_datetime = datetime.combine(game_date, datetime.min.time())
                game_datetime += timedelta(
                    hours=random.randint(19, 22),  # Games typically 7-10 PM
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )

                # Different metric types based on position
                if position in {"SP", "RP", "CL"}:  # Pitchers
                    metric_types = [
                        ("pitch_velocity", random.uniform(85.0, 102.5), "mph"),
                        ("spin_rate", random.uniform(1800, 3200), "rpm"),
                        ("release_point", random.uniform(5.5, 7.0), "feet"),
                    ]
                else:  # Position players
                    metric_types = [
                        ("exit_velocity", random.uniform(65.0, 115.0), "mph"),
                        ("launch_angle", random.uniform(-25.0, 45.0), "degrees"),
                        ("batting_avg", random.uniform(0.180, 0.400), "avg"),
                        ("sprint_speed", random.uniform(22.0, 32.0), "ft/sec"),
                    ]

                metric_type, value, unit = random.choice(metric_types)

                metrics.append((metric_id, player_id, game_id, game_datetime,
                               metric_type, round(value, 2), unit))

                metric_id_counter += 1

                if metric_id_counter > 100000:  # Limit total metrics
                    return metrics

    return metrics


def generate_standings_data():
    """Generate team season standings for 2022-2024."""
    teams = generate_team_data()

    # Group teams by (league, division)
    divisions: dict = {}
    for t in teams:
        key = (t[6], t[7])  # league, division
        divisions.setdefault(key, []).append(t[0])  # team_id

    standings = []
    for year in [2022, 2023, 2024]:
        for (league, division), team_ids in divisions.items():
            shuffled = list(team_ids)
            random.shuffle(shuffled)
            for rank, team_id in enumerate(shuffled, 1):
                wins = random.randint(55, 107)
                losses = 162 - wins
                standings.append((team_id, year, wins, losses, rank, league, division))

    return standings


def write_in_chunks(table, pa_table: pa.Table, num_chunks: int = 10) -> None:
    """Write a PyArrow table in chunks to produce multiple snapshots."""
    n = len(pa_table)
    if n == 0:
        table.overwrite(pa_table)
        return

    chunk_size = max(1, n // num_chunks)
    offsets = list(range(0, n, chunk_size))

    for batch_num, offset in enumerate(offsets):
        length = min(chunk_size, n - offset)
        chunk = pa_table.slice(offset, length)
        if batch_num == 0:
            table.overwrite(chunk)
        else:
            table.append(chunk)

    actual_chunks = len(offsets)
    logger.info(f"  → wrote {n} rows in {actual_chunks} snapshot(s) (~{chunk_size} rows/chunk)")


def create_and_populate_teams_tables(catalog):
    """Create and populate teams namespace tables."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import StringType, IntegerType, NestedField
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform

    # Create franchises table
    franchises_schema = Schema(
        NestedField(1, "franchise_id", StringType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "city", StringType(), required=True),
        NestedField(4, "founded_year", IntegerType(), required=False),
        NestedField(5, "stadium_name", StringType(), required=False),
        NestedField(6, "stadium_capacity", IntegerType(), required=False),
        NestedField(7, "league", StringType(), required=True),
        NestedField(8, "division", StringType(), required=True),
    )

    franchises_partition_spec = PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=IdentityTransform(), name="league")
    )

    try:
        table = catalog.create_table("teams.franchises", schema=franchises_schema, partition_spec=franchises_partition_spec)
        logger.info("Created table: teams.franchises")
    except Exception as e:
        logger.debug(f"teams.franchises already exists or create failed ({e}), loading instead")
        table = catalog.load_table("teams.franchises")

    # Generate and populate data
    teams_data = generate_team_data()

    pa_schema = pa.schema([
        pa.field("franchise_id", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("city", pa.string(), nullable=False),
        pa.field("founded_year", pa.int32(), nullable=True),
        pa.field("stadium_name", pa.string(), nullable=True),
        pa.field("stadium_capacity", pa.int32(), nullable=True),
        pa.field("league", pa.string(), nullable=False),
        pa.field("division", pa.string(), nullable=False),
    ])

    # Convert data to PyArrow arrays
    arrays = [
        pa.array([t[0] for t in teams_data]),  # franchise_id
        pa.array([t[1] for t in teams_data]),  # name
        pa.array([t[2] for t in teams_data]),  # city
        pa.array([t[3] for t in teams_data]),  # founded_year
        pa.array([t[4] for t in teams_data]),  # stadium_name
        pa.array([t[5] for t in teams_data]),  # stadium_capacity
        pa.array([t[6] for t in teams_data]),  # league
        pa.array([t[7] for t in teams_data]),  # division
    ]

    teams_table = pa.Table.from_arrays(arrays, schema=pa_schema)
    write_in_chunks(table, teams_table)
    logger.info(f"Populated teams.franchises with {len(teams_data)} records")


def create_and_populate_players_tables(catalog):
    """Create and populate players namespace tables."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import StringType, IntegerType, DateType, DecimalType, NestedField
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform

    # Create roster table
    roster_schema = Schema(
        NestedField(1, "player_id", StringType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "position", StringType(), required=True),
        NestedField(4, "team_id", StringType(), required=True),
        NestedField(5, "jersey_number", IntegerType(), required=False),
        NestedField(6, "bats", StringType(), required=False),
        NestedField(7, "throws", StringType(), required=False),
        NestedField(8, "birth_date", DateType(), required=False),
        NestedField(9, "debut_date", DateType(), required=False),
    )

    roster_partition_spec = PartitionSpec(
        PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="team_id")
    )

    try:
        roster_table = catalog.create_table("players.roster", schema=roster_schema, partition_spec=roster_partition_spec)
        logger.info("Created table: players.roster")
    except Exception as e:
        logger.debug(f"players.roster already exists or create failed ({e}), loading instead")
        roster_table = catalog.load_table("players.roster")

    # Generate and populate roster data
    players_data = generate_player_data()

    pa_roster_schema = pa.schema([
        pa.field("player_id", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("position", pa.string(), nullable=False),
        pa.field("team_id", pa.string(), nullable=False),
        pa.field("jersey_number", pa.int32(), nullable=True),
        pa.field("bats", pa.string(), nullable=True),
        pa.field("throws", pa.string(), nullable=True),
        pa.field("birth_date", pa.date32(), nullable=True),
        pa.field("debut_date", pa.date32(), nullable=True),
    ])

    roster_arrays = [
        pa.array([p[0] for p in players_data]),  # player_id
        pa.array([p[1] for p in players_data]),  # name
        pa.array([p[2] for p in players_data]),  # position
        pa.array([p[3] for p in players_data]),  # team_id
        pa.array([p[4] for p in players_data]),  # jersey_number
        pa.array([p[5] for p in players_data]),  # bats
        pa.array([p[6] for p in players_data]),  # throws
        pa.array([p[7] for p in players_data]),  # birth_date
        pa.array([p[8] for p in players_data]),  # debut_date
    ]

    roster_pa_table = pa.Table.from_arrays(roster_arrays, schema=pa_roster_schema)
    write_in_chunks(roster_table, roster_pa_table)
    logger.info(f"Populated players.roster with {len(players_data)} records")

    # Create batting_stats table
    batting_schema = Schema(
        NestedField(1, "player_id", StringType(), required=True),
        NestedField(2, "season", IntegerType(), required=True),
        NestedField(3, "team_id", StringType(), required=True),
        NestedField(4, "games", IntegerType(), required=False),
        NestedField(5, "at_bats", IntegerType(), required=False),
        NestedField(6, "runs", IntegerType(), required=False),
        NestedField(7, "hits", IntegerType(), required=False),
        NestedField(8, "doubles", IntegerType(), required=False),
        NestedField(9, "triples", IntegerType(), required=False),
        NestedField(10, "home_runs", IntegerType(), required=False),
        NestedField(11, "rbis", IntegerType(), required=False),
        NestedField(12, "stolen_bases", IntegerType(), required=False),
        NestedField(13, "avg", DecimalType(precision=5, scale=3), required=False),
    )

    batting_partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="season")
    )

    try:
        batting_table = catalog.create_table("players.batting_stats", schema=batting_schema, partition_spec=batting_partition_spec)
        logger.info("Created table: players.batting_stats")
    except Exception as e:
        logger.debug(f"players.batting_stats already exists or create failed ({e}), loading instead")
        batting_table = catalog.load_table("players.batting_stats")

    # Generate and populate batting stats
    batting_data = generate_batting_stats_data()

    pa_batting_schema = pa.schema([
        pa.field("player_id", pa.string(), nullable=False),
        pa.field("season", pa.int32(), nullable=False),
        pa.field("team_id", pa.string(), nullable=False),
        pa.field("games", pa.int32(), nullable=True),
        pa.field("at_bats", pa.int32(), nullable=True),
        pa.field("runs", pa.int32(), nullable=True),
        pa.field("hits", pa.int32(), nullable=True),
        pa.field("doubles", pa.int32(), nullable=True),
        pa.field("triples", pa.int32(), nullable=True),
        pa.field("home_runs", pa.int32(), nullable=True),
        pa.field("rbis", pa.int32(), nullable=True),
        pa.field("stolen_bases", pa.int32(), nullable=True),
        pa.field("avg", pa.decimal128(5, 3), nullable=True),
    ])

    batting_arrays = [
        pa.array([b[0] for b in batting_data]),  # player_id
        pa.array([b[1] for b in batting_data]),  # season
        pa.array([b[2] for b in batting_data]),  # team_id
        pa.array([b[3] for b in batting_data]),  # games
        pa.array([b[4] for b in batting_data]),  # at_bats
        pa.array([b[5] for b in batting_data]),  # runs
        pa.array([b[6] for b in batting_data]),  # hits
        pa.array([b[7] for b in batting_data]),  # doubles
        pa.array([b[8] for b in batting_data]),  # triples
        pa.array([b[9] for b in batting_data]),  # home_runs
        pa.array([b[10] for b in batting_data]), # rbis
        pa.array([b[11] for b in batting_data]), # stolen_bases
        pa.array([Decimal(str(round(b[12], 3))) for b in batting_data], type=pa.decimal128(5, 3)), # avg
    ]

    batting_pa_table = pa.Table.from_arrays(batting_arrays, schema=pa_batting_schema)
    write_in_chunks(batting_table, batting_pa_table)
    logger.info(f"Populated players.batting_stats with {len(batting_data)} records")


def create_and_populate_analytics_tables(catalog):
    """Create and populate analytics namespace tables."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (StringType, IntegerType, DateType, TimestampType,
                                 DecimalType, NestedField)
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import YearTransform, MonthTransform

    # Create game_logs table
    game_logs_schema = Schema(
        NestedField(1, "game_id", StringType(), required=True),
        NestedField(2, "game_date", DateType(), required=True),
        NestedField(3, "home_team", StringType(), required=True),
        NestedField(4, "away_team", StringType(), required=True),
        NestedField(5, "home_score", IntegerType(), required=False),
        NestedField(6, "away_score", IntegerType(), required=False),
        NestedField(7, "innings", IntegerType(), required=False),
        NestedField(8, "attendance", IntegerType(), required=False),
        NestedField(9, "weather", StringType(), required=False),
        NestedField(10, "temperature", IntegerType(), required=False),
    )

    game_logs_partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=YearTransform(), name="game_date_year")
    )

    try:
        game_logs_table = catalog.create_table("analytics.game_logs", schema=game_logs_schema, partition_spec=game_logs_partition_spec)
        logger.info("Created table: analytics.game_logs")
    except Exception as e:
        logger.debug(f"analytics.game_logs already exists or create failed ({e}), loading instead")
        game_logs_table = catalog.load_table("analytics.game_logs")

    # Generate and populate game logs
    games_data = generate_game_logs_data()

    pa_games_schema = pa.schema([
        pa.field("game_id", pa.string(), nullable=False),
        pa.field("game_date", pa.date32(), nullable=False),
        pa.field("home_team", pa.string(), nullable=False),
        pa.field("away_team", pa.string(), nullable=False),
        pa.field("home_score", pa.int32(), nullable=True),
        pa.field("away_score", pa.int32(), nullable=True),
        pa.field("innings", pa.int32(), nullable=True),
        pa.field("attendance", pa.int32(), nullable=True),
        pa.field("weather", pa.string(), nullable=True),
        pa.field("temperature", pa.int32(), nullable=True),
    ])

    games_arrays = [
        pa.array([g[0] for g in games_data]),  # game_id
        pa.array([g[1] for g in games_data]),  # game_date
        pa.array([g[2] for g in games_data]),  # home_team
        pa.array([g[3] for g in games_data]),  # away_team
        pa.array([g[4] for g in games_data]),  # home_score
        pa.array([g[5] for g in games_data]),  # away_score
        pa.array([g[6] for g in games_data]),  # innings
        pa.array([g[7] for g in games_data]),  # attendance
        pa.array([g[8] for g in games_data]),  # weather
        pa.array([g[9] for g in games_data]),  # temperature
    ]

    games_pa_table = pa.Table.from_arrays(games_arrays, schema=pa_games_schema)
    write_in_chunks(game_logs_table, games_pa_table)
    logger.info(f"Populated analytics.game_logs with {len(games_data)} records")

    # Create performance_metrics table
    metrics_schema = Schema(
        NestedField(1, "metric_id", StringType(), required=True),
        NestedField(2, "player_id", StringType(), required=True),
        NestedField(3, "game_id", StringType(), required=True),
        NestedField(4, "timestamp", TimestampType(), required=True),
        NestedField(5, "metric_type", StringType(), required=True),
        NestedField(6, "value", DecimalType(precision=10, scale=2), required=True),
        NestedField(7, "unit", StringType(), required=False),
    )

    metrics_partition_spec = PartitionSpec(
        PartitionField(source_id=4, field_id=1000, transform=MonthTransform(), name="timestamp_month")
    )

    try:
        metrics_table = catalog.create_table("analytics.performance_metrics", schema=metrics_schema, partition_spec=metrics_partition_spec)
        logger.info("Created table: analytics.performance_metrics")
    except Exception as e:
        logger.debug(f"analytics.performance_metrics already exists or create failed ({e}), loading instead")
        metrics_table = catalog.load_table("analytics.performance_metrics")

    # Generate and populate performance metrics
    metrics_data = generate_performance_metrics_data()

    pa_metrics_schema = pa.schema([
        pa.field("metric_id", pa.string(), nullable=False),
        pa.field("player_id", pa.string(), nullable=False),
        pa.field("game_id", pa.string(), nullable=False),
        pa.field("timestamp", pa.timestamp('us'), nullable=False),
        pa.field("metric_type", pa.string(), nullable=False),
        pa.field("value", pa.decimal128(10, 2), nullable=False),
        pa.field("unit", pa.string(), nullable=True),
    ])

    metrics_arrays = [
        pa.array([m[0] for m in metrics_data]),  # metric_id
        pa.array([m[1] for m in metrics_data]),  # player_id
        pa.array([m[2] for m in metrics_data]),  # game_id
        pa.array([m[3] for m in metrics_data]),  # timestamp
        pa.array([m[4] for m in metrics_data]),  # metric_type
        pa.array([Decimal(str(round(m[5], 2))) for m in metrics_data], type=pa.decimal128(10, 2)),  # value
        pa.array([m[6] for m in metrics_data]),  # unit
    ]

    metrics_pa_table = pa.Table.from_arrays(metrics_arrays, schema=pa_metrics_schema)
    write_in_chunks(metrics_table, metrics_pa_table)
    logger.info(f"Populated analytics.performance_metrics with {len(metrics_data)} records")


def create_and_populate_standings_table(catalog):
    """Create standings table with partition spec evolution.

    Demonstrates:
    - spec_0: identity(league)  — written with 2022 data
    - spec_1: identity(league) + identity(season)  — written with 2023/2024 data
    """
    from pyiceberg.schema import Schema
    from pyiceberg.types import StringType, IntegerType, NestedField
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform

    standings_schema = Schema(
        NestedField(1, "team_id", StringType(), required=True),
        NestedField(2, "season", IntegerType(), required=True),
        NestedField(3, "wins", IntegerType(), required=False),
        NestedField(4, "losses", IntegerType(), required=False),
        NestedField(5, "division_rank", IntegerType(), required=False),
        NestedField(6, "league", StringType(), required=True),
        NestedField(7, "division", StringType(), required=True),
    )

    # Initial spec: partition by league only
    initial_spec = PartitionSpec(
        PartitionField(source_id=6, field_id=1000, transform=IdentityTransform(), name="league")
    )

    try:
        table = catalog.create_table(
            "analytics.standings",
            schema=standings_schema,
            partition_spec=initial_spec,
        )
        logger.info("Created table: analytics.standings")
    except Exception as e:
        logger.debug(f"analytics.standings already exists ({e}), loading instead")
        table = catalog.load_table("analytics.standings")

    standings_data = generate_standings_data()

    pa_schema = pa.schema([
        pa.field("team_id", pa.string(), nullable=False),
        pa.field("season", pa.int32(), nullable=False),
        pa.field("wins", pa.int32(), nullable=True),
        pa.field("losses", pa.int32(), nullable=True),
        pa.field("division_rank", pa.int32(), nullable=True),
        pa.field("league", pa.string(), nullable=False),
        pa.field("division", pa.string(), nullable=False),
    ])

    def to_pa_table(rows):
        return pa.Table.from_arrays([
            pa.array([s[0] for s in rows]),  # team_id
            pa.array([s[1] for s in rows]),  # season
            pa.array([s[2] for s in rows]),  # wins
            pa.array([s[3] for s in rows]),  # losses
            pa.array([s[4] for s in rows]),  # division_rank
            pa.array([s[5] for s in rows]),  # league
            pa.array([s[6] for s in rows]),  # division
        ], schema=pa_schema)

    # Write 2022 data under spec_0 (league only)
    data_2022 = [s for s in standings_data if s[1] == 2022]
    table.overwrite(to_pa_table(data_2022))
    logger.info(f"Wrote {len(data_2022)} rows under spec_0 (league)")

    # Evolve partition spec: add season → creates spec_1 (league + season)
    with table.update_spec() as update:
        update.add_field("season", IdentityTransform(), partition_field_name="season")
    logger.info("Evolved partition spec to spec_1 (league + season)")

    # Write 2023 data under spec_1
    data_2023 = [s for s in standings_data if s[1] == 2023]
    table.append(to_pa_table(data_2023))
    logger.info(f"Wrote {len(data_2023)} rows under spec_1 (league + season)")

    # Write 2024 data under spec_1
    data_2024 = [s for s in standings_data if s[1] == 2024]
    table.append(to_pa_table(data_2024))
    logger.info(f"Wrote {len(data_2024)} rows under spec_1 (league + season)")

    logger.info(f"Populated analytics.standings with {len(standings_data)} records across 2 partition specs")


@click.command()
@click.option(
    "--drop",
    is_flag=True,
    default=False,
    help="Drop all existing baseball tables and namespaces before populating.",
)
def main(drop: bool) -> None:
    """Populate Lakekeeper with baseball test data."""
    logger.info("Starting baseball data population...")

    if not ensure_warehouse_exists():
        logger.error("Cannot proceed without warehouse access")
        raise SystemExit(1)

    try:
        catalog = create_catalog()
        logger.info("Connected to catalog successfully")

        if drop:
            logger.info("--drop specified: removing existing data first...")
            drop_all_data(catalog)

        create_namespaces(catalog)

        logger.info("Creating and populating teams tables...")
        create_and_populate_teams_tables(catalog)

        logger.info("Creating and populating players tables...")
        create_and_populate_players_tables(catalog)

        logger.info("Creating and populating analytics tables...")
        create_and_populate_analytics_tables(catalog)

        logger.info("Creating and populating standings table (partition evolution demo)...")
        create_and_populate_standings_table(catalog)

        logger.info("✅ Baseball data population completed successfully!")
        logger.info("Data summary:")
        logger.info("  - 4 namespaces: teams, players, leagues, analytics")
        logger.info("  - 5 tables with comprehensive baseball data")
        logger.info("  - 100,000+ total records for realistic testing")
        logger.info("")
        logger.info("Test with: scripts/run_integration.sh")

    except Exception as e:
        logger.error(f"Population failed: {e}")
        import traceback
        traceback.print_exc()
        raise SystemExit(1)


if __name__ == "__main__":
    main()