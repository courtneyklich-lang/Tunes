"""
Last.fm Scrobble Analyzer
=========================
Reads a Last.fm CSV export and outputs analytics data as JSON
for use with the accompanying dashboard.html file.

Usage:
    python3 Analyze.py Data/CourtneyListeningHistory.csv --output data.json

Export your Last.fm data at: https://lastfm.ghan.nl/export/
"""

import pandas as pd
import json
import argparse
import sys
from pathlib import Path


def load_data(filepath: str) -> pd.DataFrame:
    """Load and parse the Last.fm CSV export."""
    csv_path = Path(filepath).expanduser().resolve()
    df = pd.read_csv(csv_path)

    # Parse timestamps
    df["utc_time"] = pd.to_datetime(df["utc_time"], format="%d %b %Y, %H:%M", errors="coerce")
    df = df.dropna(subset=["utc_time"])
    df = df.sort_values("utc_time").reset_index(drop=True)

    # Derived time fields
    df["year"] = df["utc_time"].dt.year
    df["month"] = df["utc_time"].dt.month
    df["hour"] = df["utc_time"].dt.hour
    df["dow"] = df["utc_time"].dt.dayofweek  # 0 = Monday
    df["date"] = df["utc_time"].dt.date
    df["ym"] = df["utc_time"].dt.to_period("M").astype(str)

    return df


def compute_overview(df: pd.DataFrame) -> dict:
    """High-level summary stats."""
    date_range_days = (df["utc_time"].max() - df["utc_time"].min()).days or 1
    return {
        "total_scrobbles": int(len(df)),
        "unique_artists": int(df["artist"].nunique()),
        "unique_albums": int(df["album"].nunique()),
        "unique_tracks": int(df["track"].nunique()),
        "date_start": df["utc_time"].min().strftime("%b %d, %Y"),
        "date_end": df["utc_time"].max().strftime("%b %d, %Y"),
        "avg_per_day": round(len(df) / date_range_days, 1),
        "years_active": int(df["year"].nunique()),
    }


def compute_top_artists(df: pd.DataFrame, n: int = 15) -> list:
    """Most played artists overall."""
    counts = df["artist"].value_counts().head(n)
    return [{"artist": artist, "plays": int(plays)} for artist, plays in counts.items()]


def compute_all_artists(df: pd.DataFrame) -> list:
    """Full artist ranking for filtering/search in the dashboard."""
    counts = df["artist"].value_counts()
    return [{"artist": artist, "plays": int(plays)} for artist, plays in counts.items()]


def compute_top_tracks(df: pd.DataFrame, n: int = 15) -> list:
    """Most played tracks overall."""
    counts = (
        df.groupby(["artist", "track"])
        .size()
        .sort_values(ascending=False)
        .head(n)
        .reset_index(name="plays")
    )
    return counts.rename(columns={"artist": "artist", "track": "track", "plays": "plays"}).to_dict(orient="records")


def compute_all_tracks(df: pd.DataFrame) -> list:
    """Full track ranking for filtering/search in the dashboard."""
    counts = (
        df.groupby(["artist", "track"])
        .size()
        .sort_values(ascending=False)
        .reset_index(name="plays")
    )
    return counts.to_dict(orient="records")


def compute_top_albums(df: pd.DataFrame, n: int = 15) -> list:
    """Most played albums overall."""
    counts = (
        df[df["album"].notna() & (df["album"] != "")]
        .groupby(["artist", "album"])
        .size()
        .sort_values(ascending=False)
        .head(n)
        .reset_index(name="plays")
    )
    return counts.to_dict(orient="records")


def compute_all_albums(df: pd.DataFrame) -> list:
    """Full album ranking for filtering/search in the dashboard."""
    counts = (
        df[df["album"].notna() & (df["album"] != "")]
        .groupby(["artist", "album"])
        .size()
        .sort_values(ascending=False)
        .reset_index(name="plays")
    )
    return counts.to_dict(orient="records")


def compute_plays_by_year(df: pd.DataFrame) -> list:
    """Total scrobbles per calendar year."""
    counts = df["year"].value_counts().sort_index()
    return [{"year": int(y), "plays": int(p)} for y, p in counts.items()]


def compute_plays_by_month(df: pd.DataFrame) -> list:
    """Total scrobbles per month (YYYY-MM)."""
    counts = df.groupby("ym").size().reset_index(name="plays")
    counts = counts.sort_values("ym")
    return counts.rename(columns={"ym": "month"}).to_dict(orient="records")


def compute_plays_by_hour(df: pd.DataFrame) -> list:
    """Average scrobbles by hour of day (0-23)."""
    hour_totals = df["hour"].value_counts().sort_index()
    n_days = df["date"].nunique() or 1
    return [
        {"hour": int(h), "plays": int(hour_totals.get(h, 0)), "avg": round(hour_totals.get(h, 0) / n_days, 2)}
        for h in range(24)
    ]


def compute_plays_by_dow(df: pd.DataFrame) -> list:
    """Scrobbles by day of week."""
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    counts = df["dow"].value_counts().sort_index()
    return [{"day": day_names[i], "plays": int(counts.get(i, 0))} for i in range(7)]


def compute_sessions(df: pd.DataFrame, gap_minutes: int = 30) -> dict:
    """
    Group plays into listening sessions (gap > gap_minutes = new session).
    Returns session-level stats.
    """
    df_s = df.sort_values("utc_time").copy()
    df_s["gap"] = df_s["utc_time"].diff().dt.total_seconds() / 60
    df_s["session_id"] = (df_s["gap"] > gap_minutes).cumsum()

    session_sizes = df_s.groupby("session_id").size()

    return {
        "total_sessions": int(len(session_sizes)),
        "avg_session_length": round(float(session_sizes.mean()), 1),
        "median_session_length": round(float(session_sizes.median()), 1),
        "max_session_length": int(session_sizes.max()),
        "sessions_over_50": int((session_sizes > 50).sum()),
    }


def compute_artist_discovery(df: pd.DataFrame) -> list:
    """
    First play date for each artist - returns the 20 most recently discovered artists
    who have been played more than once (to filter one-offs).
    """
    artist_stats = df.groupby("artist").agg(
        first_play=("utc_time", "min"),
        total_plays=("track", "count"),
    ).reset_index()

    # Only artists with >1 play to filter noise
    artist_stats = artist_stats[artist_stats["total_plays"] > 1]
    recent = artist_stats.sort_values("first_play", ascending=False).head(20)

    return [
        {
            "artist": row["artist"],
            "first_play": row["first_play"].strftime("%b %Y"),
            "total_plays": int(row["total_plays"]),
        }
        for _, row in recent.iterrows()
    ]


def compute_monthly_obsessions(df: pd.DataFrame, n: int = 10) -> list:
    """Top artist per month - reveals era-specific obsessions."""
    monthly = (
        df.groupby(["ym", "artist"])
        .size()
        .reset_index(name="plays")
    )
    top_per_month = (
        monthly.sort_values("plays", ascending=False)
        .groupby("ym")
        .first()
        .reset_index()
        .sort_values("ym")
    )
    return top_per_month.rename(columns={"ym": "month"}).to_dict(orient="records")


def compute_artist_loyalty(df: pd.DataFrame, top_n: int = 10) -> dict:
    """
    What % of listening goes to top N artists vs. the long tail?
    """
    total = len(df)
    top_plays = df["artist"].value_counts().head(top_n).sum()
    return {
        "top_n": top_n,
        "top_n_plays": int(top_plays),
        "top_n_pct": round(top_plays / total * 100, 1),
        "long_tail_pct": round((total - top_plays) / total * 100, 1),
    }


def compute_yearly_top_artists(df: pd.DataFrame, n: int = 5) -> dict:
    """Top artists per year."""
    result = {}
    for year, group in df.groupby("year"):
        top = group["artist"].value_counts().head(n)
        result[str(year)] = [{"artist": a, "plays": int(p)} for a, p in top.items()]
    return result


def compute_streak(df: pd.DataFrame) -> dict:
    """Longest consecutive listening streak (days)."""
    dates = sorted(df["date"].unique())
    if not dates:
        return {"longest_streak": 0, "current_streak": 0}

    longest = current = 1
    for i in range(1, len(dates)):
        delta = (pd.Timestamp(dates[i]) - pd.Timestamp(dates[i - 1])).days
        if delta == 1:
            current += 1
            longest = max(longest, current)
        else:
            current = 1

    # Current streak (count back from most recent date)
    current_streak = 1
    for i in range(len(dates) - 1, 0, -1):
        delta = (pd.Timestamp(dates[i]) - pd.Timestamp(dates[i - 1])).days
        if delta == 1:
            current_streak += 1
        else:
            break

    return {"longest_streak": int(longest), "current_streak": int(current_streak)}


def run(csv_path: str, output_path: str):
    print(f"Loading data from {csv_path}...")
    df = load_data(csv_path)
    print(f"  -> {len(df):,} scrobbles loaded")

    print("Computing analytics...")
    data = {
        "overview": compute_overview(df),
        "top_artists": compute_top_artists(df),
        "all_artists": compute_all_artists(df),
        "top_tracks": compute_top_tracks(df),
        "all_tracks": compute_all_tracks(df),
        "top_albums": compute_top_albums(df),
        "all_albums": compute_all_albums(df),
        "plays_by_year": compute_plays_by_year(df),
        "plays_by_month": compute_plays_by_month(df),
        "plays_by_hour": compute_plays_by_hour(df),
        "plays_by_dow": compute_plays_by_dow(df),
        "sessions": compute_sessions(df),
        "artist_discovery": compute_artist_discovery(df),
        "monthly_obsessions": compute_monthly_obsessions(df),
        "artist_loyalty": compute_artist_loyalty(df),
        "yearly_top_artists": compute_yearly_top_artists(df),
        "streak": compute_streak(df),
    }

    output_file = Path(output_path).expanduser()
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"  -> Data saved to {output_file}")

    js_output = output_file.with_suffix(".js")
    with js_output.open("w") as f:
        f.write("window.TUNES_DATA = ")
        json.dump(data, f, default=str)
        f.write(";\n")

    print(f"  -> Browser data saved to {js_output}")
    print("\nSummary:")
    ov = data["overview"]
    print(f"  {ov['total_scrobbles']:,} scrobbles | {ov['unique_artists']:,} artists | {ov['date_start']} -> {ov['date_end']}")
    print(f"  Top artist: {data['top_artists'][0]['artist']} ({data['top_artists'][0]['plays']:,} plays)")
    print(f"  Longest streak: {data['streak']['longest_streak']} days")
    print(f"\nDone! Open dashboard.html in a browser to explore your data.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze Last.fm scrobble history")
    parser.add_argument("csv", help="Path to Last.fm CSV export")
    parser.add_argument("--output", default="data.json", help="Output JSON path (default: data.json)")
    args = parser.parse_args()

    if not Path(args.csv).exists():
        print(f"Error: File not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    run(args.csv, args.output)
