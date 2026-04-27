# Tunes

Deep analysis of listening activity from a Last.fm CSV export.

## Run the analyzer

```bash
python3 Analyze.py Data/CourtneyListeningHistory.csv --output output/data.json
```

This generates:

- `output/data.json` for raw analytics output
- `output/data.js` for the browser dashboard

## Open the dashboard

Open [dashboard.html](/Users/owner/Documents/New%20project/Tunes/dashboard.html) in a browser after running the analyzer.

The dashboard includes:

- overview metrics and headline rankings
- yearly and monthly listening trends
- searchable artist, track, and album rankings
- listening session, streak, hour-of-day, and day-of-week views
