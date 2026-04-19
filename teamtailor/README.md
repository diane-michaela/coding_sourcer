# Teamtailor Pipeline

## Architecture

```
find_teamtailor_companies.py
        ↓ produces
teamtailor_companies.csv
        ↓ read by
filter_tech_companies.py
        ↓ produces
tech_companies.csv
        ↓ read by
batch_scrape_people.py
        ↓ produces
all_tech_people.csv
```
