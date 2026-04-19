# Teamtailor Pipeline

## Purpose

This is a pure research project exploring whether Teamtailor's ATS (Applicant Tracking System) could be systematically scraped and enriched at scale. Since Teamtailor uses the same website architecture across all its clients, the hypothesis was: if you can scrape one company's people page, you can scrape all of them.

The goal was to go from a single Teamtailor-powered company website to a fully enriched candidate database — extracting people, filtering by tech profile, and enriching with LinkedIn URLs.

The HTML files (`html-example.html`, `profile_data.html`) were used to train an LLM to understand the structure of Teamtailor pages and extract data in the desired format.

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
        ↓ loaded into
   Google Sheet (Sheet1)
        ↓ enriched by
linkedin_search.py
        ↓ writes LinkedIn URLs back to
   Google Sheet (Sheet1)
```
