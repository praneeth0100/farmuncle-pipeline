"""Shared, cross-cutting modules used by every ingestion script:
config validation, batch lifecycle, identity resolution, quality
scoring, HTTP/parsing, and precedence-aware price writing. Not
intended to be imported directly by scripts — see ../ingest_common.py
for the facade."""
