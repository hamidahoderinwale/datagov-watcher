# API Documentation

This directory contains detailed API documentation for the Cursor Process Miner.

## Endpoints

- [Health Check](health.md)
- [Entries](entries.md)
- [Prompts](prompts.md)
- [Projects](projects.md)
- [Threads](threads.md)
- [Statistics](statistics.md)

## Authentication

No authentication is required for local development. The API runs on `http://127.0.0.1:43917` by default.

## Rate Limiting

No rate limiting is currently implemented. Use responsibly.

## Error Handling

All endpoints return appropriate HTTP status codes:
- `200` - Success
- `400` - Bad Request
- `404` - Not Found
- `500` - Internal Server Error

Error responses include a JSON object with an `error` field describing the issue.






