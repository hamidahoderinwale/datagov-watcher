# Cursor Dashboard API - Usage Examples

## Overview

The Cursor Dashboard API provides comprehensive access to your Cursor telemetry data with **meaningful categorization** and advanced filtering capabilities. Instead of everything being labeled as "ai", data is now classified into 12 specific, actionable categories.

## API Base URL

```text
http://localhost:5001/api
```

## Data Categories

### 1. **Authentication & User Data** (`auth`)

- User tokens, emails, signup information
- Membership data, authentication states

### 2. **Privacy & Security** (`privacy`)

- Privacy settings, storage modes
- Security configurations, data protection settings

### 3. **AI Chat & Composer** (`ai_chat`)

- AI chat interactions, composer panels
- Chat history, conversation data

### 4. **AI Code Tracking** (`ai_tracking`)

- Code tracking, commit scoring
- Line analysis, development metrics

### 5. **Cursor Core Features** (`cursor_core`)

- Core Cursor features, updates
- Feature configurations, version info

### 6. **Workbench UI** (`ui`)

- Panel visibility, view configurations
- UI state, workbench settings

### 7. **Terminal** (`terminal`)

- Terminal commands, shell interactions
- Command history, terminal settings

### 8. **Extensions** (`extension`)

- Extensions, plugins, third-party integrations
- Extension configurations, marketplace data

### 9. **Theme & Appearance** (`theme`)

- Visual themes, colors, icons
- UI customization settings

### 10. **Settings** (`settings`)

- General preferences, configurations
- User settings, application preferences

### 11. **Background Processes** (`background`)

- Background processes, onboarding flows
- System processes, initialization data

### 12. **Other** (`other`)

- Miscellaneous data that doesn't fit other categories

## Usage Examples

### 1. Health Check

```bash
curl http://localhost:5001/api/health
```

**Response:**

```json
{
  "status": "healthy",
  "timestamp": "2025-09-07T12:51:17.836746",
  "version": "1.0.0"
}
```

### 2. Get All Workspaces

```bash
curl http://localhost:5001/api/workspaces
```

**Response:**

```json
{
  "workspaces": [
    {
      "id": "2de0e42c99d0e36f9f36ed120b2c242f",
      "name": "network_search",
      "path": "file:///Users/hamidaho/Desktop/network_search",
      "data": {},
      "stats": {
        "telemetry_count": 0,
        "ai_interactions": 0,
        "terminal_commands": 0,
        "total_size": 0,
        "data_types": {}
      }
    }
  ],
  "total_count": 16
}
```

### 3. Filter by Data Type - Privacy Settings

```bash
curl "http://localhost:5001/api/telemetry?data_type=privacy"
```

**Response:**

```json
{
  "data": [
    {
      "key": "cursorai/donotchange/hasReconciledNewPrivacyModeWithServerOnUpgrade",
      "type": "privacy",
      "size": "4B",
      "value": "true",
      "timestamp": "2025-09-07 12:37:59",
      "workspace_id": null
    }
  ],
  "total_count": 10,
  "filters_applied": {
    "data_type": "privacy"
  }
}
```

### 4. Filter by Data Type - Authentication Data

```bash
curl "http://localhost:5001/api/telemetry?data_type=auth"
```

**Response:**

```json
{
  "data": [
    {
      "key": "cursorAuth/cachedEmail",
      "type": "auth",
      "size": "21B",
      "value": "oderinwaleh@gmail.com",
      "timestamp": "2025-09-07 12:37:59",
      "workspace_id": null
    }
  ],
  "total_count": 7
}
```

### 5. Filter by Data Type - AI Chat Interactions

```bash
curl "http://localhost:5001/api/telemetry?data_type=ai_chat"
```

**Response:**

```json
{
  "data": [
    {
      "key": "workbench.panel.composerChatViewPane.3120cff9-0593-47b6-a6e7-54a03eb03a39.hidden",
      "type": "ai_chat",
      "size": "274B",
      "value": "[{\"id\":\"workbench.panel.aichat.view.52e4dab4-77d2-459d-9496-acce7f04d87b\",\"isHidden\":false}]",
      "timestamp": "2025-09-07 12:37:59",
      "workspace_id": null
    }
  ],
  "total_count": 11
}
```

### 6. Filter by Data Type - UI Elements

```bash
curl "http://localhost:5001/api/telemetry?data_type=ui"
```

**Response:**

```json
{
  "data": [
    {
      "key": "workbench.panel.composerChatViewPane.1c69aed0-d347-4979-9d13-ffa7be51bdcd.hidden",
      "type": "ui",
      "size": "92B",
      "value": "[{\"id\":\"workbench.panel.aichat.view.a485b4a3-4ffb-4d02-b289-0d6973431fce\",\"isHidden\":false}]",
      "timestamp": "2025-09-07 12:37:59",
      "workspace_id": null
    }
  ],
  "total_count": 11
}
```

### 7. Search by Keyword

```bash
curl "http://localhost:5001/api/telemetry?search=cursor"
```

**Response:**

```json
{
  "data": [
    {
      "key": "cursorai/donotchange/hasReconciledNewPrivacyModeWithServerOnUpgrade",
      "type": "privacy",
      "size": "4B",
      "value": "true",
      "timestamp": "2025-09-07 12:37:59",
      "workspace_id": null
    }
  ],
  "total_count": 25
}
```

### 8. Sort by Size (Largest First)

```bash
curl "http://localhost:5001/api/telemetry?sort_field=size_bytes&sort_order=desc"
```

**Response:**

```json
{
  "data": [
    {
      "key": "aiCodeTrackingLines",
      "type": "ai_tracking",
      "size": "2.0MB",
      "size_bytes": 2097152,
      "value": "[{\"hash\":\"7efc50e3\",\"metadata\":{\"source\":\"composer\"...",
      "timestamp": "2025-09-07 12:37:59",
      "workspace_id": null
    }
  ],
  "total_count": 214
}
```

### 9. Filter by Size Range

```bash
curl "http://localhost:5001/api/telemetry?size_min=1000&size_max=10000"
```

**Response:**

```json
{
  "data": [
    {
      "key": "cursorai/serverConfig",
      "type": "cursor_core",
      "size": "7.4KB",
      "size_bytes": 7578,
      "value": "{\"bugConfigResponse\":{\"bugBotV1\":{\"backgroundCallFrequencyMs\":3600000}}...",
      "timestamp": "2025-09-07 12:37:59",
      "workspace_id": null
    }
  ],
  "total_count": 3
}
```

### 10. Get Analytics Overview

```bash
curl http://localhost:5001/api/analytics
```

**Response:**

```json
{
  "overview": {
    "total_workspaces": 16,
    "total_telemetry_items": 214,
    "total_ai_interactions": 0,
    "total_terminal_commands": 50,
    "total_size": 2097152
  },
  "data_type_distribution": {
    "ai_chat": 11,
    "auth": 7,
    "cursor_core": 3,
    "extension": 1,
    "other": 9,
    "privacy": 10,
    "settings": 1,
    "terminal": 1,
    "theme": 3,
    "ui": 11
  }
}
```

### 11. Get Workspace-Specific Data

```bash
curl "http://localhost:5001/api/workspaces/2de0e42c99d0e36f9f36ed120b2c242f"
```

**Response:**

```json
{
  "id": "2de0e42c99d0e36f9f36ed120b2c242f",
  "name": "network_search",
  "path": "file:///Users/hamidaho/Desktop/network_search",
  "data": {},
  "stats": {
    "telemetry_count": 0,
    "ai_interactions": 0,
    "terminal_commands": 0,
    "total_size": 0,
    "data_types": {}
  },
  "telemetry": [],
  "ai_interactions": [],
  "terminal_history": []
}
```

### 12. Get AI Interactions with Filtering

```bash
curl "http://localhost:5001/api/ai-interactions?limit=10&sort_by=timestamp&sort_order=desc"
```

**Response:**

```json
{
  "interactions": [],
  "total_count": 0,
  "filters": {
    "workspace_id": null,
    "date_from": null,
    "date_to": null,
    "sort_by": "timestamp",
    "sort_order": "desc",
    "limit": 10
  }
}
```

### 13. Get Terminal History

```bash
curl "http://localhost:5001/api/terminal-history?limit=5"
```

**Response:**

```json
{
  "commands": [
    {
      "timestamp": "2025-09-07 12:37:59",
      "command": "cd /Users/hamidaho/cursor_dashboard",
      "workspace_id": "2de0e42c99d0e36f9f36ed120b2c242f",
      "output": "",
      "exit_code": 0
    }
  ],
  "total_count": 50
}
```

## Advanced Filtering Examples

### Combine Multiple Filters

```bash
# Get privacy-related items larger than 100 bytes
curl "http://localhost:5001/api/telemetry?data_type=privacy&size_min=100"

# Get auth items containing "token" in the key
curl "http://localhost:5001/api/telemetry?data_type=auth&search=token"

# Get UI items sorted by size
curl "http://localhost:5001/api/telemetry?data_type=ui&sort_field=size_bytes&sort_order=desc"
```

### Data Type Distribution Analysis

```bash
# Get all data types and their counts
curl -s http://localhost:5001/api/telemetry | jq '.data | group_by(.type) | map({type: .[0].type, count: length})'
```

## Benefits of Meaningful Categorization

1. **Security Analysis**: Easily identify and audit authentication and privacy-related data
2. **Performance Monitoring**: Track UI state changes and workbench configurations
3. **Usage Analytics**: Understand AI interaction patterns and code tracking metrics
4. **Troubleshooting**: Quickly filter to specific data types when debugging issues
5. **Compliance**: Easily locate privacy and security settings for compliance audits

## API Documentation

Visit `http://localhost:5001/api/docs` for interactive API documentation.

## Error Handling

The API returns appropriate HTTP status codes:

- `200 OK`: Successful request
- `400 Bad Request`: Invalid parameters
- `404 Not Found`: Resource not found
- `500 Internal Server Error`: Server error

All error responses include a JSON object with error details.
