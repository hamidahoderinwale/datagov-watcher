# Cursor Database Analysis Summary

**Analysis Date:** 2025-09-06 01:29:46
**Database:** /Users/hamidaho/cursor_export_20250906_010707/User/globalStorage/state.vscdb

## Database Structure

### Tables
- **ItemTable**: Main application state storage
- **cursorDiskKV**: Cursor-specific data storage

### Key Statistics
- ItemTable keys: 214
- Cursor keys: 92061

## Exported Files

### Database Structure
- `01_tables.txt` - All database tables
- `02_schema.txt` - Complete database schema
- `03_itemtable_count.txt` - ItemTable key count
- `06_cursor_keys_count.txt` - Cursor keys count

### Data Analysis
- `04_largest_values.txt` - Largest stored values by size
- `05_cursor_ai_keys.txt` - All Cursor/AI related keys
- `07_cursor_keys_sample.txt` - Sample of cursor-specific keys
- `16_all_itemtable_keys.txt` - Complete list of ItemTable keys
- `17_cursor_keys_sample_1000.txt` - First 1000 cursor keys

### Specific Data Extracts
- `08_ai_code_tracking.txt` - AI code tracking data
- `09_ai_scored_commits.txt` - AI scored commits
- `10_personal_context.txt` - Personal context data
- `11_cursor_always_local.txt` - Cursor local setting
- `12_color_theme.txt` - Color theme configuration
- `13_icon_theme.txt` - Icon theme configuration
- `14_terminal_history.txt` - Terminal command history
- `15_python_extension.txt` - Python extension data

## How to View the Data

### Text Files
Most files are plain text and can be opened with any text editor.

### Large Data Files
Some files (like AI tracking data) may be large JSON or binary data. You can:
1. Open with a text editor to see the structure
2. Use `jq` for JSON formatting: `jq . filename.txt`
3. Use a hex editor for binary data

### Database Queries
To run custom queries on the database:
```bash
sqlite3 "/Users/hamidaho/cursor_export_20250906_010707/User/globalStorage/state.vscdb" "YOUR_QUERY_HERE"
```

## Interesting Findings

- The largest stored value is `aiCodeTrackingLines` (2.1MB)
- You have extensive AI code tracking data
- Terminal history and theme data are preserved
- Python extension settings are stored
- Personal context data for AI is available

