# Dataset Recovery System

A comprehensive dataset recovery system based on the University of Michigan "Missing Government Websites and Data" guide. This system implements a layered retrieval approach across all major archival repositories to locate vanished government datasets.

## 🏗️ Architecture

### Layered Recovery Stack

| Layer | Primary Resource | Purpose |
|-------|------------------|---------|
| **A. Harvard Law LIL + Source Cooperative** | 16 TB Data.gov mirror (311k datasets, 2024–2025) | Instant access to federal datasets; authoritative hashes and provenance |
| **B. Internet Archive Ecosystem** | Wayback Machine, CDC Datasets collection, ERIC Archive | File-level captures for CSVs, PDFs, APIs, and educational data |
| **C. Academic Mirrors** | UMich "Find Lost Data", DataLumos (ICPSR), UCSB Public Data Git | Persistent DOIs and researcher-grade provenance metadata |
| **D. Civic Rescues** | EDGI, Data Rescue Tracker, Data Rescue Project Portal | Community-contributed mirrors; key for environmental and justice datasets |
| **E. Targeted State Archives** | data.wa.gov, data.ny.gov, state Git mirrors | For state-level removals (EV population, Powerball) |
| **F. Presidential Transition Archives** | End of Term (EOT), GovWayback, Webrecorder US Gov Archive | Institutional web captures before and after inaugurations |

## 🚀 Quick Start

### Web Interface

1. Navigate to the **Post-mortem Reports** page
2. Click the 🔍 button next to any vanished dataset
3. View recovery results from all sources
4. Access recovered datasets via provided links

### Command Line Interface

```bash
# Search for a single dataset
python src/recovery/rescue_datasets_cli.py search \
  --id "844dbad1-ee1e-44b8-9799-34cb7ed24640" \
  --title "Electric Vehicle Population Data" \
  --agency "State of Washington"

# Search multiple datasets from CSV
python src/recovery/rescue_datasets_cli.py batch \
  --input vanished_datasets.csv

# Generate FOIA request template
python src/recovery/rescue_datasets_cli.py foia \
  --id "844dbad1-ee1e-44b8-9799-34cb7ed24640" \
  --title "Electric Vehicle Population Data" \
  --agency "State of Washington"

# List available recovery sources
python src/recovery/rescue_datasets_cli.py sources
```

## 📊 Recovery Sources

### Primary Sources

1. **🏛️ Harvard LIL + Source Cooperative**
   - 16 TB Data.gov mirror
   - 311k datasets (2024–2025)
   - Authoritative hashes and provenance

2. **🔍 Find Lost Data (UMich)**
   - Federated search across multiple archives
   - Aggregates Harvard LIL, Data Lumos, IA, Dataverse

3. **📊 DataLumos (ICPSR)**
   - DOI-assigned open datasets
   - Persistent identifiers
   - Research-grade metadata

4. **🕰️ Internet Archive Wayback Machine**
   - File-level captures
   - Historical snapshots
   - CDX API access

### Specialized Sources

5. **🌱 EDGI Climate & Justice**
   - Environmental datasets
   - Climate change data
   - Environmental justice information

6. **📚 End of Term Archive**
   - Presidential transition captures
   - Federal agency domain archives
   - Pre/post-inauguration snapshots

7. **💾 Webrecorder US Gov Archive**
   - High-fidelity website archives
   - Interactive content preservation
   - Government website collections

8. **🔬 UCSB Public Data Git**
   - NASA/NOAA/DOE mirrors
   - GitHub-hosted datasets
   - Version-controlled data

## 🔄 Recovery Workflow

### 1. Dataset Identification
```python
dataset_metadata = DatasetMetadata(
    title="Electric Vehicle Population Data",
    agency="State of Washington",
    data_gov_id="844dbad1-ee1e-44b8-9799-34cb7ed24640",
    landing_url="https://data.wa.gov/resource/f6w7-q2d2",
    last_seen="2025-09-19",
    keywords=["electric", "vehicle", "population", "washington"]
)
```

### 2. Priority-Based Search
- Search sources in priority order (1-8)
- Stop early if high-confidence result found (>80%)
- Aggregate results from all sources

### 3. Provenance Generation
```json
{
  "dataset": {
    "title": "Electric Vehicle Population Data",
    "agency": "State of Washington",
    "data_gov_id": "844dbad1-ee1e-44b8-9799-34cb7ed24640",
    "original_url": "https://data.wa.gov/resource/f6w7-q2d2",
    "last_seen": "2025-09-19"
  },
  "recovery_results": [
    {
      "status": "found_datalumos",
      "source": "DataLumos",
      "url": "https://www.datalumos.org/dataset/...",
      "confidence": 0.9,
      "capture_date": "2025-09-20"
    }
  ],
  "provenance_chain": ["Data.gov", "Harvard LIL", "DataLumos"]
}
```

## 🎯 Status Badges

| Status | Icon | Description |
|--------|------|-------------|
| ✅ Found (LIL Mirror) | 🏛️ | Found in Harvard LIL mirror |
| 🕰 Found (Wayback) | 🕰️ | Found in Internet Archive |
| 📊 Found (DataLumos) | 📊 | Found in DataLumos repository |
| 🌱 Found (EDGI) | 🌱 | Found in EDGI archive |
| 🏛️ Found (EOT Archive) | 📚 | Found in End of Term archive |
| ♻️ Reissued (Substitute) | ♻️ | Dataset reissued with new URL |
| 🚨 Missing (FOIA Filed) | 🚨 | Not found, FOIA request filed |
| 📋 FOIA Filed | 📋 | FOIA request submitted |

## 🔧 Configuration

Edit `recovery_config.yaml` to customize:

```yaml
# Enable/disable specific sources
sources:
  lil:
    enabled: true
    priority: 1
  
# Adjust confidence thresholds
workflow:
  early_stop_confidence: 0.8
  min_confidence_threshold: 0.5

# Customize output settings
output_dir: "./rescued_datasets"
export:
  formats: ["json", "csv", "yaml"]
```

## 📈 API Endpoints

### Web API

- `GET /api/dataset/{dataset_id}/recovery-search` - Search for specific dataset
- `GET /api/recovery-sources` - List all available recovery sources
- `GET /api/vanished-datasets/political-analysis` - Political analysis of vanished datasets

### Response Format

```json
{
  "dataset_id": "844dbad1-ee1e-44b8-9799-34cb7ed24640",
  "recovery_results": [...],
  "provenance_pack": {...},
  "status_badge": {
    "icon": "📊",
    "text": "Found (DataLumos)"
  },
  "search_timestamp": "2025-01-27T10:30:00Z"
}
```

## 🛠️ Development

### Adding New Recovery Sources

1. Add source to `UnifiedRecoverySystem.recovery_sources`
2. Implement `_search_{source_id}` method
3. Update configuration file
4. Add tests

### Example Source Implementation

```python
def _search_new_source(self, dataset_metadata: DatasetMetadata) -> Optional[RecoveryResult]:
    try:
        response = self.session.get(
            f"https://new-source.org/api/search?q={quote(dataset_metadata.title)}",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('results'):
                result = data['results'][0]
                return RecoveryResult(
                    status=RecoveryStatus.FOUND_NEW_SOURCE,
                    source='New Source',
                    url=result.get('url', ''),
                    confidence=0.8
                )
    except Exception as e:
        print(f"New source search error: {e}")
    
    return None
```

## 📚 References

- [University of Michigan "Missing Government Websites and Data" Guide](https://findlostdata.org)
- [404 Media: Data.gov Archivists Work to Identify and Save the Thousands of Datasets Disappearing](https://404media.co)
- [Harvard LIL Data.gov Mirror](https://source.coop/lil-data-gov)
- [DataLumos (ICPSR)](https://www.datalumos.org)
- [Environmental Data & Governance Institute](https://envirodatagov.org)
- [End of Term Archive](https://eotarchive.cdlib.org)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add new recovery sources or improve existing ones
4. Add tests and documentation
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
