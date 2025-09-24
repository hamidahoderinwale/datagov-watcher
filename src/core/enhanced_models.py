"""
Enhanced data models for rich entity- and topic-centric explorer
"""
from dataclasses import dataclass
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum


class DatasetStatus(Enum):
    ACTIVE = "active"
    VANISHED = "vanished"
    AT_RISK = "at_risk"


class EventSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class UsageRef:
    """Reference to external usage of a dataset"""
    title: str
    url: str
    type: str  # dashboard, report, analysis, etc.
    last_updated: Optional[datetime] = None


@dataclass
class PolicyRef:
    """Reference to relevant policy documents"""
    title: str
    url: str
    agency: str
    date: datetime
    relevance_score: float = 1.0


@dataclass
class NewsMention:
    """News mention of a dataset or agency"""
    title: str
    url: str
    source: str
    date: datetime
    sentiment: Optional[str] = None  # positive, negative, neutral


@dataclass
class Tag:
    """Tag for categorizing datasets"""
    name: str
    category: str  # topic, agency, format, etc.
    description: Optional[str] = None
    usage_count: int = 0
    volatility_score: float = 0.0


@dataclass
class DatasetProfile:
    """Rich profile for individual datasets"""
    dataset_id: str
    title: str
    agency: str
    status: DatasetStatus
    license: str
    url: str
    
    # Timeline data
    first_seen: datetime
    last_seen: datetime
    snapshot_count: int
    
    # Metrics
    volatility_score: float
    license_flip_count: int
    schema_churn_count: int
    content_drift_score: float
    
    # Rich metadata
    tags: List[Tag]
    usage_refs: List[UsageRef]
    policy_refs: List[PolicyRef]
    news_mentions: List[NewsMention]
    
    # Schema summary
    schema_columns: List[Dict[str, Any]]
    schema_stability: float
    
    # Related datasets
    related_datasets: List[Dict[str, str]]  # [{'id': '...', 'title': '...'}]


@dataclass
class AgencyMetrics:
    """Aggregate metrics for agencies"""
    agency_name: str
    total_datasets: int
    active_datasets: int
    vanished_datasets: int
    avg_volatility: float
    license_stability: float  # percentage of datasets with stable licenses
    median_lifespan_days: int
    new_datasets_this_year: int
    vanished_datasets_this_year: int
    
    # Trends
    monthly_trends: List[Dict[str, Any]]  # month -> metrics
    
    # Leaderboards
    most_volatile_datasets: List[str]
    most_at_risk_datasets: List[str]


@dataclass
class TagMetrics:
    """Metrics for tags"""
    tag_name: str
    dataset_count: int
    volatility_score: float
    avg_lifespan_days: int
    vanished_count: int
    related_tags: List[str]


@dataclass
class SystemMetrics:
    """Top-level system health metrics"""
    total_datasets: int
    active_datasets: int
    vanished_datasets: int
    new_datasets_this_year: int
    vanished_datasets_this_year: int
    median_lifespan_days: int
    avg_volatility: float
    license_flip_rate: float
    
    # Agency leaderboard
    top_agencies_by_churn: List[Dict[str, Any]]
    
    # Trends over time
    yearly_trends: List[Dict[str, Any]]
    monthly_trends: List[Dict[str, Any]]


@dataclass
class TimelineEmbed:
    """Mini timeline for embedding in profiles"""
    dataset_id: str
    date_range: List[str]
    cells: List[Dict[str, Any]]
    events: List[Dict[str, Any]]
    height: int = 100  # pixels


@dataclass
class ContextSidebar:
    """Context information for datasets/agencies"""
    usage_refs: List[UsageRef]
    policy_refs: List[PolicyRef]
    news_mentions: List[NewsMention]
    related_entities: List[Dict[str, Any]]
