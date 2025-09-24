"""
Enhanced API endpoints for rich entity- and topic-centric explorer
"""
import sqlite3
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from flask import Blueprint, request, jsonify
from core.enhanced_models import (
    DatasetProfile, AgencyMetrics, TagMetrics, SystemMetrics,
    TimelineEmbed, ContextSidebar, DatasetStatus, Tag, UsageRef, PolicyRef, NewsMention
)
from visualization.chromogram_timeline_v2 import ChromogramTimelineV2
from analysis.event_extractor import EventExtractor
from core.availability_detector import AvailabilityDetector

# Create blueprint for enhanced endpoints
enhanced_bp = Blueprint('enhanced', __name__, url_prefix='/api/enhanced')

# Initialize services
timeline_service = ChromogramTimelineV2()
event_service = EventExtractor()
availability_service = AvailabilityDetector()


@enhanced_bp.route('/datasets/<dataset_id>')
def get_dataset_profile(dataset_id: str):
    """Get rich profile for a specific dataset"""
    try:
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        # Get basic dataset info
        cursor.execute('''
            SELECT dataset_id, title, agency, url, availability, 
                   MIN(created_at) as first_seen, MAX(created_at) as last_seen,
                   COUNT(*) as snapshot_count
            FROM dataset_states 
            WHERE dataset_id = ?
            GROUP BY dataset_id
        ''', (dataset_id,))
        
        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Dataset not found'}), 404
        
        # Get metrics
        volatility_score = _calculate_volatility_score(dataset_id, cursor)
        license_flip_count = _count_license_flips(dataset_id, cursor)
        schema_churn_count = _count_schema_changes(dataset_id, cursor)
        content_drift_score = _calculate_content_drift(dataset_id, cursor)
        
        # Get tags (placeholder - would be from tag system)
        tags = _get_dataset_tags(dataset_id, cursor)
        
        # Get usage refs (placeholder - would be from external sources)
        usage_refs = _get_usage_refs(dataset_id)
        
        # Get policy refs (placeholder - would be from policy database)
        policy_refs = _get_policy_refs(dataset_id)
        
        # Get news mentions (placeholder - would be from news API)
        news_mentions = _get_news_mentions(dataset_id)
        
        # Get schema summary
        schema_columns = _get_schema_summary(dataset_id, cursor)
        schema_stability = _calculate_schema_stability(dataset_id, cursor)
        
        # Get related datasets
        related_datasets = _get_related_datasets(dataset_id, cursor)
        
        # Determine status
        status = DatasetStatus.VANISHED if row[4] == 'vanished' else DatasetStatus.ACTIVE
        
        profile = DatasetProfile(
            dataset_id=row[0],
            title=row[1] or 'Unknown Title',
            agency=row[2] or 'Unknown Agency',
            status=status,
            license='Unknown',  # Would get from metadata
            url=row[3] or '',
            first_seen=datetime.fromisoformat(row[5]) if row[5] else datetime.now(),
            last_seen=datetime.fromisoformat(row[6]) if row[6] else datetime.now(),
            snapshot_count=row[7],
            volatility_score=volatility_score,
            license_flip_count=license_flip_count,
            schema_churn_count=schema_churn_count,
            content_drift_score=content_drift_score,
            tags=tags,
            usage_refs=usage_refs,
            policy_refs=policy_refs,
            news_mentions=news_mentions,
            schema_columns=schema_columns,
            schema_stability=schema_stability,
            related_datasets=related_datasets
        )
        
        conn.close()
        
        return jsonify({
            'dataset_id': profile.dataset_id,
            'title': profile.title,
            'agency': profile.agency,
            'status': profile.status.value,
            'license': profile.license,
            'url': profile.url,
            'first_seen': profile.first_seen.isoformat(),
            'last_seen': profile.last_seen.isoformat(),
            'snapshot_count': profile.snapshot_count,
            'volatility_score': profile.volatility_score,
            'license_flip_count': profile.license_flip_count,
            'schema_churn_count': profile.schema_churn_count,
            'content_drift_score': profile.content_drift_score,
            'tags': [{'name': tag.name, 'category': tag.category} for tag in profile.tags],
            'usage_refs': [{'title': ref.title, 'url': ref.url, 'type': ref.type} for ref in profile.usage_refs],
            'policy_refs': [{'title': ref.title, 'url': ref.url, 'agency': ref.agency} for ref in profile.policy_refs],
            'news_mentions': [{'title': mention.title, 'url': mention.url, 'source': mention.source} for mention in profile.news_mentions],
            'schema_columns': profile.schema_columns,
            'schema_stability': profile.schema_stability,
            'related_datasets': profile.related_datasets
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@enhanced_bp.route('/datasets/<dataset_id>/timeline-embed')
def get_timeline_embed(dataset_id: str):
    """Get mini timeline for embedding in profiles"""
    try:
        chromogram_data = timeline_service.generate_chromogram_data(dataset_id)
        
        embed = TimelineEmbed(
            dataset_id=dataset_id,
            date_range=chromogram_data.date_range,
            cells=chromogram_data.cells[:20],  # Limit for mini view
            events=chromogram_data.events[:10]  # Limit for mini view
        )
        
        return jsonify({
            'dataset_id': embed.dataset_id,
            'date_range': embed.date_range,
            'cells': embed.cells,
            'events': embed.events,
            'height': embed.height
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@enhanced_bp.route('/datasets/<dataset_id>/context')
async def get_dataset_context(dataset_id: str):
    """Get context sidebar for a dataset using Exa API and Perplexity API"""
    try:
        # Get dataset info first
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT title, agency 
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY created_at DESC 
            LIMIT 1
        ''', (dataset_id,))
        
        result = cursor.fetchone()
        if not result:
            return jsonify({'error': 'Dataset not found'}), 404
        
        dataset_title, agency = result
        
        # Get context data using async calls
        usage_refs = await _get_usage_refs(dataset_id, dataset_title, agency)
        policy_refs = await _get_policy_refs(dataset_id, dataset_title, agency)
        news_mentions = await _get_news_mentions(dataset_id, dataset_title, agency)
        
        # Get related entities (same agency, similar tags)
        cursor.execute('''
            SELECT DISTINCT dataset_id, title, agency
            FROM dataset_states 
            WHERE agency = ?
            AND dataset_id != ?
            LIMIT 5
        ''', (agency, dataset_id))
        
        related_entities = [
            {'dataset_id': row[0], 'title': row[1], 'type': 'dataset'}
            for row in cursor.fetchall()
        ]
        
        conn.close()
        
        context = ContextSidebar(
            usage_refs=usage_refs,
            policy_refs=policy_refs,
            news_mentions=news_mentions,
            related_entities=related_entities
        )
        
        return jsonify({
            'usage_refs': [{'title': ref.title, 'url': ref.url, 'type': ref.type, 'snippet': ref.snippet, 'date': ref.date} for ref in context.usage_refs],
            'policy_refs': [{'title': ref.title, 'url': ref.url, 'agency': ref.agency, 'snippet': ref.snippet, 'date': ref.date} for ref in context.policy_refs],
            'news_mentions': [{'title': mention.title, 'url': mention.url, 'source': mention.source, 'snippet': mention.snippet, 'date': mention.date} for mention in context.news_mentions],
            'related_entities': context.related_entities,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@enhanced_bp.route('/agencies/<agency_name>')
def get_agency_metrics(agency_name: str):
    """Get metrics and trends for an agency"""
    try:
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        # Get basic agency stats
        cursor.execute('''
            SELECT 
                COUNT(DISTINCT dataset_id) as total_datasets,
                COUNT(DISTINCT CASE WHEN availability != 'vanished' THEN dataset_id END) as active_datasets,
                COUNT(DISTINCT CASE WHEN availability = 'vanished' THEN dataset_id END) as vanished_datasets
            FROM dataset_states 
            WHERE agency = ?
        ''', (agency_name,))
        
        stats = cursor.fetchone()
        
        # Calculate metrics
        total_datasets = stats[0] or 0
        active_datasets = stats[1] or 0
        vanished_datasets = stats[2] or 0
        
        # Get volatility metrics
        cursor.execute('''
            SELECT AVG(analysis_quality_score) as avg_volatility
            FROM dataset_states 
            WHERE agency = ? AND analysis_quality_score IS NOT NULL
        ''', (agency_name,))
        
        volatility_row = cursor.fetchone()
        avg_volatility = volatility_row[0] or 0.0
        
        # Get monthly trends
        cursor.execute('''
            SELECT 
                strftime('%Y-%m', created_at) as month,
                COUNT(DISTINCT dataset_id) as new_datasets
            FROM dataset_states 
            WHERE agency = ?
            GROUP BY strftime('%Y-%m', created_at)
            ORDER BY month
        ''', (agency_name,))
        
        monthly_trends = [
            {'month': row[0], 'new_datasets': row[1]}
            for row in cursor.fetchall()
        ]
        
        # Get most volatile datasets
        cursor.execute('''
            SELECT dataset_id, title, analysis_quality_score
            FROM dataset_states 
            WHERE agency = ? AND analysis_quality_score IS NOT NULL
            ORDER BY analysis_quality_score DESC
            LIMIT 5
        ''', (agency_name,))
        
        most_volatile = [
            {'dataset_id': row[0], 'title': row[1], 'volatility': row[2]}
            for row in cursor.fetchall()
        ]
        
        conn.close()
        
        metrics = AgencyMetrics(
            agency_name=agency_name,
            total_datasets=total_datasets,
            active_datasets=active_datasets,
            vanished_datasets=vanished_datasets,
            avg_volatility=avg_volatility,
            license_stability=0.8,  # Placeholder
            median_lifespan_days=365,  # Placeholder
            new_datasets_this_year=0,  # Would calculate from trends
            vanished_datasets_this_year=0,  # Would calculate from trends
            monthly_trends=monthly_trends,
            most_volatile_datasets=[item['dataset_id'] for item in most_volatile],
            most_at_risk_datasets=[]  # Would calculate from risk analysis
        )
        
        return jsonify({
            'agency_name': metrics.agency_name,
            'total_datasets': metrics.total_datasets,
            'active_datasets': metrics.active_datasets,
            'vanished_datasets': metrics.vanished_datasets,
            'avg_volatility': metrics.avg_volatility,
            'license_stability': metrics.license_stability,
            'median_lifespan_days': metrics.median_lifespan_days,
            'new_datasets_this_year': metrics.new_datasets_this_year,
            'vanished_datasets_this_year': metrics.vanished_datasets_this_year,
            'monthly_trends': metrics.monthly_trends,
            'most_volatile_datasets': metrics.most_volatile_datasets,
            'most_at_risk_datasets': metrics.most_at_risk_datasets
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@enhanced_bp.route('/tags')
def get_tag_gallery():
    """Get all tags with metrics"""
    try:
        # This would typically come from a tag database
        # For now, generate some sample tags based on agencies
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT agency, COUNT(DISTINCT dataset_id) as count
            FROM dataset_states 
            WHERE agency IS NOT NULL
            GROUP BY agency
            ORDER BY count DESC
            LIMIT 20
        ''', ())
        
        tags = []
        for row in cursor.fetchall():
            tag = Tag(
                name=row[0],
                category='agency',
                description=f'Datasets from {row[0]}',
                usage_count=row[1],
                volatility_score=0.5  # Placeholder
            )
            tags.append(tag)
        
        conn.close()
        
        return jsonify({
            'tags': [
                {
                    'name': tag.name,
                    'category': tag.category,
                    'description': tag.description,
                    'usage_count': tag.usage_count,
                    'volatility_score': tag.volatility_score
                }
                for tag in tags
            ]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@enhanced_bp.route('/tags/<tag_name>')
def get_tag_detail(tag_name: str):
    """Get detailed view of a specific tag"""
    try:
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        # Get datasets for this tag (using agency as tag for now)
        cursor.execute('''
            SELECT DISTINCT dataset_id, title, agency, availability
            FROM dataset_states 
            WHERE agency = ?
            ORDER BY title
        ''', (tag_name,))
        
        datasets = [
            {
                'dataset_id': row[0],
                'title': row[1],
                'agency': row[2],
                'status': row[3]
            }
            for row in cursor.fetchall()
        ]
        
        # Calculate tag metrics
        total_datasets = len(datasets)
        vanished_count = len([d for d in datasets if d['status'] == 'vanished'])
        
        metrics = TagMetrics(
            tag_name=tag_name,
            dataset_count=total_datasets,
            volatility_score=0.5,  # Placeholder
            avg_lifespan_days=365,  # Placeholder
            vanished_count=vanished_count,
            related_tags=[]  # Would calculate from tag relationships
        )
        
        conn.close()
        
        return jsonify({
            'tag_name': metrics.tag_name,
            'dataset_count': metrics.dataset_count,
            'volatility_score': metrics.volatility_score,
            'avg_lifespan_days': metrics.avg_lifespan_days,
            'vanished_count': metrics.vanished_count,
            'related_tags': metrics.related_tags,
            'datasets': datasets
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@enhanced_bp.route('/metrics/overview')
def get_system_metrics():
    """Get top-level system health metrics"""
    try:
        conn = sqlite3.connect('datasets.db')
        cursor = conn.cursor()
        
        # Get basic system stats
        cursor.execute('''
            SELECT 
                COUNT(DISTINCT dataset_id) as total_datasets,
                COUNT(DISTINCT CASE WHEN availability != 'vanished' THEN dataset_id END) as active_datasets,
                COUNT(DISTINCT CASE WHEN availability = 'vanished' THEN dataset_id END) as vanished_datasets
            FROM dataset_states
        ''', ())
        
        stats = cursor.fetchone()
        
        # Get agency leaderboard
        cursor.execute('''
            SELECT 
                agency,
                COUNT(DISTINCT dataset_id) as total_datasets,
                COUNT(DISTINCT CASE WHEN availability = 'vanished' THEN dataset_id END) as vanished_datasets,
                AVG(analysis_quality_score) as avg_volatility
            FROM dataset_states 
            WHERE agency IS NOT NULL
            GROUP BY agency
            ORDER BY total_datasets DESC
            LIMIT 10
        ''', ())
        
        agency_leaderboard = [
            {
                'agency': row[0],
                'total_datasets': row[1],
                'vanished_datasets': row[2],
                'vanished_rate': (row[2] / row[1]) if row[1] > 0 else 0,
                'avg_volatility': row[3] or 0
            }
            for row in cursor.fetchall()
        ]
        
        # Get yearly trends
        cursor.execute('''
            SELECT 
                strftime('%Y', created_at) as year,
                COUNT(DISTINCT dataset_id) as new_datasets
            FROM dataset_states 
            GROUP BY strftime('%Y', created_at)
            ORDER BY year
        ''', ())
        
        yearly_trends = [
            {'year': row[0], 'new_datasets': row[1]}
            for row in cursor.fetchall()
        ]
        
        conn.close()
        
        metrics = SystemMetrics(
            total_datasets=stats[0] or 0,
            active_datasets=stats[1] or 0,
            vanished_datasets=stats[2] or 0,
            new_datasets_this_year=0,  # Would calculate from trends
            vanished_datasets_this_year=0,  # Would calculate from trends
            median_lifespan_days=365,  # Placeholder
            avg_volatility=0.5,  # Placeholder
            license_flip_rate=0.1,  # Placeholder
            top_agencies_by_churn=agency_leaderboard,
            yearly_trends=yearly_trends,
            monthly_trends=[]  # Would calculate from monthly data
        )
        
        return jsonify({
            'total_datasets': metrics.total_datasets,
            'active_datasets': metrics.active_datasets,
            'vanished_datasets': metrics.vanished_datasets,
            'new_datasets_this_year': metrics.new_datasets_this_year,
            'vanished_datasets_this_year': metrics.vanished_datasets_this_year,
            'median_lifespan_days': metrics.median_lifespan_days,
            'avg_volatility': metrics.avg_volatility,
            'license_flip_rate': metrics.license_flip_rate,
            'top_agencies_by_churn': metrics.top_agencies_by_churn,
            'yearly_trends': metrics.yearly_trends,
            'monthly_trends': metrics.monthly_trends
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Helper functions
def _calculate_volatility_score(dataset_id: str, cursor) -> float:
    """Calculate volatility score for a dataset"""
    cursor.execute('''
        SELECT AVG(analysis_quality_score) 
        FROM dataset_states 
        WHERE dataset_id = ? AND analysis_quality_score IS NOT NULL
    ''', (dataset_id,))
    
    result = cursor.fetchone()
    return result[0] or 0.0


def _count_license_flips(dataset_id: str, cursor) -> int:
    """Count license changes for a dataset"""
    # This would query the events table for LICENSE_CHANGED events
    return 0  # Placeholder


def _count_schema_changes(dataset_id: str, cursor) -> int:
    """Count schema changes for a dataset"""
    # This would query the events table for SCHEMA_* events
    return 0  # Placeholder


def _calculate_content_drift(dataset_id: str, cursor) -> float:
    """Calculate content drift score for a dataset"""
    # This would analyze content similarity over time
    return 0.0  # Placeholder


def _get_dataset_tags(dataset_id: str, cursor) -> List[Tag]:
    """Get tags for a dataset"""
    # This would query a tags table
    return []  # Placeholder


async def _get_usage_refs(dataset_id: str, dataset_title: str = None, agency: str = None) -> List[UsageRef]:
    """Get usage references for a dataset using Exa API"""
    from .context_providers import context_manager
    
    if not dataset_title or not agency:
        # Get dataset info from database
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT title, agency 
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY created_at DESC 
            LIMIT 1
        ''', (dataset_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return []
        
        dataset_title, agency = result
    
    try:
        context = await context_manager.get_dataset_context(dataset_title, agency)
        return context.get('usage_refs', [])
    except Exception as e:
        logger.error(f"Error getting usage references: {e}")
        return []


async def _get_policy_refs(dataset_id: str, dataset_title: str = None, agency: str = None) -> List[PolicyRef]:
    """Get policy references for a dataset using Exa API"""
    from .context_providers import context_manager
    
    if not dataset_title or not agency:
        # Get dataset info from database
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT title, agency 
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY created_at DESC 
            LIMIT 1
        ''', (dataset_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return []
        
        dataset_title, agency = result
    
    try:
        context = await context_manager.get_dataset_context(dataset_title, agency)
        return context.get('policy_refs', [])
    except Exception as e:
        logger.error(f"Error getting policy references: {e}")
        return []


async def _get_news_mentions(dataset_id: str, dataset_title: str = None, agency: str = None) -> List[NewsMention]:
    """Get news mentions for a dataset using Exa API"""
    from .context_providers import context_manager
    
    if not dataset_title or not agency:
        # Get dataset info from database
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT title, agency 
            FROM dataset_states 
            WHERE dataset_id = ? 
            ORDER BY created_at DESC 
            LIMIT 1
        ''', (dataset_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return []
        
        dataset_title, agency = result
    
    try:
        context = await context_manager.get_dataset_context(dataset_title, agency)
        return context.get('news_mentions', [])
    except Exception as e:
        logger.error(f"Error getting news mentions: {e}")
        return []


def _get_schema_summary(dataset_id: str, cursor) -> List[Dict[str, Any]]:
    """Get schema summary for a dataset"""
    cursor.execute('''
        SELECT schema_columns, schema_dtypes
        FROM dataset_states 
        WHERE dataset_id = ?
        ORDER BY created_at DESC
        LIMIT 1
    ''', (dataset_id,))
    
    row = cursor.fetchone()
    if not row or not row[0]:
        return []
    
    try:
        columns = json.loads(row[0]) if row[0] else []
        dtypes = json.loads(row[1]) if row[1] else {}
        
        return [
            {'name': col, 'type': dtypes.get(col, 'unknown'), 'stability': 0.8}
            for col in columns
        ]
    except:
        return []


def _calculate_schema_stability(dataset_id: str, cursor) -> float:
    """Calculate schema stability for a dataset"""
    # This would analyze schema changes over time
    return 0.8  # Placeholder


def _get_related_datasets(dataset_id: str, cursor) -> List[dict]:
    """Get related datasets (same agency, similar tags)"""
    cursor.execute('''
        SELECT DISTINCT ds.dataset_id, ds.title
        FROM dataset_states ds
        INNER JOIN (
            SELECT dataset_id, MAX(created_at) as max_created
            FROM dataset_states 
            GROUP BY dataset_id
        ) latest ON ds.dataset_id = latest.dataset_id 
        AND ds.created_at = latest.max_created
        WHERE ds.agency = (SELECT agency FROM dataset_states WHERE dataset_id = ? LIMIT 1)
        AND ds.dataset_id != ?
        AND ds.title IS NOT NULL AND ds.title != ''
        ORDER BY ds.created_at DESC
        LIMIT 5
    ''', (dataset_id, dataset_id))
    
    return [{'id': row[0], 'title': row[1]} for row in cursor.fetchall()]
