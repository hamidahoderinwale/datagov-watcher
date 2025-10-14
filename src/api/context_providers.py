"""
Context providers for dataset profile sidebar using Exa API and Perplexity API
"""
import os
import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class UsageRef:
    title: str
    url: str
    type: str
    snippet: Optional[str] = None
    date: Optional[str] = None

@dataclass
class PolicyRef:
    title: str
    url: str
    agency: str
    snippet: Optional[str] = None
    date: Optional[str] = None

@dataclass
class NewsMention:
    title: str
    url: str
    source: str
    snippet: Optional[str] = None
    date: Optional[str] = None

class ExaContextProvider:
    """Context provider using Exa API for web search and content retrieval"""
    
    def __init__(self):
        self.api_key = os.getenv('EXA_API_KEY')
        self.base_url = "https://api.exa.ai"
        self.headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }
    
    async def search_usage_references(self, dataset_title: str, agency: str) -> List[UsageRef]:
        """Search for usage references of the dataset"""
        if not self.api_key:
            logger.warning("Exa API key not configured")
            return []
        
        try:
            # Search for academic papers, research, and usage examples
            search_queries = [
                f'"{dataset_title}" dataset usage research paper',
                f'"{dataset_title}" "{agency}" data analysis',
                f'"{dataset_title}" academic study methodology',
                f'"{dataset_title}" government data research'
            ]
            
            usage_refs = []
            async with aiohttp.ClientSession() as session:
                for query in search_queries:
                    results = await self._exa_search(session, query, num_results=3)
                    for result in results:
                        usage_refs.append(UsageRef(
                            title=result.get('title', ''),
                            url=result.get('url', ''),
                            type=self._classify_usage_type(result.get('url', '')),
                            snippet=result.get('text', '')[:200] + '...' if result.get('text') else None,
                            date=result.get('publishedDate')
                        ))
            
            return usage_refs[:10]  # Limit to 10 results
            
        except Exception as e:
            logger.error(f"Error searching usage references: {e}")
            return []
    
    async def search_policy_references(self, dataset_title: str, agency: str) -> List[PolicyRef]:
        """Search for policy references related to the dataset"""
        if not self.api_key:
            logger.warning("Exa API key not configured")
            return []
        
        try:
            # Search for policy documents, regulations, and government references
            search_queries = [
                f'"{dataset_title}" policy regulation "{agency}"',
                f'"{dataset_title}" government directive',
                f'"{dataset_title}" federal data policy',
                f'"{agency}" data governance "{dataset_title}"'
            ]
            
            policy_refs = []
            async with aiohttp.ClientSession() as session:
                for query in search_queries:
                    results = await self._exa_search(session, query, num_results=3, 
                                                   site_restriction=['.gov', '.org'])
                    for result in results:
                        policy_refs.append(PolicyRef(
                            title=result.get('title', ''),
                            url=result.get('url', ''),
                            agency=self._extract_agency_from_url(result.get('url', '')),
                            snippet=result.get('text', '')[:200] + '...' if result.get('text') else None,
                            date=result.get('publishedDate')
                        ))
            
            return policy_refs[:8]  # Limit to 8 results
            
        except Exception as e:
            logger.error(f"Error searching policy references: {e}")
            return []
    
    async def search_news_mentions(self, dataset_title: str, agency: str) -> List[NewsMention]:
        """Search for news mentions of the dataset"""
        if not self.api_key:
            logger.warning("Exa API key not configured")
            return []
        
        try:
            # Search for recent news mentions
            search_queries = [
                f'"{dataset_title}" news article',
                f'"{dataset_title}" "{agency}" data release',
                f'"{dataset_title}" government data announcement',
                f'"{dataset_title}" open data initiative'
            ]
            
            news_mentions = []
            async with aiohttp.ClientSession() as session:
                for query in search_queries:
                    results = await self._exa_search(session, query, num_results=3,
                                                   site_restriction=['.com', '.org', '.net'])
                    for result in results:
                        news_mentions.append(NewsMention(
                            title=result.get('title', ''),
                            url=result.get('url', ''),
                            source=self._extract_source_from_url(result.get('url', '')),
                            snippet=result.get('text', '')[:200] + '...' if result.get('text') else None,
                            date=result.get('publishedDate')
                        ))
            
            return news_mentions[:6]  # Limit to 6 results
            
        except Exception as e:
            logger.error(f"Error searching news mentions: {e}")
            return []
    
    async def _exa_search(self, session: aiohttp.ClientSession, query: str, 
                         num_results: int = 5, site_restriction: Optional[List[str]] = None) -> List[Dict]:
        """Perform Exa search"""
        payload = {
            "query": query,
            "numResults": num_results,
            "type": "search",
            "useAutoprompt": True,
            "text": True
        }
        
        if site_restriction:
            payload["site"] = site_restriction
        
        async with session.post(
            f"{self.base_url}/search",
            headers=self.headers,
            json=payload
        ) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('results', [])
            else:
                logger.error(f"Exa API error: {response.status}")
                return []
    
    def _classify_usage_type(self, url: str) -> str:
        """Classify the type of usage reference based on URL"""
        if 'arxiv.org' in url or 'scholar.google' in url:
            return 'Academic Paper'
        elif 'github.com' in url:
            return 'Code Repository'
        elif 'stackoverflow.com' in url:
            return 'Technical Discussion'
        elif 'medium.com' in url or 'blog' in url:
            return 'Blog Post'
        elif 'researchgate.net' in url:
            return 'Research'
        else:
            return 'Reference'
    
    def _extract_agency_from_url(self, url: str) -> str:
        """Extract agency name from URL"""
        if '.gov' in url:
            # Extract from .gov domain
            parts = url.split('.gov')[0].split('.')
            if len(parts) > 1:
                return parts[-1].replace('-', ' ').title()
        return 'Government'
    
    def _extract_source_from_url(self, url: str) -> str:
        """Extract news source from URL"""
        if 'reuters.com' in url:
            return 'Reuters'
        elif 'bloomberg.com' in url:
            return 'Bloomberg'
        elif 'wsj.com' in url:
            return 'Wall Street Journal'
        elif 'nytimes.com' in url:
            return 'New York Times'
        elif 'washingtonpost.com' in url:
            return 'Washington Post'
        elif 'cnn.com' in url:
            return 'CNN'
        elif 'bbc.com' in url:
            return 'BBC'
        else:
            # Extract domain name
            parts = url.split('//')[-1].split('/')[0].split('.')
            if len(parts) >= 2:
                return parts[-2].replace('-', ' ').title()
            return 'News Source'

class PerplexityContextProvider:
    """Context provider using Perplexity API for grounded LLM responses"""
    
    def __init__(self):
        self.api_key = os.getenv('PERPLEXITY_API_KEY')
        self.base_url = "https://api.perplexity.ai"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    async def get_context_summary(self, dataset_title: str, agency: str) -> Dict[str, Any]:
        """Get a comprehensive context summary using Perplexity's grounded LLM"""
        if not self.api_key:
            logger.warning("Perplexity API key not configured")
            return {}
        
        try:
            prompt = f"""
            Provide a comprehensive analysis of the dataset "{dataset_title}" from {agency}. 
            Include:
            1. Key usage examples and research applications
            2. Relevant policy documents and regulations
            3. Recent news mentions and public interest
            4. Data quality and reliability assessment
            
            Format as JSON with sections: usage_summary, policy_summary, news_summary, quality_assessment.
            """
            
            async with aiohttp.ClientSession() as session:
                payload = {
                    "model": "llama-3.1-sonar-small-128k-online",
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a data analyst expert. Provide accurate, well-researched information about government datasets."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "max_tokens": 1000,
                    "temperature": 0.1
                }
                
                async with session.post(
                    f"{self.base_url}/chat/completions",
                    headers=self.headers,
                    json=payload
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('choices', [{}])[0].get('message', {}).get('content', '')
                    else:
                        logger.error(f"Perplexity API error: {response.status}")
                        return {}
                        
        except Exception as e:
            logger.error(f"Error getting context summary: {e}")
            return {}

class ContextProviderManager:
    """Manager class to coordinate between different context providers"""
    
    def __init__(self):
        self.exa_provider = ExaContextProvider()
        self.perplexity_provider = PerplexityContextProvider()
    
    async def get_dataset_context(self, dataset_title: str, agency: str) -> Dict[str, Any]:
        """Get comprehensive context for a dataset"""
        try:
            # Run searches in parallel
            usage_task = self.exa_provider.search_usage_references(dataset_title, agency)
            policy_task = self.exa_provider.search_policy_references(dataset_title, agency)
            news_task = self.exa_provider.search_news_mentions(dataset_title, agency)
            summary_task = self.perplexity_provider.get_context_summary(dataset_title, agency)
            
            # Wait for all tasks to complete
            usage_refs, policy_refs, news_mentions, summary = await asyncio.gather(
                usage_task, policy_task, news_task, summary_task,
                return_exceptions=True
            )
            
            # Handle exceptions
            if isinstance(usage_refs, Exception):
                usage_refs = []
            if isinstance(policy_refs, Exception):
                policy_refs = []
            if isinstance(news_mentions, Exception):
                news_mentions = []
            if isinstance(summary, Exception):
                summary = {}
            
            return {
                'usage_refs': usage_refs,
                'policy_refs': policy_refs,
                'news_mentions': news_mentions,
                'summary': summary,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting dataset context: {e}")
            return {
                'usage_refs': [],
                'policy_refs': [],
                'news_mentions': [],
                'summary': {},
                'last_updated': datetime.now().isoformat()
            }

# Global instance
context_manager = ContextProviderManager()




