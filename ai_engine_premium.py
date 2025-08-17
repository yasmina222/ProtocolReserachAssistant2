"""
Protocol Education CI System - Premium AI Engine
Uses Serper API for web search + GPT-4-turbo for analysis
"""

import os
import requests
from openai import OpenAI
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import json
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class PremiumAIEngine:
    """Premium research engine using Serper + GPT-4-turbo"""
    
    def __init__(self):
        self.openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.serper_api_key = os.getenv("SERPER_API_KEY")  # You'll need to add this
        self.model = "gpt-4-turbo-preview"
        
        # Cost tracking
        self.usage = {
            'searches': 0,
            'search_cost': 0.0,
            'tokens_used': 0,
            'gpt_cost': 0.0,
            'total_cost': 0.0
        }
        
    def search_web(self, query: str, num_results: int = 10) -> List[Dict[str, Any]]:
        """Search using Serper API"""
        
        url = "https://google.serper.dev/search"
        
        payload = json.dumps({
            "q": query,
            "gl": "uk",  # UK results
            "hl": "en",
            "num": num_results
        })
        
        headers = {
            'X-API-KEY': self.serper_api_key,
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()
            
            # Track usage
            self.usage['searches'] += 1
            self.usage['search_cost'] += 0.02  # $50/2500 = $0.02 per search
            
            data = response.json()
            
            # Extract organic results
            results = []
            for item in data.get('organic', []):
                results.append({
                    'title': item.get('title', ''),
                    'url': item.get('link', ''),
                    'snippet': item.get('snippet', ''),
                    'position': item.get('position', 0)
                })
            
            # Also get knowledge graph if available
            if 'knowledgeGraph' in data:
                kg = data['knowledgeGraph']
                results.insert(0, {
                    'title': kg.get('title', ''),
                    'url': kg.get('website', ''),
                    'snippet': kg.get('description', ''),
                    'type': 'knowledge_graph',
                    'attributes': kg.get('attributes', {})
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Serper search error: {e}")
            return []
    
    def research_school(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """Complete school research using search + GPT-4"""
        
        # Step 1: Search for school information
        search_query = f"{school_name} school UK"
        if location:
            search_query = f"{school_name} school {location} UK"
            
        logger.info(f"Searching for: {search_query}")
        search_results = self.search_web(search_query)
        
        # Step 2: Search for specific information
        ofsted_results = self.search_web(f"{school_name} Ofsted rating latest inspection report")
        contact_results = self.search_web(f"{school_name} headteacher deputy head staff directory")
        news_results = self.search_web(f"{school_name} school news awards achievements 2024")
        email_results = self.search_web(f"{school_name} school email contact @")
        
        # Step 3: Combine all search results
        all_results = {
            'general': search_results[:5],
            'ofsted': ofsted_results[:3],
            'contacts': contact_results[:3],
            'news': news_results[:3],
            'email_patterns': email_results[:2]
        }
        
        # Update usage for additional searches
        self.usage['searches'] += 2  # We added 2 more searches
        self.usage['search_cost'] += 0.04
        
        # Step 4: Analyze with GPT-4
        analysis = self._analyze_with_gpt(school_name, all_results)
        
        # Step 5: Structure the results
        return {
            'school_name': school_name,
            'location': location,
            'data': analysis,
            'sources': self._extract_sources(all_results),
            'search_timestamp': datetime.now().isoformat(),
            'usage': self.usage.copy()
        }
    
    def _analyze_with_gpt(self, school_name: str, search_results: Dict[str, List]) -> Dict[str, Any]:
        """Analyze search results with GPT-4-turbo"""
        
        # Format search results for GPT
        search_text = self._format_search_results(search_results)
        
        prompt = f"""
        Analyze these search results for {school_name} and extract:

        1. BASIC INFORMATION:
        - Full school name
        - School type (primary/secondary/etc)
        - Website URL (official school website only)
        - Main phone number
        - Main email address
        - Full address
        - Number of pupils (if found)

        2. KEY LEADERSHIP CONTACTS:
        For each role, find the person's full name if available:
        - Headteacher/Principal
        - Deputy Headteacher
        - Assistant Headteacher
        - Business Manager
        - SENCO (Special Educational Needs Coordinator)
        
        3. CONTACT DETAILS:
        - Email patterns (e.g., firstname.lastname@school.org.uk)
        - Direct phone numbers or extensions
        - Best verified email addresses

        4. OFSTED INFORMATION:
        - Current Ofsted rating (Outstanding/Good/Requires Improvement/Inadequate)
        - Date of last inspection
        - Previous rating (if mentioned)
        - Key strengths mentioned
        - Areas for improvement

        5. RECENT SCHOOL NEWS (2023-2024):
        - Recent achievements or awards
        - Leadership changes
        - Major events or initiatives
        - Building projects
        - Any challenges mentioned

        6. RECRUITMENT INTELLIGENCE:
        - Any recruitment agencies mentioned in connection with the school
        - Recent job postings that mention agencies
        - Staff turnover indicators

        7. CONVERSATION STARTERS for recruitment consultants:
        Based on the information found, create 3 specific talking points:
        - One about recent Ofsted performance or academic achievements
        - One about leadership changes or new initiatives
        - One about building projects, awards, or upcoming events
        Make them specific and show genuine interest in the school.
        
        8. PROTOCOL ADVANTAGES:
        Based on any challenges or needs identified, suggest how Protocol Education could help.
        
        Format your response as JSON. Use "Not found" for missing information.
        Base everything on the search results provided.

        Search Results:
        {search_text}
        """
        
        try:
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at analyzing search results to extract school information. Be accurate and only use information found in the search results."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Very low for accuracy
                max_tokens=2000,
                response_format={"type": "json_object"}
            )
            
            # Track token usage
            if hasattr(response, 'usage'):
                tokens = response.usage.total_tokens
                self.usage['tokens_used'] += tokens
                # GPT-4-turbo pricing: $0.01/1k input, $0.03/1k output
                self.usage['gpt_cost'] += (tokens / 1000) * 0.02  # Average
            
            # Parse response
            result = json.loads(response.choices[0].message.content)
            
            # Calculate confidence scores
            result = self._add_confidence_scores(result)
            
            return result
            
        except Exception as e:
            logger.error(f"GPT analysis error: {e}")
            return {"error": str(e)}
    
    def _format_search_results(self, results: Dict[str, List]) -> str:
        """Format search results for GPT analysis"""
        
        formatted = []
        
        for category, items in results.items():
            formatted.append(f"\n=== {category.upper()} SEARCH RESULTS ===\n")
            
            for i, item in enumerate(items, 1):
                formatted.append(f"{i}. {item.get('title', 'No title')}")
                formatted.append(f"   URL: {item.get('url', 'No URL')}")
                formatted.append(f"   {item.get('snippet', 'No snippet')}")
                
                # Include knowledge graph attributes if present
                if item.get('type') == 'knowledge_graph' and 'attributes' in item:
                    for key, value in item['attributes'].items():
                        formatted.append(f"   {key}: {value}")
                
                formatted.append("")
        
        return "\n".join(formatted)
    
    def _extract_sources(self, results: Dict[str, List]) -> List[str]:
        """Extract unique source URLs"""
        
        sources = set()
        for category, items in results.items():
            for item in items:
                if url := item.get('url'):
                    sources.add(url)
        
        return list(sources)
    
    def _add_confidence_scores(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add confidence scores based on data completeness"""
        
        # Calculate confidence for contacts
        if 'contacts' in data:
            for role, info in data['contacts'].items():
                if isinstance(info, dict):
                    confidence = 0.0
                    if info.get('name') and info['name'] != 'Not found':
                        confidence += 0.5
                    if info.get('email') and info['email'] != 'Not found':
                        confidence += 0.3
                    if info.get('phone') and info['phone'] != 'Not found':
                        confidence += 0.2
                    info['confidence_score'] = confidence
        
        # Overall data quality score
        quality_score = 0.0
        checks = [
            ('basic_info', 'website', 0.2),
            ('basic_info', 'phone', 0.1),
            ('basic_info', 'email', 0.1),
            ('ofsted', 'rating', 0.2),
            ('contacts', 'headteacher', 0.2),
            ('recent_news', None, 0.1),
            ('conversation_starters', None, 0.1)
        ]
        
        for section, field, weight in checks:
            if section in data:
                if field:
                    if data[section].get(field) and data[section][field] != 'Not found':
                        quality_score += weight
                elif data[section]:  # Just check if section has content
                    quality_score += weight
        
        data['data_quality_score'] = quality_score
        
        return data
    
    def get_usage_report(self) -> Dict[str, Any]:
        """Get current usage and costs"""
        
        self.usage['total_cost'] = self.usage['search_cost'] + self.usage['gpt_cost']
        
        return {
            **self.usage,
            'cost_per_school': self.usage['total_cost'] / max(self.usage['searches'] / 3, 1),
            'monthly_projection': self.usage['total_cost'] * 30  # If running daily
        }


# Quick test
if __name__ == "__main__":
    # Note: You need to add SERPER_API_KEY to your .env file
    print("Note: Add this to your .env file:")
    print("SERPER_API_KEY=your-serper-api-key-here")
    print("\nGet your API key from: https://serper.dev")
    print("(Free trial available, then $50/month for 2,500 searches)")
    
    # Test with dummy data
    engine = PremiumAIEngine()
    
    if not engine.serper_api_key:
        print("\n⚠️  SERPER_API_KEY not found in .env file!")
        print("The system won't work without it.")
    else:
        print("\n✅ Serper API key found!")
        print("Ready to research schools...")