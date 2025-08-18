"""
Protocol Education Research Assistant - Financial Data Engine
Retrieves TRUST-LEVEL recruitment spending data using URN lookups
"""

import re
import logging
import requests
import json
import os
from typing import Dict, Optional, Tuple, List, Any
from datetime import datetime
from models import ConversationStarter
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """Retrieves school financial data from government sources"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper = serper_engine
        self.scraper_api_key = os.getenv('SCRAPER_API_KEY')  # You'll need this in .env
        
    def get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Find school URN and TRUST information using government database
        
        Returns:
            {
                'urn': '141133',
                'official_name': 'Brookfield School',
                'trust_name': 'Excellence Academy Trust',
                'trust_urn': '12345',
                'schools_in_trust': 15,
                'address': 'Birmingham, B13 0RG',
                'type': 'Academy',
                'confidence': 0.95,
                'is_trust': False,  # True if we found trust-level data
                'alternatives': []
            }
        """
        
        # Build search query
        search_query = f'"{school_name}"'
        if location:
            search_query += f' {location}'
        search_query += ' site:get-information-schools.service.gov.uk'
        
        logger.info(f"Searching for URN: {search_query}")
        
        # Search using Serper
        results = self.serper.search_web(search_query, num_results=5)
        
        if not results:
            # Try FBIT site as fallback
            return self._search_fbit_direct(school_name, location)
        
        # Parse results for URN - Extract from URLs instead of snippets
        urn_matches = []
        for result in results:
            url = result.get('url', '')
            text = f"{result.get('title', '')} {result.get('snippet', '')}"
            
            # Extract URN from URL patterns
            urn_from_url = None
            
            # GIAS pattern: /Establishments/Establishment/Details/134225
            gias_match = re.search(r'/Details/(\d{5,7})', url)
            if gias_match:
                urn_from_url = gias_match.group(1)
            
            # GIAS group pattern: /Groups/Group/Details/3319
            elif '/Groups/Group/' in url:
                # This is a trust/federation
                group_match = re.search(r'/Groups/Group/Details/(\d+)', url)
                if group_match:
                    # For groups, look for URN in snippet
                    urn_pattern = r'URN:?\s*(\d{5,7})'
                    urn_match = re.search(urn_pattern, text)
                    if urn_match:
                        urn_from_url = urn_match.group(1)
            
            # Also check snippet for URN if not found in URL
            if not urn_from_url:
                urn_pattern = r'URN:?\s*(\d{5,7})'
                urn_match = re.search(urn_pattern, text)
                if urn_match:
                    urn_from_url = urn_match.group(1)
            
            if urn_from_url:
                # Check if this is a trust
                is_trust = any(word in text.lower() for word in ['federation', 'trust', 'mat', 'multi-academy'])
                
                # Extract trust info
                trust_name = None
                schools_count = None
                if is_trust:
                    # Try to extract trust name
                    trust_pattern = r'(?:trust|federation):\s*([A-Z][A-Za-z\s&]+)'
                    trust_match = re.search(trust_pattern, text, re.IGNORECASE)
                    if trust_match:
                        trust_name = trust_match.group(1).strip()
                    
                    # Try to extract number of schools
                    schools_pattern = r'(\d+)\s*(?:schools|academies)'
                    schools_match = re.search(schools_pattern, text, re.IGNORECASE)
                    if schools_match:
                        schools_count = int(schools_match.group(1))
                
                urn_matches.append({
                    'urn': urn_from_url,
                    'official_name': self._extract_school_name(result),
                    'trust_name': trust_name,
                    'schools_in_trust': schools_count,
                    'address': self._extract_location(result),
                    'url': url,
                    'is_trust': is_trust,
                    'confidence': self._calculate_name_match(school_name, result, is_trust)
                })
        
        if not urn_matches:
            return {'urn': None, 'confidence': 0.0, 'error': 'No URN found'}
        
        # PREFER TRUST DATA if available
        trust_matches = [m for m in urn_matches if m['is_trust']]
        if trust_matches:
            trust_matches.sort(key=lambda x: x['confidence'], reverse=True)
            best_match = trust_matches[0]
            logger.info(f"Found trust-level data: {best_match['trust_name'] or best_match['official_name']}")
        else:
            urn_matches.sort(key=lambda x: x['confidence'], reverse=True)
            best_match = urn_matches[0]
        
        best_match['alternatives'] = [m for m in urn_matches if m != best_match][:2]
        
        return best_match
    
    def _fetch_fbit_page(self, urn: str) -> Optional[str]:
        """Fetch actual FBIT page content using ScraperAPI"""
        
        if not self.scraper_api_key:
            logger.error("SCRAPER_API_KEY not found in environment")
            return None
            
        base_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}/spending-and-costs"
        
        # ScraperAPI parameters
        params = {
            'api_key': self.scraper_api_key,
            'url': base_url,
            'render': 'true',  # Enable JavaScript rendering
            'country_code': 'gb'
        }
        
        try:
            response = requests.get('http://api.scraperapi.com', params=params, timeout=30)
            if response.status_code == 200:
                return response.text
            else:
                logger.error(f"ScraperAPI returned status {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching FBIT page: {e}")
            return None
    
    def get_financial_data(self, urn: str, entity_name: str = None, is_trust: bool = False) -> Dict[str, Any]:
        """
        Retrieve financial data from FBIT website using URN
        Now fetches actual page content instead of relying on search snippets
        """
        
        logger.info(f"Fetching financial data for URN {urn} ({'Trust' if is_trust else 'School'})")
        
        financial_data = {
            'urn': urn,
            'entity_name': entity_name,
            'entity_type': 'Trust' if is_trust else 'School',
            'source_url': f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}",
            'extracted_date': datetime.now().isoformat()
        }
        
        # Fetch the actual FBIT page
        html_content = self._fetch_fbit_page(urn)
        
        if not html_content:
            # Fallback to search approach if scraping fails
            logger.warning("Failed to fetch FBIT page, falling back to search approach")
            return self._get_financial_data_from_search(urn, entity_name, is_trust)
        
        # Parse the HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for embedded JSON data (FBIT often includes data in script tags)
        script_tags = soup.find_all('script', type='application/json')
        for script in script_tags:
            try:
                data = json.loads(script.string)
                # Extract financial metrics from JSON
                if 'financialData' in data or 'spending' in data:
                    financial_data.update(self._parse_fbit_json(data))
                    break
            except:
                continue

        # If no JSON found, parse HTML directly
        if 'teaching_staff_per_pupil' not in financial_data:
            # Look for spending table or divs
            spending_rows = soup.find_all(['tr', 'div'], class_=re.compile(r'spending|cost|expense'))

            print(f"=== DEBUG: Found {len(spending_rows)} spending rows ===")
            for i, row in enumerate(spending_rows[:3]):
                text = row.get_text(strip=True)
                print(f"Row {i}: {text[:100]}...")
            
            for row in spending_rows:
                text = row.get_text(strip=True)
                
                # Teaching staff costs
                if 'Teaching' in text and 'staff' in text:
                    value_match = re.search(r'[£￡]([\d,]+)', text)
                    if value_match:
                        financial_data['teaching_staff_per_pupil'] = int(value_match.group(1).replace(',', ''))
                
                # Supply staff costs
                elif 'Supply staff' in text:
                    value_match = re.search(r'£([\d,]+)', text)
                    if value_match:
                        financial_data['supply_staff_costs'] = int(value_match.group(1).replace(',', ''))
                
                # Indirect employee expenses
                elif 'Indirect employee' in text:
                    value_match = re.search(r'£([\d,]+)', text)
                    if value_match:
                        financial_data['indirect_employee_expenses'] = int(value_match.group(1).replace(',', ''))
                
                # Administrative supplies
                elif 'Administrative supplies' in text:
                    value_match = re.search(r'£([\d,]+)', text)
                    if value_match:
                        financial_data['admin_supplies_per_pupil'] = int(value_match.group(1).replace(',', ''))
                
                # In-year balance
                elif 'In year balance' in text or 'In-year balance' in text:
                    value_match = re.search(r'[-]?[£￡]?([\d,]+)', text)
                    if value_match:
                        value = int(value_match.group(1).replace(',', ''))
                        if '-' in text or 'deficit' in text.lower():
                            value = -value
                        financial_data['in_year_balance'] = value
        
        # Calculate recruitment estimates
        if 'indirect_employee_expenses' in financial_data:
            # For trusts, show total and per-school breakdown
            if is_trust and hasattr(self, '_last_schools_count') and self._last_schools_count:
                schools = self._last_schools_count
                total_recruitment = int(financial_data['indirect_employee_expenses'] * 0.25)
                
                financial_data['recruitment_estimates'] = {
                    'total_trust': total_recruitment,
                    'per_school_avg': int(total_recruitment / schools),
                    'economies_of_scale_saving': '35-45%',
                    'explanation': f"Trust-wide recruitment for {schools} schools provides significant cost savings"
                }
                
                financial_data['per_school_estimates'] = {
                    'avg_indirect_employee': int(financial_data['indirect_employee_expenses'] / schools),
                    'avg_supply': int(financial_data.get('supply_staff_costs', 0) / schools) if financial_data.get('supply_staff_costs') else None
                }
            else:
                # School-level estimates
                financial_data['recruitment_estimates'] = {
                    'low': int(financial_data['indirect_employee_expenses'] * 0.2),
                    'high': int(financial_data['indirect_employee_expenses'] * 0.3),
                    'midpoint': int(financial_data['indirect_employee_expenses'] * 0.25)
                }
        
        return financial_data
    
    def _parse_fbit_json(self, data: Dict) -> Dict[str, Any]:
        """Parse financial data from FBIT JSON structure"""
        
        parsed = {}
        
        # This would need to be adapted based on actual FBIT JSON structure
        # Example structure (you'll need to inspect actual JSON):
        if 'spendingData' in data:
            spending = data['spendingData']
            
            if 'teachingStaff' in spending:
                parsed['teaching_staff_per_pupil'] = spending['teachingStaff'].get('perPupil', 0)
            
            if 'supplyStaff' in spending:
                parsed['supply_staff_costs'] = spending['supplyStaff'].get('total', 0)
            
            if 'indirectEmployee' in spending:
                parsed['indirect_employee_expenses'] = spending['indirectEmployee'].get('total', 0)
        
        return parsed
    
    def _get_financial_data_from_search(self, urn: str, entity_name: str, is_trust: bool) -> Dict[str, Any]:
        """Fallback method using search (original approach)"""
        
        base_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        
        financial_data = {
            'urn': urn,
            'entity_name': entity_name,
            'entity_type': 'Trust' if is_trust else 'School',
            'source_url': base_url,
            'extracted_date': datetime.now().isoformat(),
            'data_source': 'search_fallback'
        }
        
        # Search for specific pages
        search_queries = [
            f'site:financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn} "Indirect employee expenses" "Supply staff costs"',
            f'site:financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn} "Teaching and Teaching support staff" "per pupil"'
        ]
        
        for query in search_queries:
            results = self.serper.search_web(query, num_results=3)
            
            if results:
                # Combine all snippets
                all_content = ' '.join([r.get('snippet', '') for r in results])
                
                # Extract values (using proper £ symbol)
                patterns = {
                    'teaching_staff_per_pupil': r'Teaching and Teaching support staff.*?£([\d,]+)\s*per pupil',
                    'admin_supplies_per_pupil': r'Administrative supplies.*?£([\d,]+)\s*per pupil',
                    'supply_staff_costs': r'Supply staff costs[:\s]+£?([\d,]+)',
                    'indirect_employee_expenses': r'Indirect employee expenses[:\s]+£?([\d,]+)',
                    'in_year_balance': r'In year balance\s*[-£]?([\d,]+)'
                }
                
                for key, pattern in patterns.items():
                    match = re.search(pattern, all_content, re.IGNORECASE)
                    if match:
                        value_str = match.group(1).replace(',', '')
                        financial_data[key] = int(value_str)
                        
                        # Handle negative balance
                        if key == 'in_year_balance' and '-' in all_content[max(0, match.start()-10):match.start()]:
                            financial_data[key] = -financial_data[key]
        
        return financial_data
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Complete recruitment cost intelligence for a school/trust
        Now provides trust-level insights when available
        """
        
        # Step 1: Get URN (preferring trust data)
        urn_result = self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            return {
                'error': 'Could not find school or trust URN',
                'suggestions': urn_result.get('alternatives', [])
            }
        
        # Store schools count for financial calculations
        if urn_result.get('schools_in_trust'):
            self._last_schools_count = urn_result['schools_in_trust']
        
        # Step 2: Get financial data
        financial_data = self.get_financial_data(
            urn_result['urn'],
            urn_result.get('trust_name') or urn_result['official_name'],
            urn_result.get('is_trust', False)
        )
        
        # Step 3: Combine and enhance
        intelligence = {
            'school_searched': school_name,
            'entity_found': {
                'name': urn_result.get('trust_name') or urn_result['official_name'],
                'type': 'Trust' if urn_result.get('is_trust') else 'School',
                'urn': urn_result['urn'],
                'location': urn_result.get('address', ''),
                'schools_in_trust': urn_result.get('schools_in_trust'),
                'confidence': urn_result['confidence']
            },
            'financial': financial_data,
            'insights': self._generate_insights(financial_data, urn_result.get('is_trust')),
            'comparison': self._get_benchmarks(financial_data),
            'conversation_starters': self._generate_cost_conversations(
                financial_data, 
                urn_result.get('trust_name'),
                urn_result.get('schools_in_trust')
            )
        }
        
        return intelligence
    
    def _calculate_name_match(self, search_name: str, result: Dict, is_trust: bool) -> float:
        """Calculate confidence score for name match - now preferring trusts"""
        result_name = self._extract_school_name(result).lower()
        search_name = search_name.lower()
        
        # Boost confidence for trust results
        base_confidence = 0.7 if is_trust else 0.5
        
        # Exact match
        if search_name == result_name:
            return 1.0
        
        # Contains match
        if search_name in result_name or result_name in search_name:
            return base_confidence + 0.2
        
        # Partial word match
        search_words = set(search_name.split())
        result_words = set(result_name.split())
        common_words = search_words.intersection(result_words)
        
        if common_words:
            return base_confidence + (0.2 * len(common_words) / len(search_words))
        
        return base_confidence - 0.2
    
    def _generate_insights(self, financial_data: Dict, is_trust: bool) -> List[str]:
        """Generate insights from financial data - now trust-aware"""
        insights = []
        
        if is_trust and 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            insights.append(f"Trust-wide annual recruitment spend: £{est['total_trust']:,}")
            insights.append(f"Average per school: £{est['per_school_avg']:,} (saving {est['economies_of_scale_saving']} vs independent)")
            
            if 'supply_staff_costs' in financial_data and 'per_school_estimates' in financial_data:
                supply_per = financial_data['per_school_estimates'].get('avg_supply', 0)
                if supply_per:
                    insights.append(f"Average supply costs per school: £{supply_per:,}")
        
        elif 'recruitment_estimates' in financial_data:
            # School-level insights
            midpoint = financial_data['recruitment_estimates']['midpoint']
            insights.append(f"Estimated annual recruitment spend: £{midpoint:,}")
            
            if 'supply_staff_costs' in financial_data:
                supply = financial_data['supply_staff_costs']
                total_temp_costs = midpoint + supply
                insights.append(f"Total temporary staffing costs: £{total_temp_costs:,}")
        
        return insights
    
    def _generate_cost_conversations(self, financial_data: Dict, trust_name: str = None, schools_count: int = None) -> List[str]:
        """Generate conversation starters about costs - now trust-aware"""
        starters = []
        
        if trust_name and 'recruitment_estimates' in financial_data:
            est = financial_data['recruitment_estimates']
            starters.append(
                f"I see {trust_name} manages {schools_count or 'multiple'} schools with approximately "
                f"£{est['total_trust']:,} in annual recruitment costs. Protocol Education's trust-wide "
                "partnership could reduce this by 30-40% through economies of scale."
            )
            
            if schools_count and est.get('per_school_avg'):
                starters.append(
                    f"With an average recruitment spend of £{est['per_school_avg']:,} per school, "
                    "a trust-wide agreement with Protocol could standardize quality while reducing costs."
                )
        
        elif 'recruitment_estimates' in financial_data:
            # School-level starters
            cost = financial_data['recruitment_estimates']['midpoint']
            starters.append(
                f"Your school spends approximately £{cost:,} annually on recruitment. "
                "As part of a larger trust, Protocol Education could offer preferential rates."
            )
        
        if 'supply_staff_costs' in financial_data:
            supply = financial_data['supply_staff_costs']
            if trust_name:
                starters.append(
                    f"With trust-wide supply costs totaling £{supply:,}, our dedicated trust team "
                    "can ensure consistent quality cover across all your schools."
                )
            else:
                starters.append(
                    f"Your £{supply:,} annual supply costs could be reduced through our long-term "
                    "staffing solutions and trust-wide agreements."
                )
        
        return starters
    
    def _extract_school_name(self, search_result: Dict) -> str:
        """Extract official school name from search result"""
        title = search_result.get('title', '')
        # Remove common suffixes
        name = re.split(r' - URN:| - Get Information| - GOV.UK', title)[0]
        return name.strip()
    
    def _extract_location(self, search_result: Dict) -> str:
        """Extract location from search result"""
        snippet = search_result.get('snippet', '')
        # Look for postcode pattern
        postcode_match = re.search(r'[A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2}', snippet)
        if postcode_match:
            return postcode_match.group()
        return ''
    
    def _search_fbit_direct(self, school_name: str, location: Optional[str]) -> Dict:
        """Try searching FBIT directly"""
        search_query = f'"{school_name}" site:financial-benchmarking-and-insights-tool.education.gov.uk'
        if location:
            search_query += f' {location}'
            
        results = self.serper.search_web(search_query, num_results=3)
        
        for result in results:
            # Extract URN from FBIT URL
            url = result.get('url', '')
            urn_match = re.search(r'/school/(\d{5,7})', url)
            if urn_match:
                return {
                    'urn': urn_match.group(1),
                    'official_name': self._extract_school_name(result),
                    'confidence': 0.7,
                    'is_trust': False,
                    'alternatives': []
                }
        
        return {'urn': None, 'confidence': 0.0, 'error': 'No results found'}
    
    def _get_benchmarks(self, financial_data: Dict) -> Dict:
        """Get benchmark comparisons"""
        return {
            'national_average': {
                'indirect_employee_expenses': 35000,
                'supply_costs': 100000
            },
            'comparison': 'Trust-level data provides better economies of scale insights'
        }

# Integration function for the premium processor - OUTSIDE THE CLASS
def enhance_school_with_financial_data(intel, serper_engine):
    """
    Add financial data to existing school intelligence
    
    Args:
        intel: SchoolIntelligence object
        serper_engine: Existing PremiumAIEngine instance
    """
    
    financial_engine = FinancialDataEngine(serper_engine)
    
    # Get recruitment cost intelligence
    financial_intel = financial_engine.get_recruitment_intelligence(
        intel.school_name,
        intel.address
    )
    
    # Add to existing intelligence
    if not financial_intel.get('error'):
        # Add financial insights to conversation starters
        if 'conversation_starters' in financial_intel:
            for starter in financial_intel['conversation_starters']:
                intel.conversation_starters.append(
                    ConversationStarter(
                        topic="Recruitment Costs",
                        detail=starter,
                        source_url=financial_intel.get('financial', {}).get('source_url', ''),
                        relevance_score=0.9
                    )
                )
        
        # Store financial data in intel object
        intel.financial_data = financial_intel
    
    return intel
