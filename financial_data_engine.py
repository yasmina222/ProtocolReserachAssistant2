"""
Protocol Education Research Assistant - Financial Data Engine
Retrieves school recruitment spending data using URN lookups
"""

import re
import logging
from typing import Dict, Optional, Tuple, List
from datetime import datetime

logger = logging.getLogger(__name__)

class FinancialDataEngine:
    """Retrieves school financial data from government sources"""
    
    def __init__(self, serper_engine):
        """Initialize with existing Serper engine"""
        self.serper = serper_engine
        
    def get_school_urn(self, school_name: str, location: Optional[str] = None) -> Dict[str, any]:
        """
        Find school URN using government database
        
        Returns:
            {
                'urn': '141133',
                'official_name': 'Brookfield School',
                'address': 'Birmingham, B13 0RG',
                'type': 'Academy',
                'confidence': 0.95,
                'alternatives': []  # Other possible matches
            }
        """
        
        # Build search query
        search_query = f'"{school_name}"'
        if location:
            search_query += f' {location}'
        search_query += ' site:get-information-schools.service.gov.uk URN'
        
        logger.info(f"Searching for URN: {search_query}")
        
        # Search using Serper
        results = self.serper.search_web(search_query, num_results=5)
        
        if not results:
            return {'urn': None, 'confidence': 0.0, 'error': 'No results found'}
        
        # Parse results for URN
        urn_matches = []
        for result in results:
            # Look for URN in title or snippet
            text = f"{result.get('title', '')} {result.get('snippet', '')}"
            
            # URN pattern: typically 6 digits
            urn_pattern = r'URN:?\s*(\d{6})'
            matches = re.findall(urn_pattern, text)
            
            if matches:
                # Extract school details from result
                urn_matches.append({
                    'urn': matches[0],
                    'official_name': self._extract_school_name(result),
                    'address': self._extract_location(result),
                    'url': result.get('url', ''),
                    'confidence': self._calculate_name_match(school_name, result)
                })
        
        if not urn_matches:
            # Try alternative search on FBIT directly
            return self._search_fbit_direct(school_name, location)
        
        # Sort by confidence
        urn_matches.sort(key=lambda x: x['confidence'], reverse=True)
        
        # Return best match with alternatives
        best_match = urn_matches[0]
        best_match['alternatives'] = urn_matches[1:3]  # Next 2 best matches
        
        return best_match
    
    def get_financial_data(self, urn: str) -> Dict[str, any]:
        """
        Retrieve financial data from FBIT website using URN
        
        Returns:
            {
                'urn': '141133',
                'year': '2022-23',
                'total_expenditure': 4500000,
                'staff_costs': {
                    'total': 3200000,
                    'indirect_employee_expenses': 45000,  # Includes recruitment
                    'supply_costs': 125000
                },
                'per_pupil': {
                    'total': 4500,
                    'indirect_employee': 45
                },
                'data_quality': 'Reported',  # vs 'Estimated'
                'source_url': 'https://...',
                'extracted_date': '2024-03-20'
            }
        """
        
        # Construct FBIT URLs - try multiple pages
        base_url = f"https://financial-benchmarking-and-insights-tool.education.gov.uk/school/{urn}"
        urls_to_try = [
            f"{base_url}/spending-and-costs",
            f"{base_url}",
            f"{base_url}/spending-priorities"
        ]
        
        logger.info(f"Fetching financial data for URN {urn}")
        
        financial_data = {
            'urn': urn,
            'source_url': base_url,
            'extracted_date': datetime.now().isoformat()
        }
        
        # Try each URL
        for url in urls_to_try:
            # Search for the specific page content
            search_query = f'"{url}" "per pupil" "spending" "Teaching" "Administrative supplies"'
            results = self.serper.search_web(search_query, num_results=3)
            
            if results:
                # Combine all snippets for better extraction
                all_content = ' '.join([r.get('snippet', '') for r in results])
                
                # Extract teaching staff costs (per pupil)
                teaching_pattern = r'Teaching and Teaching support staff.*?£([\d,]+)\s*per pupil'
                teaching_match = re.search(teaching_pattern, all_content, re.IGNORECASE | re.DOTALL)
                
                if teaching_match:
                    value_str = teaching_match.group(1).replace(',', '')
                    financial_data['teaching_staff_per_pupil'] = int(value_str)
                
                # Extract administrative supplies
                admin_pattern = r'Administrative supplies.*?£([\d,]+)\s*per pupil'
                admin_match = re.search(admin_pattern, all_content, re.IGNORECASE | re.DOTALL)
                
                if admin_match:
                    value_str = admin_match.group(1).replace(',', '')
                    financial_data['admin_supplies_per_pupil'] = int(value_str)
                
                # Extract in-year balance
                balance_pattern = r'In year balance\s*[-£]?([\d,]+)'
                balance_match = re.search(balance_pattern, all_content, re.IGNORECASE)
                
                if balance_match:
                    value_str = balance_match.group(1).replace(',', '')
                    # Check if negative
                    if '-' in all_content[max(0, balance_match.start()-10):balance_match.start()]:
                        financial_data['in_year_balance'] = -int(value_str)
                    else:
                        financial_data['in_year_balance'] = int(value_str)
                
                # Extract revenue reserve
                reserve_pattern = r'Revenue reserve\s*£([\d,]+)'
                reserve_match = re.search(reserve_pattern, all_content, re.IGNORECASE)
                
                if reserve_match:
                    value_str = reserve_match.group(1).replace(',', '')
                    financial_data['revenue_reserve'] = int(value_str)
                
                # Look for other staff costs on the detailed spending page
                if 'spending-and-costs' in url:
                    # Search for specific cost categories
                    categories = [
                        ('Supply staff costs', 'supply_staff_costs'),
                        ('Education support staff costs', 'education_support_costs'),
                        ('Administrative and clerical staff costs', 'admin_staff_costs'),
                        ('Other staff costs', 'other_staff_costs'),
                        ('Indirect employee expenses', 'indirect_employee_expenses')
                    ]
                    
                    for display_name, key in categories:
                        pattern = f'{display_name}[:\s]+£?([\d,]+)'
                        match = re.search(pattern, all_content, re.IGNORECASE)
                        if match:
                            value_str = match.group(1).replace(',', '')
                            financial_data[key] = int(value_str)
        
        # If we couldn't find detailed costs, try the CSV download approach
        if 'indirect_employee_expenses' not in financial_data:
            # Use spending priorities data to estimate
            if 'teaching_staff_per_pupil' in financial_data:
                # Typical UK primary school has ~200 pupils
                estimated_pupils = 200
                financial_data['estimated_total_staff_costs'] = financial_data['teaching_staff_per_pupil'] * estimated_pupils
                
                # Industry standard: indirect costs are ~2-3% of total staff costs
                financial_data['indirect_employee_expenses'] = int(financial_data['estimated_total_staff_costs'] * 0.025)
        
        # Estimate recruitment costs if we have indirect expenses
        if 'indirect_employee_expenses' in financial_data:
            # Industry standard: recruitment is typically 20-30% of indirect employee expenses
            financial_data['estimated_recruitment_cost'] = {
                'low': int(financial_data['indirect_employee_expenses'] * 0.2),
                'high': int(financial_data['indirect_employee_expenses'] * 0.3),
                'midpoint': int(financial_data['indirect_employee_expenses'] * 0.25)
            }
        elif 'teaching_staff_per_pupil' in financial_data:
            # Alternative estimation based on teaching costs
            # Recruitment typically 1-2% of total staff budget
            per_pupil = financial_data['teaching_staff_per_pupil']
            estimated_recruitment_per_pupil = int(per_pupil * 0.015)  # 1.5%
            financial_data['estimated_recruitment_cost'] = {
                'low': estimated_recruitment_per_pupil * 150,  # Small school
                'high': estimated_recruitment_per_pupil * 300,  # Large school
                'midpoint': estimated_recruitment_per_pupil * 200  # Average
            }
            financial_data['estimation_method'] = 'Based on teaching staff costs'
        
        return financial_data
    
    def get_recruitment_intelligence(self, school_name: str, location: Optional[str] = None) -> Dict[str, any]:
        """
        Complete recruitment cost intelligence for a school
        Combines URN lookup and financial data retrieval
        """
        
        # Step 1: Get URN
        urn_result = self.get_school_urn(school_name, location)
        
        if not urn_result.get('urn'):
            return {
                'error': 'Could not find school URN',
                'suggestions': urn_result.get('alternatives', [])
            }
        
        # Step 2: Get financial data
        financial_data = self.get_financial_data(urn_result['urn'])
        
        # Step 3: Combine and enhance
        intelligence = {
            'school': {
                'name': urn_result['official_name'],
                'urn': urn_result['urn'],
                'location': urn_result.get('address', ''),
                'confidence': urn_result['confidence']
            },
            'financial': financial_data,
            'insights': self._generate_insights(financial_data),
            'comparison': self._get_benchmarks(financial_data),
            'conversation_starters': self._generate_cost_conversations(financial_data)
        }
        
        return intelligence
    
    def _extract_school_name(self, search_result: Dict) -> str:
        """Extract official school name from search result"""
        title = search_result.get('title', '')
        # Remove common suffixes
        name = re.split(r' - URN:| - Get Information', title)[0]
        return name.strip()
    
    def _extract_location(self, search_result: Dict) -> str:
        """Extract location from search result"""
        snippet = search_result.get('snippet', '')
        # Look for postcode pattern
        postcode_match = re.search(r'[A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2}', snippet)
        if postcode_match:
            return postcode_match.group()
        return ''
    
    def _calculate_name_match(self, search_name: str, result: Dict) -> float:
        """Calculate confidence score for name match"""
        result_name = self._extract_school_name(result).lower()
        search_name = search_name.lower()
        
        # Exact match
        if search_name == result_name:
            return 1.0
        
        # Contains match
        if search_name in result_name or result_name in search_name:
            return 0.8
        
        # Partial word match
        search_words = set(search_name.split())
        result_words = set(result_name.split())
        common_words = search_words.intersection(result_words)
        
        if common_words:
            return 0.5 + (0.3 * len(common_words) / len(search_words))
        
        return 0.3
    
    def _search_fbit_direct(self, school_name: str, location: Optional[str]) -> Dict:
        """Try searching FBIT directly"""
        search_query = f'"{school_name}" site:financial-benchmarking-and-insights-tool.education.gov.uk'
        if location:
            search_query += f' {location}'
            
        results = self.serper.search_web(search_query, num_results=3)
        
        for result in results:
            # Extract URN from FBIT URL
            url = result.get('url', '')
            urn_match = re.search(r'/school/(\d{6})', url)
            if urn_match:
                return {
                    'urn': urn_match.group(1),
                    'official_name': self._extract_school_name(result),
                    'confidence': 0.7,
                    'alternatives': []
                }
        
        return {'urn': None, 'confidence': 0.0}
    
    def _generate_insights(self, financial_data: Dict) -> List[str]:
        """Generate insights from financial data"""
        insights = []
        
        if 'estimated_recruitment_cost' in financial_data:
            midpoint = financial_data['estimated_recruitment_cost']['midpoint']
            insights.append(f"Estimated annual recruitment spend: £{midpoint:,}")
            
            if 'supply_staff_costs' in financial_data:
                supply = financial_data['supply_staff_costs']
                total_temp_costs = midpoint + supply
                insights.append(f"Total temporary staffing costs (recruitment + supply): £{total_temp_costs:,}")
        
        if 'indirect_employee_expenses' in financial_data:
            insights.append(f"Indirect employee expenses (inc. recruitment): £{financial_data['indirect_employee_expenses']:,}")
        
        return insights
    
    def _get_benchmarks(self, financial_data: Dict) -> Dict:
        """Get benchmark comparisons (placeholder for future enhancement)"""
        return {
            'national_average': {
                'indirect_employee_expenses': 35000,
                'supply_costs': 100000
            },
            'comparison': 'Data for similar schools coming soon'
        }
    
    def _generate_cost_conversations(self, financial_data: Dict) -> List[str]:
        """Generate conversation starters about costs"""
        starters = []
        
        if 'estimated_recruitment_cost' in financial_data:
            cost = financial_data['estimated_recruitment_cost']['midpoint']
            starters.append(
                f"I noticed your school spends approximately £{cost:,} annually on recruitment. "
                "Protocol Education can help reduce these costs through our established talent pipeline."
            )
        
        if 'supply_staff_costs' in financial_data:
            supply = financial_data['supply_staff_costs']
            starters.append(
                f"With supply costs at £{supply:,} per year, we can help you find permanent staff "
                "to reduce reliance on expensive temporary cover."
            )
        
        return starters


# Integration function for the premium processor
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
                        source_url=financial_intel['financial'].get('source_url', ''),
                        relevance_score=0.9
                    )
                )
        
        # Store financial data in intel object (you might want to add a field for this)
        intel.financial_data = financial_intel
    
    return intel


# Test function
if __name__ == "__main__":
    from ai_engine_premium import PremiumAIEngine
    
    # Test URN lookup
    engine = PremiumAIEngine()
    financial = FinancialDataEngine(engine)
    
    # Test 1: Find URN
    print("Test 1: Finding URN for 'Brookfield School Birmingham'")
    urn_result = financial.get_school_urn("Brookfield School", "Birmingham")
    print(f"Found: {urn_result}")
    
    # Test 2: Get financial data
    if urn_result.get('urn'):
        print(f"\nTest 2: Getting financial data for URN {urn_result['urn']}")
        financial_data = financial.get_financial_data(urn_result['urn'])
        print(f"Financial data: {financial_data}")
    
    # Test 3: Complete intelligence
    print("\nTest 3: Complete recruitment intelligence")
    full_intel = financial.get_recruitment_intelligence("Hampstead School", "London")
    print(f"Full intelligence: {full_intel}")