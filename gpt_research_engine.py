"""
Protocol Education CI System - GPT-Powered Research Engine
Uses OpenAI's models directly for research instead of web scraping
"""

import os
from openai import OpenAI
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
from dotenv import load_dotenv

load_dotenv()

class GPTResearchEngine:
    """Uses OpenAI models for direct research instead of scraping"""
    
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-4-turbo-preview"  # Has knowledge up to April 2023, good for research
        
    def research_school(self, school_name: str, borough: Optional[str] = None) -> Dict[str, Any]:
        """Research a school using GPT's knowledge and web search capabilities"""
        
        # Build a comprehensive research prompt
        location = f" in {borough}" if borough else " in the UK"
        
        prompt = f"""
        Research {school_name}{location} and provide the following information:

        1. BASIC INFORMATION:
        - Full official name
        - Website URL
        - Main phone number
        - Email address
        - Full address

        2. KEY CONTACTS (if publicly available):
        - Headteacher/Principal name
        - Deputy Head name
        - Assistant Head name
        - Business Manager name
        - SENCO name
        - Any available email addresses or phone extensions

        3. OFSTED INFORMATION:
        - Latest Ofsted rating
        - Date of last inspection
        - Key findings summary

        4. RECENT UPDATES (2023-2024):
        - Recent achievements or awards
        - Notable events or news
        - Leadership changes
        - Building projects or expansions

        5. RECRUITMENT INTELLIGENCE:
        - Any known recruitment agencies they work with
        - Recent job postings mentioning agencies
        - Recruitment challenges mentioned in news/reports

        6. CONVERSATION STARTERS for recruitment consultants:
        - 3 specific, relevant talking points based on recent school news
        - Any challenges where Protocol Education could help

        Format as JSON with clear sections. If information is not available, mark as "Not found" rather than guessing.
        Base your response on publicly available information only.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert UK education researcher helping recruitment consultants. Provide accurate, up-to-date information based on public sources."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,  # Lower temperature for factual accuracy
                max_tokens=2000
            )
            
            # Parse the response
            content = response.choices[0].message.content
            
            # Try to extract JSON if the model formatted it
            try:
                # Look for JSON in the response
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    # Parse as text if not JSON
                    result = self._parse_text_response(content)
            except:
                result = self._parse_text_response(content)
            
            # Add metadata
            result['research_timestamp'] = datetime.now().isoformat()
            result['model_used'] = self.model
            
            return result
            
        except Exception as e:
            print(f"Research error: {e}")
            return {
                "error": str(e),
                "school_name": school_name
            }
    
    def _parse_text_response(self, text: str) -> Dict[str, Any]:
        """Parse text response into structured data"""
        # Basic parsing - you can enhance this
        result = {
            "school_info": {},
            "contacts": {},
            "ofsted": {},
            "recent_updates": [],
            "recruitment_intel": {},
            "conversation_starters": []
        }
        
        # Extract sections based on headers
        sections = text.split('\n\n')
        current_section = None
        
        for section in sections:
            lower_section = section.lower()
            
            if 'basic information' in lower_section or 'website' in lower_section:
                result['school_info']['raw'] = section
            elif 'contact' in lower_section or 'head' in lower_section:
                result['contacts']['raw'] = section
            elif 'ofsted' in lower_section:
                result['ofsted']['raw'] = section
            elif 'recent' in lower_section or 'update' in lower_section:
                result['recent_updates'].append(section)
            elif 'conversation' in lower_section:
                result['conversation_starters'].append(section)
        
        return result
    
    def research_borough_schools(self, borough: str, max_schools: int = 10) -> List[Dict[str, Any]]:
        """Research multiple schools in a borough"""
        
        # First, get a list of schools in the borough
        list_prompt = f"""
        List up to {max_schools} schools in {borough}, UK.
        Focus on a mix of primary and secondary schools.
        Provide just the school names, one per line.
        """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",  # Cheaper model for simple list
                messages=[
                    {"role": "user", "content": list_prompt}
                ],
                temperature=0.5,
                max_tokens=500
            )
            
            # Extract school names
            school_names = [
                name.strip() 
                for name in response.choices[0].message.content.split('\n') 
                if name.strip() and not name.startswith('#')
            ]
            
            # Research each school
            results = []
            for school_name in school_names[:max_schools]:
                print(f"Researching: {school_name}")
                school_data = self.research_school(school_name, borough)
                results.append(school_data)
            
            return results
            
        except Exception as e:
            print(f"Borough research error: {e}")
            return []


# Test function
def test_gpt_research():
    """Test the GPT research engine"""
    
    engine = GPTResearchEngine()
    
    # Test single school
    print("=== TESTING SINGLE SCHOOL RESEARCH ===\n")
    
    result = engine.research_school("Hampstead School", "Camden")
    
    print(json.dumps(result, indent=2))
    
    # Test borough search
    print("\n\n=== TESTING BOROUGH RESEARCH ===\n")
    
    schools = engine.research_borough_schools("Islington", max_schools=3)
    
    for school in schools:
        print(f"\nSchool: {school.get('school_name', 'Unknown')}")
        print(f"Ofsted: {school.get('ofsted', {}).get('rating', 'Not found')}")


if __name__ == "__main__":
    test_gpt_research()