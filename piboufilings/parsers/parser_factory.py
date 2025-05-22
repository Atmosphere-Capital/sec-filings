"""
Factory for creating parsers based on form type.
"""

import re
from typing import Optional
from .base_parser import BaseFilingParser
from .form_13f_parser import Form13FParser

class ParserFactory:
    """Factory class for creating the appropriate parser for a filing type."""
    
    @staticmethod
    def create_parser(form_type: Optional[str] = None, content: Optional[str] = None, base_dir: str = "./data_parse") -> BaseFilingParser:
        """
        Create the appropriate parser for a filing type.
        
        Args:
            form_type: Type of form (e.g., '13F-HR')
            content: Raw filing content (can be used to detect form type if form_type is None)
            base_dir: Base directory for parsed data
            
        Returns:
            BaseFilingParser: The appropriate parser for the form type
        """
        # If form_type is not provided, try to extract it from content
        if form_type is None and content is not None:
            form_type = ParserFactory._extract_form_type(content)
        
        # Choose the appropriate parser based on form type
        if form_type and (form_type.startswith("13F") or "13F" in form_type):
            return Form13FParser(base_dir=base_dir)
        
        # Default to base parser (which may not be fully functional for all form types)
        return BaseFilingParser(base_dir=base_dir)
    
    @staticmethod
    def _extract_form_type(content: str) -> Optional[str]:
        """
        Extract form type from filing content.
        
        Args:
            content: Raw filing content
            
        Returns:
            str: Form type or None if not found
        """
        form_type_match = re.search(r"CONFORMED SUBMISSION TYPE:\s+([\w-]+)", content)
        return form_type_match.group(1).strip() if form_type_match else None 