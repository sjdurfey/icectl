"""Navigation state management for TUI."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class NavigationState:
    """Track current navigation context across TUI screens."""
    
    catalog: Optional[str] = None    # Current catalog
    database: Optional[str] = None   # Current database  
    table: Optional[str] = None      # Current table
    
    def get_breadcrumb(self) -> str:
        """Generate breadcrumb string for current context."""
        parts = []
        if self.catalog:
            parts.append(self.catalog)
        if self.database:
            parts.append(self.database)
        if self.table:
            parts.append(self.table)
        return " > ".join(parts) if parts else "icectl"
    
    def get_context_level(self) -> str:
        """Get the current context level for refresh operations."""
        if self.table:
            return "table"
        elif self.database:
            return "tables"
        elif self.catalog:
            return "databases"
        else:
            return "catalogs"
    
    def reset_from_level(self, level: str) -> None:
        """Reset navigation state from a specific level downward."""
        if level == "catalogs":
            self.catalog = None
            self.database = None
            self.table = None
        elif level == "databases":
            self.database = None
            self.table = None
        elif level == "tables":
            self.table = None
    
    def set_catalog(self, catalog: str) -> None:
        """Set catalog and reset downstream context."""
        self.catalog = catalog
        self.database = None
        self.table = None
    
    def set_database(self, database: str) -> None:
        """Set database and reset downstream context."""
        self.database = database
        self.table = None
    
    def set_table(self, table: str) -> None:
        """Set table."""
        self.table = table