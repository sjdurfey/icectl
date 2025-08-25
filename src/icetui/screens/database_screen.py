"""Database/namespace screen for listing available databases in a catalog."""

from typing import List, Dict, Any
import logging

from .base_screen import BaseListScreen
import icelib.clients as clients

logger = logging.getLogger(__name__)


class DatabaseScreen(BaseListScreen):
    """Screen for listing and selecting databases/namespaces in a catalog."""
    
    def __init__(self, catalog_name: str):
        self.catalog_name = catalog_name
        super().__init__(title=f"Databases - {catalog_name}")
        
    def load_data(self) -> None:
        """Load database/namespace data from the selected catalog."""
        try:
            # Get config from app
            config = getattr(self.app, 'config', None)
            if not config:
                logger.warning("App config not yet loaded, setting empty data")
                self.set_data(["NAMESPACE"], [])
                return
                
            # Get the catalog configuration
            catalog_config = config.catalogs.get(self.catalog_name)
            if not catalog_config:
                logger.error(f"Catalog '{self.catalog_name}' not found in config")
                self.set_data(["ERROR"], [{"ERROR": f"Catalog '{self.catalog_name}' not found"}])
                return
            
            # Load namespaces from the catalog
            logger.info(f"Loading namespaces for catalog '{self.catalog_name}'")
            namespaces = clients.list_namespaces(catalog_config)
            logger.info(f"Found {len(namespaces)} namespaces")
            
            if not namespaces:
                self.set_data(["NAMESPACE"], [])
                return
            
            columns = ["NAMESPACE"]
            rows = []
            
            for namespace in namespaces:
                rows.append({
                    "NAMESPACE": namespace,
                    "_namespace": namespace,  # Hidden field for selection
                    "_catalog_name": self.catalog_name  # Keep catalog context
                })
            
            # Sort by namespace name
            rows.sort(key=lambda x: x["NAMESPACE"])
            
            self.set_data(columns, rows)
            logger.info(f"Loaded {len(rows)} namespaces for catalog '{self.catalog_name}'")
            
        except Exception as e:
            logger.error(f"Failed to load namespaces for catalog '{self.catalog_name}': {e}")
            self.set_data(["ERROR"], [{"ERROR": f"Failed to load namespaces: {str(e)}"}])