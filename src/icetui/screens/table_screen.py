"""Table screen for listing available tables in a namespace."""

from typing import List, Dict, Any
import logging

from .base_screen import BaseListScreen
import icelib.clients as clients

logger = logging.getLogger(__name__)


class TableScreen(BaseListScreen):
    """Screen for listing and selecting tables in a namespace."""
    
    def __init__(self, catalog_name: str, namespace: str):
        self.catalog_name = catalog_name
        self.namespace = namespace
        super().__init__(title=f"Tables - {catalog_name}.{namespace}")
        
    def load_data(self) -> None:
        """Load table data from the selected namespace."""
        try:
            # Get config from app
            config = getattr(self.app, 'config', None)
            if not config:
                logger.warning("App config not yet loaded, setting empty data")
                self.set_data(["TABLE"], [])
                return
                
            # Get the catalog configuration
            catalog_config = config.catalogs.get(self.catalog_name)
            if not catalog_config:
                logger.error(f"Catalog '{self.catalog_name}' not found in config")
                self.set_data(["ERROR"], [{"ERROR": f"Catalog '{self.catalog_name}' not found"}])
                return
            
            # Load tables from the namespace
            logger.info(f"Loading tables for namespace '{self.namespace}' in catalog '{self.catalog_name}'")
            tables = clients.list_tables_metadata(catalog_config, self.namespace)
            logger.info(f"Found {len(tables)} tables")
            
            if not tables:
                self.set_data(["TABLE"], [])
                return
            
            columns = ["TABLE", "LAST_COMMIT_TS", "LAST_SNAPSHOT_ID", "TOTAL_RECORDS"]
            rows = []
            
            for table in tables:
                rows.append({
                    "TABLE": table.get("name", ""),
                    "LAST_COMMIT_TS": table.get("last_commit_ts", "—"),
                    "LAST_SNAPSHOT_ID": table.get("last_snapshot_id", "—"),
                    "TOTAL_RECORDS": str(table.get("total_records")) if table.get("total_records") is not None else "—",
                    "_table_name": table.get("name", ""),  # Hidden field for selection
                    "_namespace": self.namespace,  # Keep namespace context
                    "_catalog_name": self.catalog_name  # Keep catalog context
                })
            
            # Sort by table name
            rows.sort(key=lambda x: x["TABLE"])
            
            self.set_data(columns, rows)
            logger.info(f"Loaded {len(rows)} tables for namespace '{self.namespace}'")
            
        except Exception as e:
            logger.error(f"Failed to load tables for namespace '{self.namespace}': {e}")
            self.set_data(["ERROR"], [{"ERROR": f"Failed to load tables: {str(e)}"}])