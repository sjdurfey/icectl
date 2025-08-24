"""Error handling utilities for icectl."""

from __future__ import annotations

import re
from typing import Any


def format_error_message(operation: str, error: Exception, context: dict[str, Any] | None = None) -> str:
    """Format a user-friendly error message based on the exception type and context."""
    error_str = str(error)
    context = context or {}
    
    # Connection-related errors
    if "connection" in error_str.lower() or "timeout" in error_str.lower():
        catalog_name = context.get("catalog", "catalog")
        return (
            f"Failed to connect to {catalog_name}. "
            f"Please check that the catalog service is running and accessible. "
            f"Original error: {error_str}"
        )
    
    # Authentication errors
    if any(word in error_str.lower() for word in ["auth", "unauthorized", "forbidden", "credentials"]):
        return (
            f"Authentication failed. Please check your credentials and permissions. "
            f"Original error: {error_str}"
        )
    
    # Table/namespace not found
    if "not found" in error_str.lower() or "does not exist" in error_str.lower():
        if "table" in operation.lower():
            table_name = context.get("table", "table")
            return (
                f"Table '{table_name}' not found. "
                f"Use 'icectl tables list --db <namespace>' to see available tables. "
                f"Original error: {error_str}"
            )
        elif "namespace" in operation.lower():
            return (
                f"Namespace not found. "
                f"Use 'icectl db list' to see available namespaces. "
                f"Original error: {error_str}"
            )
    
    # Warehouse/catalog errors
    if "warehouse" in error_str.lower():
        return (
            f"Warehouse configuration error. "
            f"Please check your catalog configuration and warehouse settings. "
            f"Original error: {error_str}"
        )
    
    # Schema/format errors  
    if any(word in error_str.lower() for word in ["schema", "format", "parse", "invalid"]):
        return (
            f"Data format or schema error. The table may have an incompatible schema. "
            f"Original error: {error_str}"
        )
    
    # Generic error with helpful context
    return f"Failed to {operation}: {error_str}"


def suggest_troubleshooting_steps(operation: str, error: Exception) -> list[str]:
    """Suggest troubleshooting steps based on the operation and error."""
    error_str = str(error).lower()
    suggestions = []
    
    if "connection" in error_str or "timeout" in error_str:
        suggestions.extend([
            "Check that Docker services are running: docker-compose ps",
            "Verify catalog endpoint is accessible: curl http://localhost:8181/health", 
            "Ensure correct ICECTL_CONFIG path is set",
            "Try running 'make infra-up' to start services",
        ])
    
    elif "not found" in error_str or "does not exist" in error_str:
        if "table" in operation.lower():
            suggestions.extend([
                "List available tables: icectl tables list --db <namespace>",
                "Check the table name spelling and namespace",
                "Verify the table exists in the expected namespace",
            ])
        else:
            suggestions.extend([
                "List available namespaces: icectl db list",
                "Check namespace name spelling",
                "Run seeder to create sample data: make infra-seed",
            ])
    
    elif "warehouse" in error_str:
        suggestions.extend([
            "Check warehouse configuration in your config file",
            "Verify MinIO/S3 storage is accessible",
            "Ensure warehouse permissions are correctly set",
            "Try recreating the warehouse with 'make infra-reset'",
        ])
    
    elif "auth" in error_str or "credentials" in error_str:
        suggestions.extend([
            "Check storage credentials in your configuration",
            "Verify S3/MinIO access keys are correct",
            "Ensure the catalog has proper permissions",
        ])
    
    if not suggestions:
        suggestions.extend([
            "Check the logs with -v/--verbose flag for more details",
            "Verify your configuration file is correct",
            "Try running 'make infra-reset' to restart services",
        ])
    
    return suggestions


def format_config_error(error: Exception) -> str:
    """Format configuration-related error messages."""
    error_str = str(error)
    
    if "no config file found" in error_str.lower():
        return (
            "No configuration file found. Please either:\n"
            "  • Set ICECTL_CONFIG=/path/to/config.yaml, or\n"
            "  • Create ~/.config/icectl/config.yaml\n"
            "\n"
            "See the README for configuration examples."
        )
    
    if "catalog not found" in error_str.lower():
        return (
            f"Catalog configuration error: {error_str}\n"
            "Check your config file and ensure the catalog is properly defined."
        )
    
    return f"Configuration error: {error_str}"