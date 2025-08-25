"""Parse and execute vim-style commands for TUI."""

import shlex
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class CommandType(Enum):
    """Types of commands supported in TUI."""
    TABLE = "table"
    CATALOG = "catalog" 
    DATABASE = "database"
    REFRESH = "refresh"
    QUIT = "quit"
    HELP = "help"
    UNKNOWN = "unknown"


@dataclass
class ParsedCommand:
    """Result of parsing a command string."""
    command_type: CommandType
    args: List[str]
    raw_input: str
    error: Optional[str] = None


class CommandParser:
    """Parser for vim-style TUI commands."""
    
    # Command aliases mapping
    ALIASES = {
        "t": "table",
        "c": "catalog", 
        "d": "database",
        "r": "refresh",
        "q": "quit",
        "?": "help"
    }
    
    def parse(self, input_text: str) -> ParsedCommand:
        """Parse command input and return parsed command."""
        input_text = input_text.strip()
        
        # Empty input
        if not input_text:
            return ParsedCommand(
                command_type=CommandType.UNKNOWN,
                args=[],
                raw_input=input_text,
                error="Empty command"
            )
        
        # Must start with : for command mode
        if not input_text.startswith(":"):
            return ParsedCommand(
                command_type=CommandType.UNKNOWN,
                args=[],
                raw_input=input_text,
                error="Commands must start with ':'"
            )
        
        # Remove : prefix
        command_line = input_text[1:].strip()
        
        if not command_line:
            return ParsedCommand(
                command_type=CommandType.UNKNOWN,
                args=[],
                raw_input=input_text,
                error="No command after ':'"
            )
        
        # Parse arguments using shell-like parsing
        try:
            parts = shlex.split(command_line)
        except ValueError as e:
            return ParsedCommand(
                command_type=CommandType.UNKNOWN,
                args=[],
                raw_input=input_text,
                error=f"Invalid command syntax: {e}"
            )
        
        if not parts:
            return ParsedCommand(
                command_type=CommandType.UNKNOWN,
                args=[],
                raw_input=input_text,
                error="No command specified"
            )
        
        # Get command and resolve aliases
        command = parts[0].lower()
        args = parts[1:] if len(parts) > 1 else []
        
        # Resolve alias
        resolved_command = self.ALIASES.get(command, command)
        
        # Map to command type
        try:
            command_type = CommandType(resolved_command)
        except ValueError:
            return ParsedCommand(
                command_type=CommandType.UNKNOWN,
                args=args,
                raw_input=input_text,
                error=f"Unknown command: {command}"
            )
        
        # Validate command-specific arguments
        error = self._validate_command_args(command_type, args)
        
        return ParsedCommand(
            command_type=command_type,
            args=args,
            raw_input=input_text,
            error=error
        )
    
    def _validate_command_args(self, command_type: CommandType, args: List[str]) -> Optional[str]:
        """Validate arguments for specific command types."""
        if command_type in [CommandType.TABLE, CommandType.CATALOG, CommandType.DATABASE]:
            if not args:
                return f"{command_type.value} command requires a name argument"
            if len(args) > 1:
                return f"{command_type.value} command accepts only one argument"
        
        elif command_type in [CommandType.REFRESH, CommandType.QUIT, CommandType.HELP]:
            if args:
                return f"{command_type.value} command does not accept arguments"
        
        return None
    
    def get_help_text(self) -> str:
        """Get help text for all commands."""
        return """Command Mode Help:

:table <name> (or :t)    - Jump to table details  
:catalog <name> (or :c)  - Switch catalogs
:database <name> (or :d) - Select database
:refresh (or :r)         - Context-aware refresh
:quit (or :q)           - Exit application
:help (or :?)           - Show this help

Search Mode:
Type directly (without :) to filter current view by name
Case-insensitive matching, real-time filtering

Navigation:
↑↓ - Navigate items
Enter - Select item
Esc - Go back/cancel
"""