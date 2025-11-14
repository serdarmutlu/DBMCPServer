"""
Client manager for tracking client sessions and handling their database connections.
"""

import asyncio
import uuid
import logging
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ClientSession:
    """Represents a client session."""
    
    id: str
    connection_time: datetime
    last_activity: datetime
    database_connection_id: Optional[str] = None
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def update_activity(self):
        """Update the last activity timestamp."""
        self.last_activity = datetime.now()
    
    def is_expired(self, timeout_minutes: int = 30) -> bool:
        """Check if the session has expired."""
        if not self.active:
            return True
        
        timeout_threshold = datetime.now() - timedelta(minutes=timeout_minutes)
        return self.last_activity < timeout_threshold


class ClientManager:
    """Manages client sessions and their database connections."""
    
    def __init__(self, session_timeout_minutes: int = 30):
        self.sessions: Dict[str, ClientSession] = {}
        self.session_timeout_minutes = session_timeout_minutes
        self.cleanup_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        
