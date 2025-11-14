"""
Encryption utilities for securely storing database passwords.
"""



import base64
import os
import logging
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from config import settings

logger = logging.getLogger(__name__)


class PasswordEncryption:
    """Handles encryption and decryption of database passwords."""
    
    def __init__(self, encryption_key: str = None):
        """
        Initialize the encryption handler.
        
        Args:
            encryption_key: Base encryption key. If None, uses environment variable or generates one.
        """
        self._cipher = self._get_cipher(encryption_key)
    
    def _get_cipher(self, encryption_key: str = None) -> Fernet:
        """Get or create the cipher for encryption/decryption."""
        if encryption_key:
            # Use provided key
            key = encryption_key.encode()
        else:
            # Try to get from environment variable
            key_str = os.environ.get('MCP_ENCRYPTION_KEY')

            if key_str:
                key = key_str.encode()
            else:
                if settings.encryption_key:
                    key = settings.encryption_key.encode()
                else:
                    # Generate a new key and warn the user
                    key = self._generate_key()
                    logger.warning(
                        "No encryption key provided. Generated a new one. "
                        "Set MCP_ENCRYPTION_KEY environment variable to persist encryption key across restarts."
                    )
        
        # Derive a proper Fernet key from the provided key
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'mcp_database_server_salt',  # Fixed salt for consistency
            iterations=100000,
        )
        fernet_key = base64.urlsafe_b64encode(kdf.derive(key))
        return Fernet(fernet_key)
    
    def _generate_key(self) -> bytes:
        """Generate a new encryption key."""
        return os.urandom(32)
    
    def encrypt_password(self, password: str) -> str:
        """
        Encrypt a password.
        
        Args:
            password: Plain text password to encrypt
            
        Returns:
            Base64 encoded encrypted password
        """
        try:
            encrypted_bytes = self._cipher.encrypt(password.encode())
            return base64.urlsafe_b64encode(encrypted_bytes).decode()
        except Exception as e:
            logger.error(f"Failed to encrypt password: {e}")
            raise ValueError("Password encryption failed")
    
    def decrypt_password(self, encrypted_password: str) -> str:
        """
        Decrypt a password.
        
        Args:
            encrypted_password: Base64 encoded encrypted password
            
        Returns:
            Plain text password
        """
        try:
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_password.encode())
            decrypted_bytes = self._cipher.decrypt(encrypted_bytes)
            return decrypted_bytes.decode()
        except Exception as e:
            logger.error(f"Failed to decrypt password: {e}")
            raise ValueError("Password decryption failed")
    
    def is_encrypted(self, password: str) -> bool:
        """
        Check if a password appears to be encrypted.
        
        Args:
            password: Password string to check
            
        Returns:
            True if the password appears to be encrypted
        """
        try:
            # Try to decode as base64 and decrypt
            base64.urlsafe_b64decode(password.encode())
            return True
        except:
            return False


# Global instance for the application
_password_encryption = None


def get_password_encryption() -> PasswordEncryption:
    """Get the global password encryption instance."""
    global _password_encryption
    if _password_encryption is None:
        _password_encryption = PasswordEncryption()
    return _password_encryption


def encrypt_password(password: str) -> str:
    """Convenience function to encrypt a password."""
    return get_password_encryption().encrypt_password(password)


def decrypt_password(encrypted_password: str) -> str:
    """Convenience function to decrypt a password."""
    return get_password_encryption().decrypt_password(encrypted_password)