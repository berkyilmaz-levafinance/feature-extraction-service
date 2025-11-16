"""
Grok API işlemleri için özel exceptionlar
"""

class GrokException(Exception):
    """Base exception for Grok API operations"""
    pass

# Grok baglanti hatasi
class GrokConnectionError(GrokException):
    """
    Grok API baglanti hatası
    """
    pass

class GrokAuthError(GrokException):
    """
    API key authentication hatası
    """
    pass

class GrokRateLimitError(GrokException):
    """
    Rate limit aşım hatası

    """
    pass

class GrokTimeoutError(GrokException):
    """
    Request timeout hatası
    """
    pass

class GrokParseError(GrokException):
    """
    Response parse hatası -tr
    Response parsing error -eng
    """
    pass


class GrokAPIError(GrokException):
    """
    Genel API hatası
    """
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"API Error {status_code}: {message}")

