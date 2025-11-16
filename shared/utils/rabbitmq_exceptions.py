"""
RabbitMQ islemleri icin ozel exceptionlar
"""

class RabbitMQException(Exception):
    """Base exception for RabbitMQ operations"""
    pass

class ConnectionError(RabbitMQException):
    """RabbitMQ baglanti hatası -tr
       RabbitMQ connection error -eng """
    pass

class PublishError(RabbitMQException):
    """Mesaj gönderme hatası"""
    pass

class QueueDeclarationError(RabbitMQException):
    """Queue tanımlama hatası"""
    pass

class ConsumerError(RabbitMQException):
    """Mesaj tüketme hatası"""
    pass