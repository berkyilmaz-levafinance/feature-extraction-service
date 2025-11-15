"""
Feature Extraction Mikroservisi icin config dosyasi. Bu dosya;
.env dosyasındaki tüm ayarları okur ve yönetir
"""
## env'deki verileri okumak icin kullandigimiz sistem
# Kutuphaneler
import os
from typing import Optional
from dataclasses import dataclass
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Konfigurasyon:
    """
    Tum env degiskenleri buradan okunur ve dogrulamasi yapilir
    """
    # Genel AYARLAR
    ortam: str = os.getenv("ENVIRONMENT", "development")

    # Veritabani AYARLARI
    vt_host: str = os.getenv("DATABASE_HOST", "")
    vt_port: int = int(os.getenv("DATABASE_PORT", "5432"))
    vt_isim: str = os.getenv("DATABASE_NAME", "")
    vt_kullanici: str = os.getenv("DATABASE_USER", "")
    vt_sifre: str = os.getenv("DATABASE_PASSWORD", "")

    # ==================== RABBITMQ AYARLARI ====================
    # CloudAMQP URL'i
    rabbitmq_url: str = os.getenv("RABBITMQ_URL", "")

    # Queue isimleri
    rabbitmq_feature_queue: str = os.getenv("RABBITMQ_QUEUE_NAME", "feature_extraction")

    # RETRY AYARLARI
    rabbitmq_max_retries: int = int(os.getenv("RABBITMQ_MAX_RETRIES", "3"))
    rabbitmq_retry_delay: int = int(os.getenv("RABBITMQ_RETRY_DELAY", "1"))

    # BAGLANTI AYARLARI
    rabbitmq_connection_timeout: int = int(os.getenv("RABBITMQ_CONNECTION_TIMEOUT", "30"))
    rabbitmq_heartbeat: int = int(os.getenv("RABBITMQ_HEARTBEAT", "60"))
    rabbitmq_prefetch_count: int = int(os.getenv("RABBITMQ_PREFETCH_COUNT", "1"))

    # Rate Limiting - CloudAMQP free tier: 4 req/min
    rabbitmq_rate_limit: int = int(os.getenv("RABBITMQ_RATE_LIMIT", "4"))

    # ==================== XAI Grok AYARLARI ====================
    xai_api_key: Optional[str] = os.getenv("XAI_API_KEY", "")
    xai_base_url: str = os.getenv("XAI_BASE_URL", "https://api.x.ai/v1")
    xai_model: str = os.getenv("XAI_MODEL", "grok-beta")
    xai_max_tokens: int = int(os.getenv("XAI_MAX_TOKENS", "2000"))
    xai_temperature: float = float(os.getenv("XAI_TEMPERATURE", "0.1"))

    # Grok Rate Limiting
    grok_rate_limit_requests: int = int(os.getenv("GROK_RATE_LIMIT_REQUESTS", "4"))
    grok_rate_limit_period: int = int(os.getenv("GROK_RATE_LIMIT_PERIOD", "60"))

    # ==================== WORKER AYARLARI ====================
    # Calisanlarin kac haber okuyacagini soyler
    worker_prefetch_count: int = int(os.getenv("WORKER_PREFETCH_COUNT", "1"))
    max_retry_count: int = int(os.getenv("MAX_RETRY_COUNT", "3"))
    retry_delay: int = int(os.getenv("RETRY_DELAY", "60"))

    # ==================== QUEUE YONETICI AYARLARI ====================
    queue_manager_poll_interval: int = int(os.getenv("QUEUE_MANAGER_POLL_INTERVAL", "30"))

    # ==================== GERI YUKLEME AYARLARI ====================
    recovery_check_interval: int = int(os.getenv("RECOVERY_CHECK_INTERVAL", "600"))
    recovery_timeout_minutes: int = int(os.getenv("RECOVERY_TIMEOUT_MINUTES", "30"))

    # ==================== LOGGING AYARLARI ====================
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_file: str = os.getenv("LOG_FILE", "")


    # ==================== PROPERTIES ====================
    @property
    def rabbitmq_dlq_queue(self) -> str:
        """Dead Letter Queue ismi - Ana queue'dan türetilir"""
        return f"{self.rabbitmq_feature_queue}_dlq"

    @property
    def min_bekleme_suresi(self) -> float:
        """Her atilan request arasi minimum bekleme suresi (saniye)"""
        return self.grok_rate_limit_period / self.grok_rate_limit_requests

    @property
    def database_string_olustur(self) -> str:
        """Veritabani baglanti stringini olusturur"""
        return (
            f"postgresql://{self.vt_kullanici}:{self.vt_sifre}@"
            f"{self.vt_host}:{self.vt_port}/{self.vt_isim}"
        )

    @property
    def rabbit_dlq_queue(self) -> str:
        """
        .env'de olmasa bile otomatik olarak olusturur
        Ana queue adindan turetir
        """
        return f"{self.rabbitmq_feature_queue}_dlq"

    # ==================== GROK CLIENT PROPERTIES ====================
    @property
    def grok_api_key(self) -> str:
        """Grok API key"""
        return self.xai_api_key

    @property
    def grok_base_url(self) -> str:
        """Grok API base URL"""
        return self.xai_base_url

    @property
    def grok_model(self) -> str:
        """Grok model name"""
        return self.xai_model

    @property
    def grok_max_tokens(self) -> int:
        """Grok max tokens"""
        return self.xai_max_tokens

    @property
    def grok_temperature(self) -> float:
        """Grok temperature"""
        return self.xai_temperature

    @property
    def grok_timeout(self) -> int:
        """Grok request timeout (saniye)"""
        return 30  # Sabit deger, isterseniz .env'e eklenebilir

    @property
    def grok_max_retries(self) -> int:
        """Grok max retry sayisi"""
        return self.max_retry_count

    @property
    def grok_rate_limit(self) -> int:
        """Grok rate limit (requests per minute)"""
        return self.grok_rate_limit_requests

    # DOGRULAMA METODLARI
    def dogrula(self) -> bool:
        """"
        Konfiglerin var olup olmadigini kontrol eder
        Eksik varsa hata verir
        """
        zorunlu = {
            "DATABASE_HOST": self.vt_host,
            "DATABASE_NAME": self.vt_isim,
            "DATABASE_USER": self.vt_kullanici,
            'DATABASE_PASSWORD': self.vt_sifre,
            'XAI_API_KEY': self.xai_api_key,
            'RABBITMQ_URL': self.rabbitmq_url,
        }

        # Eksikler varsa bunlari cikar
        eksikler = [isim for isim, deger in zorunlu.items() if not deger]

        if eksikler:
            raise ValueError(
                f" Eksik environment variable'lar: {', '.join(eksikler)}\n"
                f" Lutfen .env dosyasini kontrol edin"
            )
        return True

    # rabbitmq_utils doyasinda kullanilan parse fonskiyonu
    # Icerden gelen bilgileri pars ederek bize dondurur
    def parse_rabbitmq_url(self) -> dict:
        """
        RabbitMQ URL'inden host, port vb. bilgileri çıkar
        Güvenli loglama için kullanılır (şifreyi loglamadan)

        Returns:
            dict: URL bilgileri (scheme, host, port, username, vhost)

        Example:
            >>> url_info = config.parse_rabbitmq_url()
            >>> print(url_info['host'])
            'cougar.rmq.cloudamqp.com'
        """
        if not self.rabbitmq_url:
            return {
                "scheme": "amqp",
                "host": "localhost",
                "port": 5672,
                "username": "guest",
                "vhost": "/"
            }

        parsed = urlparse(self.rabbitmq_url)
        return {
            "scheme": parsed.scheme,  # amqps
            "host": parsed.hostname,  # cougar.rmq.cloudamqp.com
            "port": parsed.port or (5671 if parsed.scheme == "amqps" else 5672),
            "username": parsed.username,  # yqhkuwum
            "vhost": parsed.path[1:] if parsed.path else "/"  # yqhkuwum
        }

    def __post_init__(self):
        """Dataclass oluşturulduktan sonra doğrulama yap"""
        self.dogrula()

config = Konfigurasyon()
