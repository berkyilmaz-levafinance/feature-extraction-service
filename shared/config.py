"""
Feature Extraction Mikroservisi icin config dosyasi. Bu dosya;
.env dosyasındaki tüm ayarları okur ve yönetir
"""
## env'deki verileri okumak icin kullandigimiz sistem
# Kutuphaneler
import os
from typing import Optional
from dataclasses import dataclass
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

    # XAI Grok AYARLARI
    xai_api_key: Optional[str] = os.getenv("XAI_API_KEY", "")
    xai_base_url: str = os.getenv("XAI_BASE_URL", "https://api.x.ai/v1")
    xai_model: str = os.getenv("XAI_MODEL", "grok-4-fast-non-reasoning")
    xai_max_tokens: int = int(os.getenv("XAI_MAX_TOKENS", "2000"))
    # Su an icin 0.1'de *NOT1: 11/10/2025 - Temp degerlerini karsilastir
    xai_temperature: float = float(os.getenv("XAI_TEMPERATURE", "0.1"))

    #Rate Limiting
    grok_rate_limit_requests: int = int(os.getenv("GROK_RATE_LIMIT_REQUESTS", "4"))
    grok_rate_limit_period: int = int(os.getenv("GROK_RATE_LIMIT_PERIOD", "60"))

    # RABBITMQ AYARLARI
    rabbitmq_url: str = os.getenv("RABBITMQ_URL", "")
    rabbitmq_queue_name: str = os.getenv("RABBITMQ_QUEUE_NAME", "feature_extraction")

    # Worker AYARLARI
    # Calisanlarin kac haber okuyacagini soyler
    # Sistemimiz paralel olarak haberleri worker ile alir - buradan sonra
    # worker_prefetch_count her thread'e* kac haber atanacagini gosterir
    worker_prefetch_count: int = int(os.getenv("WORKER_PREFETCH_COUNT", "1"))
    max_retry_count: int = int(os.getenv("MAX_RETRY_COUNT", "3"))
    retry_delay: int = int(os.getenv("RETRY_DELAY", "60"))

    # QUEUE YONETICI AYARLARI
    queue_manager_poll_interval: int = int(os.getenv("QUEUE_MANAGER_POLL_INTERVAL", "30"))

    # GERI YUKLEME AYARLARI
    recovery_check_interval: int = int(os.getenv("RECOVERY_CHECK_INTERVAL", "600"))
    recovery_timeout_minutes: int = int(os.getenv("RECOVERY_TIMEOUT_MINUTES", "30"))

    # LOGGING AYARLARI
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_file: str = os.getenv("LOG_FILE", "")

    @property
    def min_bekleme_suresi(self) -> float:
        "Her atilan request arasi minimum bekleme suresi(saniye)"
        return self.grok_rate_limit_period / self.grok_rate_limit_requests

    @property
    def database_string_olustur(self) -> str:
        "Veritabani baglanti stringini olusturur"
        return f"postgresql://{self.vt_kullanici}:{self.vt_sifre}@{self.vt_host}:{self.vt_port}/{self.vt_isim}"

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

    def __post_init__(self):
        """Dataclass oluşturulduktan sonra doğrulama yap"""
        self.dogrula()

config = Konfigurasyon()
