"""
Grok API işlemleri için Client Class
xAI Grok API uyumlu
"""
"""
Yazilimciya not;
Bu client, Türkçe haber metinlerinden semantic feature extraction yapar.
RabbitMQ utils ile aynı pattern'i takip eder - detaylı Türkçe açıklamalar içerir.
"""
import httpx
import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional
from datetime import datetime

from ..config import config
from .grok_exceptions import (
    GrokConnectionError,
    GrokAuthError,
    GrokRateLimitError,
    GrokTimeoutError,
    GrokParseError,
    GrokAPIError
)

logger = logging.getLogger(__name__)

# Grok Client classini olustur
class GrokClient:
    """
    Feature Extraction sistemi icin GROK API client
    Özellikler:
    - Async HTTP client (httpx)
    - Rate limiting
    - Retry mekanizması
    - Error handling
    - JSON parse validation
    - Metrik toplama
    """

    def __init__(self):
        """
        Grok Client'i başlatır
        Config'den gerekli ayarları okur ve client'i hazırlar
        """

        # API kimlik bilgileri
        # Config dosyasindan XAI API key'i aliyoruz
        self.api_key = config.grok_api_key

        # Grok API'nin base URL'i
        # Örnek: https://api.x.ai/v1
        self.base_url = config.grok_base_url

        # HTTP request ayarları
        # Timeout: Request'in maksimum ne kadar sürebileceği
        # Su an icin 30 saniye olarak ayarli, daha sonra degistirebiliriz
        self.timeout = config.grok_timeout

        # Max retries: Hata olursa kaç kere tekrar deneyelim?
        self.max_retries = config.grok_max_retries  # 3

        # Rate limiting ayarları
        # Dakikada kaç request atabiliriz?
        # Free tier için limit var, bunu aşmamak için kontrol ederiz
        self.rate_limit = config.grok_rate_limit  # requests per minute

        # Son request zamanı
        # Rate limiting hesaplamak için kullanılır
        self.last_request_time = 0

        # Async HTTP client objesi
        # Başlangıçta None, connect() ile oluşturulur
        self.client: Optional[httpx.AsyncClient] = None

        # Metrikler - İstatistik toplama
        # Bu bilgiler monitoring ve debugging için kullanılır
        self.metrics = {
            "requests_sent": 0,           # Toplam gönderilen request sayısı
            "requests_success": 0,         # Başarılı request sayısı
            "requests_failed": 0,          # Başarısız request sayısı
            "rate_limit_hits": 0,          # Rate limit nedeniyle bekleme sayısı
            "parse_errors": 0,             # JSON parse hata sayısı
            "total_tokens_used": 0         # Toplam kullanılan token sayısı
        }

        # İlk log mesajı
        logger.info("GROK --- INIT --- Client oluşturuldu (xAI)")


    async def connect(self):
        """
        HTTP client'i başlatır

        httpx.AsyncClient oluşturur ve connection pooling ayarlar
        Bu metod servis başlarken bir kere çağrılır

        Returns:
            bool: Başarılıysa True
        Raises:
            GrokConnectionError: Client başlatılamazsa
        """
        try:
            # Eğer client zaten varsa tekrar oluşturmaya gerek yok
            if self.client is None:
                # Async HTTP client oluştur
                # timeout: Request'lerin maksimum süresi
                # limits: Connection pooling ayarları
                self.client = httpx.AsyncClient(
                    timeout=self.timeout,
                    limits=httpx.Limits(
                        max_connections=10,         # Aynı anda max 10 connection
                        max_keepalive_connections=5  # Keep-alive için max 5 connection
                    )
                )
                logger.info("GROK --- CONNECT --- HTTP client başlatıldı")
            return True

        except Exception as e:
            # Beklenmeyen hata - client oluşturulamadı
            logger.error(f"GROK --- CONNECT ERROR --- {e}", exc_info=True)
            raise GrokConnectionError(f"HTTP client başlatılamadı: {e}") from e

    async def disconnect(self):
        """
        HTTP client'i kapatır

        Servis kapanırken connection'ları düzgünce kapatır.
        Resource leak'i önlemek için önemli.
        """
        try:
            if self.client is not None:
                # Client'i kapat
                await self.client.aclose()
                self.client = None
                logger.info("GROK --- DISCONNECT --- HTTP client kapatıldı")
        except Exception as e:
            # Kapatma hatası - critical değil, logla ve devam et
            logger.error(f"GROK --- DISCONNECT ERROR --- {e}", exc_info=True)

    async def _send_request(self, news_text:str, title:str = "") -> Dict[str, Any]:
        """
        Grok API'ye request gönderir ve response'u parse eder
        Bu metod:
        Prompt'u hazırlar (title + content ile)
        API request payload'ını oluşturur
        HTTP POST request gönderir
        Response'u kontrol eder
        JSON parse eder
        Ardindan da validate eder

        Args:
            news_text: Haber içeriği (string)
            title: Haber başlığı (opsiyonel, string)

        Returns:
            dict: Parsed ve validate edilmiş features

        Raises:
            GrokAuthError: API key hatası
            GrokRateLimitError: Rate limit aşıldı
            GrokAPIError: API hatası
            GrokTimeoutError: Timeout
            GrokParseError: JSON parse hatası
        """

        try:
            # Promptu hazirlayalim
            # NOT: EXTRACTION_PROMPT'ta {title} ve {content} placeholder'ları var
            from .prompts import GROK_EXTRACTION_PROMPT

            prompt = GROK_EXTRACTION_PROMPT.format(
                title=title if title else "Baslik yok",
                content = news_text[:5000] # Ilk 5000 karakteri
                # Su anlik kelime siniri icin daha sonra arttirilabilir
            )

            # API request payload olustur
            # GROK-4 API formati
            payload= {
                "model" : config.grok_model,
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a financial news feature extraction system. Output only valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": config.grok_temperature,
                "max_tokens": config.grok_max_tokens,
                "response_format": {"type": "json_object"}
            }

            # HTTP headerlari
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            # Metrikleri güncelle
            self.metrics["requests_sent"] += 1

            # Request zamanını ölç
            start_time = time.time()

            # HTTP POST request gönder
            response = await self.client.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload
            )
            request_duration = time.time() - start_time

            # Response status kontrolü
            # Her status code için özel exception fırlat
            if response.status_code == 401:
                # Authentication hatası - API key yanlış
                raise GrokAuthError("Invalid API key. Check XAI_API_KEY in .env file")

            elif response.status_code == 429:
                # Rate limit aşıldı
                # Free tier için dakikada X request limiti var
                raise GrokRateLimitError("Rate limit exceeded. Wait before retrying.")

            elif response.status_code >= 500:
                # Server error - Grok API'de sorun var
                raise GrokAPIError(response.status_code,
                    f"Grok API server error: {response.text}")

            elif response.status_code != 200:
                # Diğer hatalar
                raise GrokAPIError(
                    response.status_code,
                    f"Unexpected API response: {response.text}")

            # Response'u parse et
            response_data = response.json()

            # Token kullanımını logla
            # Grok API response'unda usage bilgisi var
            if "usage" in response_data:
                tokens_used = response_data["usage"].get("total_tokens", 0)
                self.metrics["total_tokens_used"] += tokens_used
                logger.debug(
                    f"GROK --- TOKENS --- {tokens_used} token kullanıldı "
                    f"(toplam: {self.metrics['total_tokens_used']})"
            )
            # Content'i çıkar
            # Grok response formatı: response_data["choices"][0]["message"]["content"]
            if "choices" not in response_data or len(response_data["choices"]) == 0:
                raise GrokParseError("Response'da 'choices' alanı yok")

            content = response_data["choices"][0]["message"]["content"]

            # JSON parse et
            try:
                # Content string olarak JSON gelir
                # Parse edip dict'e çevir
                features = json.loads(content)

            except json.JSONDecodeError as e:
                # JSON parse hatası
                # Grok bazen markdown code block ile döndürebilir
                # Örn: ```json\n{...}\n```
                # Bunu temizle ve tekrar dene

                self.metrics["parse_errors"] += 1

                logger.error(
                    f"GROK --- PARSE ERROR --- JSON parse hatası. "
                    f"Raw content: {content[:200]}..."
                )

                # Markdown temizleme denemesi
                cleaned_content = content.strip()

                # ```json ve ``` işaretlerini kaldır
                if cleaned_content.startswith("```"):
                    # İlk satırı at (```json)
                    lines = cleaned_content.split("\n")
                    if len(lines) > 2:
                        # İlk ve son satırları at
                        cleaned_content = "\n".join(lines[1:-1])

                # Tekrar dene
                try:
                    features = json.loads(cleaned_content)
                except json.JSONDecodeError:
                    # Hala parse edilemedi - gerçek hata
                    raise GrokParseError(
                        f"JSON parse edilemedi: {e}\nContent: {content[:5000]}"
                    ) from e

            # Feature validation
            # Dönen JSON'un formatını kontrol et
            self._validate_features(features)

            # Başarılı - request süresini logla
            logger.debug(f"GROK --- TIMING --- Request süresi: {request_duration:.2f}s")

            return features

        except httpx.TimeoutException as e:
            # Request timeout - network yavaş veya Grok API yanıt vermiyor
            raise GrokTimeoutError(f"Request timeout ({self.timeout}s): {e}") from e

        except httpx.ConnectError as e:
            # Bağlantı hatası - internet yok veya Grok API down
            raise GrokConnectionError(f"Connection error: {e}") from e

        except (GrokAuthError, GrokRateLimitError, GrokParseError, GrokAPIError):
            # Zaten tanımlanmış exceptionlar - tekrar raise et
            raise

        except Exception as e:
            # Beklenmeyen hata
            raise GrokAPIError(500, f"Unexpected error: {e}") from e

    # GROK'tan gelen JSON dosyasini validate ediyoruz
    # Eger sikinti varsa hemen RL'de kullanilmak uzere yapilmak kotu olur
    def _validate_features(self, features: Dict[str, Any]):
        """
        Grok'tan dönen features'ın doğru formatta olduğunu kontrol eder

        Bu metod:
        1. Required field'ların varlığını kontrol eder
        2. Data type'ları kontrol eder
        3. Value range'leri kontrol eder (örn: confidence 0.00-1.00)
        4. Enum değerlerini kontrol eder (örn: duygu sadece belirli değerler)

        Args:
            features: Grok'tan dönen parsed JSON dict

        Raises:
            GrokParseError: Format yanlışsa veya required field eksikse

        Note:
            Bu validation RL model için critical - hatalı data noise yaratır
        """

        # Zorunlu alanlari gosterelim
        required_fields = [
            # Entities
            "unlu_isimler",
            "firmalar",
            "ulkeler",
            "kurumlar",

            # Classification
            "is_makroekonomi",
            "is_mikroekonomi",
            "alt_kategori",
            "guven_skoru_makro",

            # Event
            "olay_tipi",
            "etki_yonu",
            "etkilenen_varlik_sinifi",
            "piyasa_etki_tahmini",
            "etki_degeri_confidence",

            # Sentiment
            "duygu",
            "duygu_skoru",
            "ton",
            "aciliyet",

            # Surprise
            "surpriz_mi",
            "surpriz_confidence",

            # Temporal
            "etki_suresi",
            "gelecege_yonelik_mi",
            "gelecek_confidence",

            # Numeric
            "sayisal_degerler",
            "beklenti_karsilama"
        ]

        # Ilk olarak eksik var mi diye kontrol et
        missing_fields = [field for field in required_fields if field not in features]

        # Eger eksik varsa bildir
        if missing_fields:
            raise GrokParseError(
                f"Eksik zorunlu alanlar: {', '.join(missing_fields)}"
            )

        logger.debug("GROK --- VALIDATE --- Tüm required field'lar mevcut")

        #DATA TYPE VALIDATION

        # Arrays (list olmalı, null olmamalı)
        array_fields = ["unlu_isimler", "firmalar", "ulkeler", "kurumlar", "etkilenen_varlik_sinifi"]
        for field in array_fields:
            if not isinstance(features[field], list):
                raise GrokParseError(
                    f"'{field}' list olmalı, {type(features[field])} geldi"
                )

        # Booleans
        boolean_fields = ["is_makroekonomi", "is_mikroekonomi", "surpriz_mi", "gelecege_yonelik_mi"]
        for field in boolean_fields:
            if not isinstance(features[field], bool):
                raise GrokParseError(
                    f"'{field}' boolean olmalı, {type(features[field])} geldi"
                )

        # Floats (confidence skorları)
        float_fields = ["guven_skoru_makro", "duygu_skoru", "etki_degeri_confidence",
                        "surpriz_confidence", "gelecek_confidence"]
        for field in float_fields:
            if not isinstance(features[field], (int, float)):
                raise GrokParseError(
                    f"'{field}' float olmalı, {type(features[field])} geldi"
                )

        # Integer
        if not isinstance(features["alt_kategori"], int):
            raise GrokParseError(
                f"'alt_kategori' int olmalı, {type(features['alt_kategori'])} geldi"
            )

        # Strings
        string_fields = ["olay_tipi", "etki_yonu", "duygu", "ton", "aciliyet",
                         "etki_suresi", "beklenti_karsilama"]
        for field in string_fields:
            if not isinstance(features[field], str):
                raise GrokParseError(
                    f"'{field}' string olmalı, {type(features[field])} geldi"
                )

        # Objects (dict olmalı)
        if not isinstance(features["piyasa_etki_tahmini"], dict):
            raise GrokParseError(
                f"'piyasa_etki_tahmini' dict olmalı, {type(features['piyasa_etki_tahmini'])} geldi"
            )
        if not isinstance(features["sayisal_degerler"], dict):
            raise GrokParseError(
                f"'sayisal_degerler' dict olmalı, {type(features['sayisal_degerler'])} geldi"
            )

        logger.debug("GROK --- VALIDATE --- Data type'lar doğru")

        #   VALUE RANGE VALIDATION


        # Confidence skorları: 0.00 - 1.00 arasında olmalı
        for field in float_fields:
            value = features[field]
            if not (0.0 <= value <= 1.0):
                raise GrokParseError(
                    f"'{field}' 0.00-1.00 arasında olmalı, {value} geldi"
                )
        # alt_kategori: 0-13 arasında olmalı
        if not (0 <= features["alt_kategori"] <= 13):
            raise GrokParseError(
                f"'alt_kategori' 0-13 arasında olmalı, {features['alt_kategori']} geldi"
            )
        logger.debug("GROK --- VALIDATE --- Value range'ler doğru")


        #   ENUM VALIDATION (İzin verilen değerler)


        # etki_yonu: Sadece belirli değerler
        allowed_etki_yonu = ["pozitif", "negatif", "notr"]
        if features["etki_yonu"] not in allowed_etki_yonu:
            raise GrokParseError(
                f"'etki_yonu' şunlardan biri olmalı: {allowed_etki_yonu}, "
                f"'{features['etki_yonu']}' geldi"
            )
        # duygu: Sadece belirli değerler
        allowed_duygu = ["pozitif", "negatif", "notr_pozitif", "notr_negatif", "notr"]
        if features["duygu"] not in allowed_duygu:
            raise GrokParseError(
                f"'duygu' şunlardan biri olmalı: {allowed_duygu}, "
                f"'{features['duygu']}' geldi"
            )
        # ton: Sadece belirli değerler
        allowed_ton = ["resmi", "teknik", "populer", "belirsiz"]
        if features["ton"] not in allowed_ton:
            raise GrokParseError(
                f"'ton' şunlardan biri olmalı: {allowed_ton}, "
                f"'{features['ton']}' geldi"
            )
        # aciliyet: Sadece belirli değerler
        allowed_aciliyet = ["yuksek", "orta", "dusuk"]
        if features["aciliyet"] not in allowed_aciliyet:
            raise GrokParseError(
                f"'aciliyet' şunlardan biri olmalı: {allowed_aciliyet}, "
                f"'{features['aciliyet']}' geldi"
            )
        # etki_suresi: Sadece belirli değerler
        allowed_etki_suresi = ["kisa_vadeli", "uzun_vadeli", "belirsiz"]
        if features["etki_suresi"] not in allowed_etki_suresi:
            raise GrokParseError(
                f"'etki_suresi' şunlardan biri olmalı: {allowed_etki_suresi}, "
                f"'{features['etki_suresi']}' geldi"
            )
        # beklenti_karsilama: Sadece belirli değerler
        allowed_beklenti = ["ustunde", "altinda", "beklendiği_gibi", "belirsiz"]
        if features["beklenti_karsilama"] not in allowed_beklenti:
            raise GrokParseError(
                f"'beklenti_karsilama' şunlardan biri olmalı: {allowed_beklenti}, "
                f"'{features['beklenti_karsilama']}' geldi"
            )
        logger.debug("GROK --- VALIDATE --- Enum değerler doğru")

        #   KURUMLAR VALIDATION (ID kontrolü)

        # kurumlar array'inde her eleman {"ad": str, "id": int} formatında olmalı
        for i, kurum in enumerate(features["kurumlar"]):
            if not isinstance(kurum, dict):
                raise GrokParseError(
                    f"kurumlar[{i}] dict olmalı, {type(kurum)} geldi"
                )
            if "ad" not in kurum or "id" not in kurum:
                raise GrokParseError(
                    f"kurumlar[{i}] 'ad' ve 'id' alanları içermeli"
                )
            if not isinstance(kurum["ad"], str):
                raise GrokParseError(
                    f"kurumlar[{i}].ad string olmalı"
                )
            if not isinstance(kurum["id"], int):
                raise GrokParseError(
                    f"kurumlar[{i}].id int olmalı"
                )

        logger.debug("GROK --- VALIDATE --- Kurumlar formatı doğru")

        #   VARLIK SINIFI VALIDATION

        # etkilenen_varlik_sinifi: Sadece izin verilen asset'ler
        allowed_assets = ["TL", "BIST100", "USDTRY", "EURTRY", "ALTIN", "BRENT", "tahvil"]

        for asset in features["etkilenen_varlik_sinifi"]:
            if asset not in allowed_assets:
                raise GrokParseError(
                    f"'etkilenen_varlik_sinifi' içinde geçersiz asset: '{asset}'. "
                    f"İzin verilenler: {allowed_assets}"
                )

        # piyasa_etki_tahmini: Key'ler izin verilen asset'lerden olmalı
        for asset in features["piyasa_etki_tahmini"].keys():
            if asset not in allowed_assets:
                raise GrokParseError(
                    f"'piyasa_etki_tahmini' içinde geçersiz asset key: '{asset}'. "
                    f"İzin verilenler: {allowed_assets}"
                )

        # piyasa_etki_tahmini: Value'lar izin verilen direction'lardan olmalı
        allowed_directions = ["guclenme", "zayiflama", "stabil", "pozitif", "negatif"]
        for asset, direction in features["piyasa_etki_tahmini"].items():
            if direction not in allowed_directions:
                raise GrokParseError(
                    f"'piyasa_etki_tahmini[{asset}]' geçersiz değer: '{direction}'. "
                    f"İzin verilenler: {allowed_directions}"
                )
        logger.debug("GROK --- VALIDATE --- Varlık sınıfı değerleri doğru")
        logger.debug("GROK --- VALIDATE --- Tüm validation kontrolleri geçti ")

    # Development icin test fonskiyon*****
    async def _apply_rate_limit(self):
        """
        Rate limiting uygular

        Free tier API'ler için dakikada X request limiti var
        Bu metod her request öncesinde çağrılır ve gerekirse bekler.

        Nasıl çalışır:
        1. Son request'ten bu yana geçen süreyi hesaplar
        2. Minimum interval'i hesaplar (60 saniye / rate_limit)
        3. Eğer çok hızlı request atılıyorsa bekler
        4. Metriği günceller

        Example:
            rate_limit = 10 (dakikada 10 request)
            min_interval = 60 / 10 = 6 saniye
            → Her request arasında minimum 6 saniye bekle

        Returns:
            None
        """

        # Rate limit 0 ise (unlimited) → skip
        # Production'da unlimited olabilir, development'ta limit var
        if self.rate_limit == 0:
            return

        # Şu anki zaman
        current_time = time.time()

        # Son request'ten bu yana geçen süre (saniye)
        time_since_last = current_time - self.last_request_time

        # Minimum interval hesapla
        # Örnek: 60 saniye / 10 request = 6 saniye interval
        min_interval = 60.0 / self.rate_limit

        # Eğer çok hızlı request atılıyorsa bekle
        if time_since_last < min_interval:
            # Beklemesi gereken süreyi hesapla
            wait_time = min_interval - time_since_last

            logger.debug(
                f"GROK --- RATE LIMIT --- {wait_time:.1f}s bekleniyor "
                f"(rate limit: {self.rate_limit} req/min)"
            )

            # Async olarak bekle
            # Bu süre zarfında başka task'lar çalışabilir
            await asyncio.sleep(wait_time)

            # Metriği güncelle
            # Bu bilgi monitoring için önemli - rate limit'e ne kadar takılıyoruz?
            self.metrics["rate_limit_hits"] += 1

        # Son request zamanını güncelle
        self.last_request_time = time.time()

    # ANA FONKSIYONUMUZ
    # Haber metnini alip feature extraction yapar

    async def extract_features(self, news_text: str, title: str = "") -> Dict[str, Any]:
        """
        A
        Haber metninden feature extraction yapar

        Args:
            news_text: Haber içeriği
            title: Haber başlığı (opsiyonel)

        Returns:
            dict: Extracted features
        """

        # Client kontrolü
        if self.client is None:
            await self.connect()

        # Rate limiting uygula
        await self._apply_rate_limit()

        # Retry loop
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    f"GROK --- REQUEST --- Feature extraction başlıyor "
                    f"(deneme: {attempt}/{self.max_retries})"
                )
                # Request gönder
                features = await self._send_request(
                    news_text=news_text,
                    title=title
                )
                # Başarılı
                self.metrics["requests_success"] += 1

                logger.info(
                    f"GROK --- SUCCESS --- Feature extraction tamamlandı "
                    f"(deneme: {attempt}/{self.max_retries})"
                )
                return features

            except (GrokTimeoutError, GrokConnectionError) as e:
                # Retry yapılabilir hatalar
                last_exception = e
                self.metrics["requests_failed"] += 1

                if attempt < self.max_retries:
                    # Exponential backoff
                    wait_time = 2 ** attempt

                    logger.warning(
                        f"GROK --- RETRY --- Hata: {e}. "
                        f"{wait_time}s bekleyip tekrar denenecek..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    # Max retry aşıldı
                    logger.error(f"GROK --- FAILED --- Max retry aşıldı: {e}")
                    raise

            except (GrokAuthError, GrokParseError, GrokAPIError) as e:
                # Retry yapılamaz hatalar - hemen fırlat
                self.metrics["requests_failed"] += 1
                logger.error(f"GROK --- ERROR --- {e}", exc_info=True)
                raise

        # Eğer buraya geldiyse, tüm retry'ler başarısız
        if last_exception:
            raise last_exception

    def get_metrics(self) -> Dict[str, Any]:
        """
        Anlık metrikleri döndürür

        Returns:
            dict: Metrik bilgileri
        """
        return {
            **self.metrics,
            "timestamp": datetime.now().isoformat()
        }




































