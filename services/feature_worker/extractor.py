"""
Feature Extractor
Haberlerden Grok ile feature çıkarır ve database'e kaydeder

"""

import logging
import time
import json

from typing import Dict, Any, Optional
from datetime import datetime

from shared.utils.grok_utils import GrokClient
from shared.database import VeriHavuzuYoneticisi as DatabaseManager
from shared.utils.grok_exceptions import (
    GrokConnectionError,
    GrokAuthError,
    GrokParseError,
    GrokAPIError
)
from shared.config import config

logger = logging.getLogger(__name__)

# class olusturalim
class FeatureExtractor:
    """
    Feature Extraction Pipeline

    Bu class şunları yapar:
    1. Database'den article çeker (ID ile)
    2. Grok ile feature extraction yapar
    3. Features'ı database'e kaydeder

    Özellikler:
    - Grok Client kullanır (API communication)
    *** test asamasinda oldugundan daha sonra buralar degisebilir
    - Database Manager kullanır (PostgreSQL)
    - Error handling
    - Processing time tracking
    - Logging
    """

    def __init__(self):
        """
        Feature Extractor'ı başlatır
        Grok Client ve Database Manager'ı oluşturur
        """

        # Grok Client
        # API ile konusur
        self.grok_client = GrokClient()

        # Database yoneticisi - PostgreSQL ile konusulmasini saglar
        self.db_manager = DatabaseManager()

        # Su anlik baglanti durumu false
        self.is_connected = False

        logger.info("EXTRACTOR --- INIT --- Feature Extractor oluşturuldu")

    async def connect(self):
        """
        Grok ve Database bağlantılarını başlatır

        Her iki servise de bağlanır:
        - Grok API (HTTP client)
        - PostgreSQL (asyncpg pool)

        Raises:
            GrokConnectionError: Grok'a bağlanılamazsa
            ConnectionError: Database'e bağlanılamazsa
        """

        try:
            logger.info("EXTRACTOR --- CONNECT --- Bağlantılar kuruluyor...")

            # Grok'a bağlan
            await self.grok_client.connect()
            logger.info("EXTRACTOR --- CONNECT --- Grok client bağlandı")

            # Database'e bağlan
            await self.db_manager.baglanti_ac()
            logger.info("EXTRACTOR --- CONNECT --- Database bağlandı")

            self.is_connected = True
            logger.info("EXTRACTOR --- CONNECT --- Tüm bağlantılar hazır")

        except Exception as e:
            logger.error(f"EXTRACTOR --- CONNECT ERROR --- {e}", exc_info=True)
            raise

    async def disconnect(self):
        """
        Bağlantıları kapatır

        Graceful shutdown için her iki bağlantıyı da kapatır
        """
        try:
            logger.info("EXTRACTOR --- DISCONNECT --- Bağlantılar kapatılıyor...")

            # Grok client'i kapat
            await self.grok_client.disconnect()

            # Database'i kapat
            await self.db_manager.baglanti_kapat()

            self.is_connected = False
            logger.info("EXTRACTOR --- DISCONNECT --- Bağlantılar kapatıldı")

        except Exception as e:
            logger.error(f"EXTRACTOR --- DISCONNECT ERROR --- {e}", exc_info=True)

    async def extract_and_save(self, article_id: int) -> bool:
        """
        Ana pipeline metodu

        Şu adımları takip eder:
        1. Database'den article çek
        2. Grok ile feature extraction yap
        3. Features'ı database'e kaydet

        Args:
            article_id: İşlenecek haberin ID'si

        Returns:
            bool: Başarılıysa True, hata varsa False

        Note:
            Bu metod RabbitMQ consumer tarafından çağrılır
            True dönerse → ACK gönderilir
            False dönerse → NACK gönderilir (retry)
        """
        start_time = time.time()

        try:
            logger.info(f"EXTRACTOR --- START --- Article #{article_id} işleniyor...")

            # Bağlantı kontrolü
            if not self.is_connected:
                logger.error("EXTRACTOR --- ERROR --- Bağlantılar aktif değil!")
                return False

            # 1. Database'den article çek
            article = await self._get_article(article_id)

            if article is None:
                logger.error(f"EXTRACTOR --- ERROR --- Article #{article_id} bulunamadı")
                return False

            # 2. Grok ile feature extraction
            features = await self._extract_features(article)

            # 3. Database'e kaydet
            await self._save_features(article_id, features, start_time)

            # Başarılı
            processing_time = time.time() - start_time
            logger.info(
                f"EXTRACTOR --- SUCCESS --- Article #{article_id} tamamlandı "
                f"({processing_time:.2f}s)"
            )

            return True

        except GrokAuthError as e:
            # API key hatası - retry yapmaya gerek yok
            logger.error(f"EXTRACTOR --- AUTH ERROR --- Article #{article_id}: {e}")
            return False

        except GrokParseError as e:
            # JSON parse hatası - Grok bozuk döndü
            logger.error(f"EXTRACTOR --- PARSE ERROR --- Article #{article_id}: {e}")
            return False

        except (GrokConnectionError, GrokAPIError) as e:
            # Geçici hatalar - retry yapılabilir
            logger.warning(f"EXTRACTOR --- RETRY ERROR --- Article #{article_id}: {e}")
            return False

        except Exception as e:
            # Beklenmeyen hata
            logger.error(
                f"EXTRACTOR --- UNEXPECTED ERROR --- Article #{article_id}: {e}",
                exc_info=True
            )
            return False

    async def _get_article(self, article_id: int) -> Optional[Dict[str, Any]]:
        """
        Database'den article çeker

        Args:
            article_id: Article ID

        Returns:
            dict: Article data (id, title, content, source, published_date)
            None: Article bulunamazsa
        """
        try:
            # Database Manager'ın metodunu kullan
            article = await self.db_manager.haber_id_ile_getir(article_id)

            if article:
                logger.debug(
                    f"EXTRACTOR --- DB --- Article #{article_id} çekildi "
                    f"(source: {article['source']})"
                )
            else:
                logger.warning(f"EXTRACTOR --- DB --- Article #{article_id} bulunamadı")

            return article

        except Exception as e:
            logger.error(f"EXTRACTOR --- DB ERROR --- Article çekme hatası: {e}", exc_info=True)
            raise

    async def _extract_features(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Grok API ile feature extraction yapar

        Args:
            article: Database'den çekilen article dict

        Returns:
            dict: Extracted features
        """
        try:
            # Article'dan title ve content çıkar
            title = article.get("title", "")
            content = article.get("content", "")

            if not content:
                raise ValueError("Article content boş!")

            # Grok'a gönder
            features = await self.grok_client.extract_features(
                news_text=content,
                title=title
            )

            return features

        except Exception as e:
            logger.error(f"EXTRACTOR --- GROK ERROR --- Feature extraction hatası: {e}")
            raise

    async def _save_features(self, article_id: int, features: Dict[str, Any], processing_time_ms: int):
        """
        Extracted features'ı database'e kaydeder
        """
        try:
            async with self.db_manager.pool.acquire() as conn:
                query = """
                    INSERT INTO extracted_features (
                        article_id,
                        unlu_isimler,
                        firmalar,
                        ulkeler,
                        kurumlar,
                        is_makroekonomi,
                        is_mikroekonomi,
                        alt_kategori,
                        guven_skoru_makro,
                        olay_tipi,
                        etki_yonu,
                        etkilenen_varlik_sinifi,
                        piyasa_etki_tahmini,
                        etki_degeri_confidence,
                        duygu,
                        duygu_skoru,
                        ton,
                        aciliyet,
                        surpriz_mi,
                        surpriz_confidence,
                        etki_suresi,
                        gelecege_yonelik_mi,
                        gelecek_confidence,
                        sayisal_degerler,
                        beklenti_karsilama,
                        extraction_date,
                        model_version,
                        processing_time_ms
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                        $21, $22, $23, $24, $25, NOW(), $26, $27
                    )
                """

                await conn.execute(
                    query,
                    article_id,  # $1
                    features["unlu_isimler"],  # $2 - Python list direkt
                    features["firmalar"],  # $3 - Python list direkt
                    features["ulkeler"],  # $4 - Python list direkt
                    json.dumps(features["kurumlar"]),  # $5 - JSONB için json.dumps
                    features["is_makroekonomi"],  # $6
                    features["is_mikroekonomi"],  # $7
                    features["alt_kategori"],  # $8
                    features["guven_skoru_makro"],  # $9
                    features["olay_tipi"],  # $10
                    features["etki_yonu"],  # $11
                    features["etkilenen_varlik_sinifi"],  # $12 - Python list direkt
                    json.dumps(features["piyasa_etki_tahmini"]),  # $13 - JSONB için json.dumps
                    features["etki_degeri_confidence"],  # $14
                    features["duygu"],  # $15
                    features["duygu_skoru"],  # $16
                    features["ton"],  # $17
                    features["aciliyet"],  # $18
                    features["surpriz_mi"],  # $19
                    features["surpriz_confidence"],  # $20
                    features["etki_suresi"],  # $21
                    features["gelecege_yonelik_mi"],  # $22
                    features["gelecek_confidence"],  # $23
                    json.dumps(features["sayisal_degerler"]),  # $24 - JSONB için json.dumps
                    features["beklenti_karsilama"],  # $25
                    "grok-beta",  # $26 model_version
                    processing_time_ms  # $27
                )

                logger.info(
                    f"EXTRACTOR --- DB --- Features kaydedildi "
                    f"(article_id: {article_id}, processing: {processing_time_ms}ms)"
                )

        except Exception as e:
            logger.error(f"EXTRACTOR --- DB ERROR --- Feature kaydetme hatası: {e}", exc_info=True)
            raise















