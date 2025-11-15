# shared/utils/rabbitmq_utils.py
"""
RabbitMQ işlemleri için Manager Class
CloudAMQP uyumlu
"""
"""
Yazilimciya not;
Eğer mikroservisi incelemeye buradan başladıysanız, bazı fonksiyon adlarının İngilizce olduğunu fark edebilirsiniz. 
Tutarlılığı korumak amacıyla, gerekli noktalarda Türkçe açıklamalar yorum satırlarıyla eklenmiştir. 
Projenin genel senkronizasyonunu ve okunabilirliğini artırmak adına bu yaklaşım tercih edilmiştir.
"""
import aio_pika
import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional, Callable
from datetime import datetime

from ..config import config
from .rabbitmq_exceptions import (
    ConnectionError,
    PublishError,
    QueueDeclarationError,
    ConsumerError
)

logger = logging.getLogger(__name__)


class RabbitMQManager:
    """
    RabbitMQ bağlantı ve mesaj yönetimi

    Özellikler:
    - Otomatik reconnection
    - Message persistence
    - Dead Letter Queue (DLQ) desteği
    - Retry mekanizması -exponential backoff-
    - Rate limiting -CloudAMQP free tier: --4 req/min--
    - Metriks toplama (ileride yapilacak olan RL optimizasyonlari**)
    """
    def __init__(self):

        """
        RabbitMQ Manager/Yoneticisi'ni baslatir
        """

        # CloudAMQPN URL'sini alalim
        self.rabbitmq_url = config.rabbitmq_url

        #Queue isimleri
        self.feature_queue = config.rabbitmq_feature_queue
        self.dlq_queue = config.rabbitmq_dlq_queue

        # Tekrar deneme/ Retry ayarlari
        # En fazla kac kere denenebilir int degerinde
        self.max_retries = config.rabbitmq_max_retries
        # Deneme arasi verilen sureyi tanimliyoruz
        self.retry_delay = config.rabbitmq_retry_delay

        # Su anlik, ucretsiz surumu kullandigimiz icin rate limiting
        self.rate_limit = config.rabbitmq_rate_limit

        # Bağlantı objeleri
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None

        # Queue objeleri
        self.feature_queue_obj: Optional[aio_pika.Queue] = None
        self.dlq_queue_obj: Optional[aio_pika.Queue] = None

        # Metrikler
        self.metrics = {
            "messages_published": 0,
            "messages_consumed": 0,
            "publish_errors": 0,
            "consume_errors": 0,
            "reconnections": 0,
            "rate_limit_hits": 0
        }

        # Log olustur
        logger.info("RABBITMQ --- INIT --- Manager oluşturuldu (CloudAMQP)")

# Burdan itibaren CloudAMQP ile konsumaya baslayalim

    # async def baglan_()
    async def connect(self):
        """
        CloudAMQP sunucusuna bağlantı kurar ve queue yapısını oluşturur.

        Bu metod şu adımları takip eder:
        1. URL'den güvenli bilgi çıkarımı (credential'lar loglanmaz)
        2. Robust connection kurulumu (bağlantı koptuğunda otomatik yeniden bağlanır)
        3. Event handler'ların kaydedilmesi (reconnect, close olayları için)
        4. Channel açılması (mesaj gönderme/alma için iletişim kanalı)
        5. QoS ayarlarının yapılması (worker başına mesaj limiti)
        6. Queue topolojisinin oluşturulması (DLQ + Ana Queue)

        Raises:
            ConnectionError: Bağlantı kurulamazsa veya queue setup başarısız olursa

        Note:
            - RobustConnection kullanımı: Network kesintilerinde otomatik recovery sağlar
            - Heartbeat mekanizması: Her 60 saniyede bağlantı sağlığını kontrol eder
            - Reconnect interval: Bağlantı koptuğunda 5 saniyede bir tekrar dener
        """
        try:
            # URL'i parse et - Loglama sırasında şifrenin görünmemesi için
            # Sadece host bilgisini loglayacağız
            url_info = config.parse_rabbitmq_url()
            logger.info(
                f"RABBITMQ --- BAGLANTI --- "
                f"{url_info['host']} adresine bağlanılıyor..."
            )

            # CloudAMQP'ya robust connection kur
            # Robust = Bağlantı koptuğunda otomatik yeniden bağlanır
            # Normal connection'dan farkı: Fail-safe mekanizma built-in
            self.connection = await aio_pika.connect_robust(
                self.rabbitmq_url,  # Tam URL: amqps://user:pass@host/vhost
                timeout=config.rabbitmq_connection_timeout,  # 30s - Bağlantı timeout süresi
                heartbeat=config.rabbitmq_heartbeat,  # 60s - Bağlantı canlılık kontrolü
                reconnect_interval=5,  # Kopma durumunda 5 saniyede bir tekrar dene
                fail_fast=False  # Başlangıçta bağlanamazsa bekle, crash etme
            )

            # Bağlantı event handler'larını kaydet
            # Reconnect callback: Bağlantı yeniden kurulduğunda çağrılır
            self.connection.reconnect_callbacks.add(self._on_reconnect)
            # Close callback: Bağlantı kapandığında çağrılır
            self.connection.close_callbacks.add(self._on_connection_close)

            logger.info("RABBITMQ --- BAGLANTI --- Bağlantı başarıyla kuruldu")

            # Mesajlaşma için channel aç
            # Channel: Bir connection üzerinde birden fazla açılabilir
            # Her channel bağımsız olarak mesaj gönderip alabilir
            self.channel = await self.connection.channel()

            # QoS (Quality of Service) ayarlarını yap
            # prefetch_count: Bir consumer'a aynı anda kaç mesaj gönderilebileceğini belirler
            # prefetch_count=1: Worker mesajı işleyene kadar başka mesaj gönderme
            # Bu sayede yük dengeli dağıtılır (hızlı worker'lar daha çok iş alır)
            await self.channel.set_qos(prefetch_count=config.rabbitmq_prefetch_count)

            logger.info(
                f"RABBITMQ --- KANAL --- Channel açıldı "
                f"(prefetch_count={config.rabbitmq_prefetch_count})"
            )

            # Queue yapısını oluştur
            # DLQ (Dead Letter Queue) + Ana Queue
            await self._setup_queues()

            logger.info("RABBITMQ --- BASARILI --- Tüm hazırlıklar tamamlandı")
            return True

        except aio_pika.exceptions.AMQPError as e:
            # AMQP protokol seviyesi hataları (authentication, permission vb.)
            logger.error(
                f"RABBITMQ --- HATA --- AMQP hatası: {e}",
                exc_info=True  # Stack trace'i de logla
            )
            raise ConnectionError(f"CloudAMQP bağlantı hatası: {e}") from e

        except Exception as e:
            # Beklenmeyen hatalar (network, timeout vb.)
            logger.error(
                f"RABBITMQ --- HATA --- Beklenmeyen hata: {e}",
                exc_info=True
            )
            raise ConnectionError(f"Bağlantı kurulamadı: {e}") from e

    def _on_reconnect(self, connection: aio_pika.RobustConnection):
        """
        Bağlantı yeniden kurulduğunda otomatik olarak çağrılır.

        Bu callback network kesintileri sonrası bağlantı recovery durumunda
        tetiklenir. Metriği günceller ve loglar.

        Args:
            connection: Yeniden kurulan RabbitMQ bağlantı objesi
        """
        # Reconnection metriğini artır (monitoring için önemli)
        self.metrics["reconnections"] += 1
        logger.warning(
            f"RABBITMQ --- RECONNECT --- Bağlantı yeniden kuruldu "
            f"(Toplam reconnection: {self.metrics['reconnections']})"
        )

    def _on_connection_close(self, connection: aio_pika.RobustConnection, exc: Exception):
        """
        Bağlantı kapandığında otomatik olarak çağrılır.

        İki senaryo vardır:
        1. Graceful shutdown (exc=None): Normal kapanma
        2. Unexpected close (exc!=None): Hata ile kapanma

        Args:
            connection: Kapanan bağlantı objesi
            exc: Hata varsa exception objesi, yoksa None
        """
        if exc:
            # Beklenmeyen kapanma - Network hatası, server crash vb.
            logger.error(f"RABBITMQ --- DISCONNECT --- Bağlantı beklenmedik şekilde kesildi: {exc}")
        else:
            # Normal kapanma - Uygulama kapatılıyor
            logger.info("RABBITMQ --- DISCONNECT --- Bağlantı düzgün bir şekilde kapatıldı")

    async def _setup_queues(self):

        """
        Queue yapısını oluşturur

        Dead Letter Exchange (DLX) Pattern'i kullaniyoruz;
        1. DLQ oluştur (başarısız mesajların gideceği yer)
        2. Ana queue oluştur ve DLQ'ya bağla
        3. Mesaj fail olursa → otomatik DLQ'ya gider

        Queue Özellikleri:
        - durable=True: Server restart'ta queue kalır
        - auto_delete=False: Consumer kapanınca queue silinmez
        - exclusive=False: Birden fazla consumer bağlanabilir

        Raises:
            QueueDeclarationError: Queue oluşturulamazsa

        """
        try:
            logger.info("RABBITMQ --- QUEUE SETUP --- Queue yapısı oluşturuluyor...")

            # Ilk Asamada : Dead Letter Queue (DLQ) olusuturuluyor
            # Basarisiz olan mesajlar buraya gidiyor

            self.dlq_queue_obj = await self.channel.declare_queue(
                name = self.dlq_queue,
                durable = True,
                auto_delete = False,
                exclusive = False
            )

            logger.info(
            f"RABBITMQ --- QUEUE --- DLQ oluşturuldu: '{self.dlq_queue}' ")

            # Ikinci adimda Ana Queue olusturuyoruz,
            #   Ana queue - feature extraction mesajları buraya gelir
            #       x-dead-letter-exchange: Mesaj reject edilince nereye gidecek?
            #           x-dead-letter-routing-key: DLQ'ya routing için key

            self.feature_queue_obj = await self.channel.declare_queue(
            name=self.feature_queue,  # Örn: "feature_extraction"
            durable=True,
            auto_delete=False,
            exclusive=False,
            arguments={
                # Mesaj fail olursa default exchange'e gönder
                "x-dead-letter-exchange": "",
                # DLQ routing key
                "x-dead-letter-routing-key": self.dlq_queue
            }
            )

            logger.info(
            f"RABBITMQ --- QUEUE --- Ana queue oluşturuldu: '{self.feature_queue}'")
            logger.info(
            f"RABBITMQ --- QUEUE --- DLX bağlantısı: {self.feature_queue} → {self.dlq_queue}")
            # Queue bilgilerini logla
            logger.info(
                                            f"RABBITMQ --- QUEUE INFO ---\n"
                                            f"  Ana Queue: {self.feature_queue}\n"
                                            f"    - Durable: True\n"
                                            f"    - Auto-delete: False\n"
                                            f"    - DLX Target: {self.dlq_queue}\n"
                                            f"  Dead Letter Queue: {self.dlq_queue}\n"
                                            f"    - Durable: True\n"
                                            f"    - Auto-delete: False"
            )

            return True

        except aio_pika.exceptions.AMQPError as e:
            # AMQP seviyesi hata (permission, invalid argument vb.)
            logger.error(
            f"RABBITMQ --- QUEUE HATA --- AMQP hatası: {e}",
            exc_info=True
            )
            raise QueueDeclarationError(f"Queue oluşturma hatası: {e}") from e

        except Exception as e:
            # Beklenmeyen hata
            logger.error(
                f"RABBITMQ --- QUEUE HATA --- Beklenmeyen hata: {e}",exc_info=True )
            raise QueueDeclarationError(f"Queue setup başarısız: {e}") from e


    # Publish metodu

    # ┌─────────────────┐
    # │     Ilk   │
    # │  Mikroservisi   │
    # └────────┬────────┘
    #          │ 1. Yeni haber bulur
    #          ▼
    # ┌─────────────────┐
    # │   PostgreSQL    │
    # │   articles      │ approved (daha sonra status olarak degistirelecek)
    # └────────┬────────┘
    #          │
    #          │ 2. Queue Manager çalışır
    #          ▼
    # ┌─────────────────┐
    # │ publish() metodu│
    # │ RabbitMQ'ya at │
    # └────────┬────────┘

    async def publish(self, article_id:int, retry_count:int = 0) -> bool:
        """
        Article id'sini okur, buna gore feature extraction queue'suna gonderir

        Ornek bir mesaj formati;

        {
                    "article_id": 123,
                    "retry_count": 0,
                    "timestamp": "2025-11-15T12:34:56.789Z",
                    "enqueued_at": "2025-11-15T12:34:56.789Z"
        }

        Args:
            article_id: Islenecek haberin ID'si
            retry_count: Kacinci deneme oldugu

        Returns:
            bool: Basarili olursa True, otw False

        Raises:
            PublishError: Mesaj gonderilmez ise
        """
        try:
            # Bağlantı kontrolü
            if not await self.is_connected():
                logger.error("RABBITMQ --- PUBLISH --- Bağlantı yok, önce connect() çağırın!")
                raise PublishError("RabbitMQ bağlantısı yok")

            # Rate limiting kontrolü
            await self._apply_rate_limit()

            # Mesaj payload'ını hazırla
            # Mesaj formati ayni yukarida yazildigi gibi
            message_data = {
                "article_id": article_id,
                "retry_count": retry_count,
                "timestamp": datetime.now().isoformat(),
                "enqueued_at": datetime.now().isoformat()
            }

            # JSON'a çevir
            message_body = json.dumps(message_data)

            # Mesajı oluştur
            message = aio_pika.Message(
                body=message_body.encode('utf-8'),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # Mesaj persistent olsun
                content_type='application/json',
                headers={
                    'x-retry-count': retry_count,
                    'x-article-id': article_id
                }
            )

            # Queue'ya gönder
            await self.channel.default_exchange.publish(
                message,
                routing_key=self.feature_queue  # Queue adı routing key olarak kullanılır
            )

            # Metrikleri güncelle
            self.metrics["messages_published"] += 1

            logger.info(
                f"RABBITMQ --- PUBLISH --- Article #{article_id} queue'ya eklendi "
                f"(retry: {retry_count}, toplam: {self.metrics['messages_published']})"
            )

            return True

        except aio_pika.exceptions.AMQPError as e:
            self.metrics["publish_errors"] += 1
            logger.error(
                f"RABBITMQ --- PUBLISH ERROR --- AMQP hatası (article #{article_id}): {e}",
                exc_info=True
            )
            raise PublishError(f"Mesaj gönderilemedi: {e}") from e

        except Exception as e:
            self.metrics["publish_errors"] += 1
            logger.error(
                f"RABBITMQ --- PUBLISH ERROR --- Beklenmeyen hata (article #{article_id}): {e}",
                exc_info=True
            )
            raise PublishError(f"Publish başarısız: {e}") from e

    async def _apply_rate_limit(self):
        """
        Su an deneme hesabinda rate-limiting kullanildigi icin
        """
        delay_time = 60
        # Rate limit yoksa skip et
        if self.rate_limit == 0:
            return
        delay = delay_time / self.rate_limit
        logger.debug(f"RABBITMQ --- RATE LIMIT --- {delay:.1f}s bekleniyor...")
        await asyncio.sleep(delay)
        # Metrik
        self.metrics["rate_limit_hits"] += 1

    async def consume(self, callback: Callable):
        """
        Queue'dan mesaj dinler ve gelen her mesaj için callback'i çağırır.

        Callback fonksiyonu şu signature'a sahip olmalı:
        async def process_message(article_id: int, retry_count: int) -> bool:
            # True = başarılı (ACK), False = başarısız (NACK)
            pass

        Args:
            callback: Mesaj işleme fonksiyonu

        Raises:
            ConsumerError: Consumer başlatılamazsa
        """

        try:
            # Bağlantı kontrolü
            if not await self.is_connected():
                logger.error("RABBITMQ --- CONSUME --- Bağlantı yok!")
                raise ConsumerError("RabbitMQ bağlantısı yok")

            logger.info(
                f"RABBITMQ --- CONSUME --- '{self.feature_queue}' queue'su dinleniyor..."
            )

            # Consumer başlat
            async with self.feature_queue_obj.iterator() as queue_iter:
                async for message in queue_iter:
                    # Mesajı handle et (ACK/NACK içeride yapılıyor)
                    await self._handle_message(message, callback)

        except aio_pika.exceptions.AMQPError as e:
            self.metrics["consume_errors"] += 1
            logger.error(
                f"RABBITMQ --- CONSUME ERROR --- AMQP hatası: {e}",
                exc_info=True
            )
            raise ConsumerError(f"Consumer başlatılamadı: {e}") from e

        except Exception as e:
            self.metrics["consume_errors"] += 1
            logger.error(
                f"RABBITMQ --- CONSUME ERROR --- Beklenmeyen hata: {e}",
                exc_info=True
            )
            raise ConsumerError(f"Consume başarısız: {e}") from e


    async def _handle_message(
            self,
            message: aio_pika.IncomingMessage,
            callback: Callable):
            """
            Gelen mesajı işler ve callback'i çağırır.

            Nasil calisir:
            Mesajı ilk olarak pars eder
            Callback'i çagirir
            Başarılıysa ACK gonderir
            Basarısızsa:
                - Retry limit aşılmadıysa → NACK + requeue
                - Retry limit aşıldıysa → NACK (DLQ'ya gider)

            Args:
            message: RabbitMQ mesaj objesi
            callback: İşleme fonksiyonu
            """
            try:
                # Mesajı parse et
                message_body = message.body.decode('utf-8')
                message_data = json.loads(message_body)

                article_id = message_data.get("article_id")
                retry_count = message_data.get("retry_count", 0)

                logger.info(
                    f"RABBITMQ --- MESSAGE --- Article #{article_id} işleniyor "
                    f"(retry: {retry_count}/{self.max_retries})..."
                )

                # Callbacki cagir
                # Zamani baslat
                start_time = time.time()
                success = await callback(article_id, retry_count)
                # Sonlanma zamanini da tutalim
                processing_time = time.time() - start_time

                if success:
                    # Eger sistem basariliysa ACK gonder
                    await message.ack()
                    self.metrics["messages_consumed"] +=1

                    logger.info(
                        f"RABBITMQ --- SUCCESS --- Article #{article_id} başarıyla işlendi "
                        f"({processing_time:.2f}s) "
                    )

                else:
                # Başarısız - Retry kontrolü
                    if retry_count < self.max_retries:
                        # Retry limit aşılmadı mi?
                        # tekrar queue'ya at
                        # DLQ'ya gitmesin
                        await message.nack(requeue=False)

                        # Exponential backoff ile tekrar queue'ya ekle
                        await asyncio.sleep(self.retry_delay * (2 ** retry_count))
                        await self.publish(article_id, retry_count + 1)

                        # Logger'a uyari girelim
                        logger.warning(
                            f"RABBITMQ --- RETRY --- Article #{article_id} "
                            f"tekrar denenecek ({retry_count + 1}/{self.max_retries})"
                        )
                        # Eger retry limitimiz de asildi ise
                    else:
                        # Artik DLQ'ya gönder
                        await message.nack(requeue=False)
                        logger.error(
                            f"RABBITMQ --- DLQ --- Article #{article_id} "
                            f"DLQ'ya gönderildi (max retry aşıldı)"
                        )

            except json.JSONDecodeError as e:
                # JSON parse hatası - mesaj bozuk
                logger.error(f"RABBITMQ --- PARSE ERROR --- JSON parse hatası: {e}",exc_info=True)
                await message.nack(requeue=False)

            except Exception as e:
                # Beklenmeyen hata
                logger.error(
                    f"RABBITMQ --- HANDLE ERROR --- Mesaj işleme hatası: {e}",
                    exc_info=True
                )
                try:
                    await message.nack(requeue=False)
                except Exception:
                    # Zaten process edilmisse ignore et
                    pass
    def get_metrics(self) -> Dict[str,Any]:
        """
        Anlik metrikleri dondurur

        Returns:
            dict: Metrik bilgileri
        """
        return {
            **self.metrics,
            "connected": asyncio.run(self.is_connected()) if not asyncio.get_event_loop().is_running() else False,
            "timestamp": datetime.now().isoformat()
        }

    async def disconnect(self):
        """
        RabbitMQ bağlantısını güvenli şekilde kapatır.

        Ilk olarak channel'ı kapat
        Daha sonra connection'ı kapat
        En sonunda kaynakları temizle

        Bu metod exception fırlatmaz, sessizce kapanır
        """
        try:
            logger.info("RABBITMQ ---DISCONNECT ---baglanti kapatiliyor")
            ###
            # https: // www.youtube.com / watch?v = ARJ8cAGm6JE
            ###

            # Ilk olarak channel i kapatalim
            if self.channel is not None:
                try:
                    await self.channel.close()
                    logger.info("RABBITMQ --- DISCONNECT --- Channel kapatıldı")
                except Exception as e:
                    logger.warning(f"RABBITMQ --- DISCONNECT --- Channel kapatma hatası: {e}")

            if self.connection is not None:
                try:
                    await self.connection.close()
                    logger.info("RABBITMQ --- DISCONNECT --- Connection kapatıldı")
                except Exception as e:
                    logger.warning(f"RABBITMQ --- DISCONNECT --- Connection kapatma hatası: {e}")

            # Referansları temizle
            self.channel = None
            self.connection = None
            self.feature_queue_obj = None
            self.dlq_queue_obj = None

            logger.info("RABBITMQ --- DISCONNECT --- Bağlantı başarıyla kapatıldı")

        except Exception as e:
            # Hata olsa bile logla ve devam et
            logger.error(
            f"RABBITMQ --- DISCONNECT --- Kapatma sırasında beklenmeyen hata: {e}",
            exc_info=True
        )

    async def is_connected(self) -> bool:
        """
        Bağlantının aktif olup olmadığını kontrol eder.

        Returns:
            bool: Bağlantı aktifse True, değilse False
        """
        try:
            # Connection ve channel var mı kontrol et
            if self.connection is None or self.channel is None:
                return False

            # RobustConnection'ın is_closed property'si var
            # Channel'ın is_closed property'si var
            return not self.connection.is_closed and not self.channel.is_closed

        except Exception as e:
            logger.debug(f"RABBITMQ --- IS_CONNECTED --- Kontrol hatası: {e}")
            return False























