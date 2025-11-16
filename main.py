"""
Feature Extraction Microservice
- FastAPI endpoints (health check, metrics, manual operations)
- RabbitMQ Consumer (queue'dan mesaj dinler)
- Feature Extractor (Grok + Database)
"""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from typing import Dict, Any, List

# Absolute imports
from shared.database import VeriHavuzuYoneticisi
from shared.utils.rabbitmq_utils import RabbitMQManager
from services.feature_worker.extractor import FeatureExtractor
from shared.config import config

# Logging setup
# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
db_manager = VeriHavuzuYoneticisi()
rmq_manager = RabbitMQManager()
extractor = FeatureExtractor()

# Consumer task
consumer_task = None


# ============================================
# CONSUMER LOGIC
# ============================================

async def process_message(message: Dict[str, Any]) -> bool:
    """
    RabbitMQ'dan gelen mesajı işle

    Args:
        message: {"article_id": 123}

    Returns:
        bool: Başarılıysa True (ACK), değilse False (NACK/Retry)
    """
    try:
        article_id = message.get("article_id")

        if not article_id:
            logger.error("CONSUMER --- ERROR --- Mesajda article_id yok!")
            return False

        logger.info(f"CONSUMER --- PROCESSING --- Article #{article_id}")

        # Feature extraction yap
        success = await extractor.extract_and_save(article_id)

        if success:
            logger.info(f"CONSUMER --- SUCCESS --- Article #{article_id}")
            return True
        else:
            logger.warning(f"CONSUMER --- FAILED --- Article #{article_id}")
            return False

    except Exception as e:
        logger.error(f"CONSUMER --- ERROR --- {e}", exc_info=True)
        return False


async def process_message_wrapper(article_id: int, retry_count: int) -> bool:
    """
    RabbitMQ callback - RabbitMQManager.consume() için

    Args:
        article_id: İşlenecek article ID
        retry_count: Kaçıncı deneme

    Returns:
        bool: Başarılıysa True (ACK), değilse False (NACK)
    """
    try:
        logger.info(f"CONSUMER --- PROCESSING --- Article #{article_id} (retry: {retry_count})")

        # Feature extraction yap
        success = await extractor.extract_and_save(article_id)

        if success:
            logger.info(f"CONSUMER --- SUCCESS --- Article #{article_id}")
            return True
        else:
            logger.warning(f"CONSUMER --- FAILED --- Article #{article_id}")
            return False

    except Exception as e:
        logger.error(f"CONSUMER --- ERROR --- {e}", exc_info=True)
        return False


async def start_consumer():
    """
    RabbitMQ consumer'ı başlat
    Queue'dan sürekli mesaj dinler
    """
    try:
        logger.info("CONSUMER --- STARTING --- Queue dinleniyor...")

        # consume() metodunu çağır
        await rmq_manager.consume(callback=process_message_wrapper)

    except asyncio.CancelledError:
        logger.info("CONSUMER --- CANCELLED --- Consumer durduruldu")
    except Exception as e:
        logger.error(f"CONSUMER --- ERROR --- {e}", exc_info=True)


# ============================================
# FASTAPI LIFECYCLE
# ============================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI startup/shutdown lifecycle

    Startup:
    1. Database bağlantısı aç
    2. RabbitMQ bağlantısı aç
    3. Grok client başlat
    4. Consumer task'ı başlat

    Shutdown:
    1. Consumer task'ı durdur
    2. Tüm bağlantıları kapat
    """
    global consumer_task

    # ========== STARTUP ==========
    logger.info("SYSTEM --- STARTUP --- Sistem başlatılıyor...")

    try:
        # 1. Database bağlantısı
        logger.info("DATABASE --- Bağlanıyor...")
        await db_manager.baglanti_ac()
        logger.info("DATABASE --- Bağlandı")

        # 2. RabbitMQ bağlantısı
        logger.info("RABBITMQ --- Bağlanıyor...")
        await rmq_manager.connect()
        logger.info("RABBITMQ --- Bağlandı")

        # 3. Grok client başlat
        logger.info("GROK --- Bağlanıyor...")
        await extractor.connect()
        logger.info("GROK --- Bağlandı")

        # 4. Consumer task başlat (arka planda çalışır)
        logger.info("CONSUMER --- Başlatılıyor...")
        consumer_task = asyncio.create_task(start_consumer())
        logger.info("CONSUMER --- Başlatıldı")

        logger.info("SYSTEM --- READY --- Sistem hazır!")

    except Exception as e:
        logger.error(f"SYSTEM --- STARTUP ERROR --- {e}", exc_info=True)
        raise

    # ========== YIELD (Uygulama çalışıyor) ==========
    yield

    # ========== SHUTDOWN ==========
    logger.info("SYSTEM --- SHUTDOWN --- Sistem kapatılıyor...")

    try:
        # 1. Consumer'ı durdur
        if consumer_task:
            logger.info("CONSUMER --- Durduruluyor...")
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
            logger.info("CONSUMER --- Durduruldu")

        # 2. Grok client kapat
        logger.info("GROK --- Kapatılıyor...")
        await extractor.disconnect()
        logger.info("GROK --- Kapatıldı")

        # 3. RabbitMQ kapat
        logger.info("RABBITMQ --- Kapatılıyor...")
        await rmq_manager.baglanti_kapat()
        logger.info("RABBITMQ --- Kapatıldı")

        # 4. Database kapat
        logger.info("DATABASE --- Kapatılıyor...")
        await db_manager.disconnect()
        logger.info("DATABASE --- Kapatıldı")

        logger.info("SYSTEM --- SHUTDOWN COMPLETE")

    except Exception as e:
        logger.error(f"SYSTEM --- SHUTDOWN ERROR --- {e}", exc_info=True)


# ============================================
# FASTAPI APP
# ============================================

app = FastAPI(
    title="Feature Extraction Microservice",
    description="Grok-powered semantic feature extraction for Turkish financial news",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================
# API ENDPOINTS
# ============================================

@app.get("/")
async def root():
    """
    Ana endpoint - Sistem bilgisi ve mevcut endpoint'lerin listesi
    """
    return {
        "service": "Feature Extraction Microservice",
        "status": "running",
        "version": "1.0.0",
        "description": "Turkish financial news semantic feature extraction using Grok AI",
        "endpoints": {
            "system": {
                "root": "GET /",
                "health": "GET /health",
                "metrics": "GET /metrics",
                "grok_metrics": "GET /metrics/grok"
            },
            "articles": {
                "get_pending": "GET /api/articles/pending",
                "get_by_id": "GET /api/articles/{article_id}",
                "update_status": "PUT /api/articles/{article_id}/status"
            },
            "features": {
                "get_all": "GET /api/features",
                "get_by_article": "GET /api/features/article/{article_id}",
                "get_by_category": "GET /api/features/category/{category_id}"
            },
            "extraction": {
                "manual_extract": "POST /api/extract/{article_id}",
                "queue_article": "POST /api/queue/{article_id}"
            }
        }
    }


@app.get("/health")
async def health_check():
    """
    Sistem sağlık kontrolü
    Database, RabbitMQ, Grok client ve Consumer durumlarını kontrol eder
    """
    health_status = {
        "status": "healthy",
        "components": {}
    }

    # Database kontrolü
    db_healthy = db_manager.pool is not None
    health_status["components"]["database"] = {
        "status": "healthy" if db_healthy else "unhealthy",
        "connected": db_healthy
    }

    # RabbitMQ kontrolü
    rmq_healthy = rmq_manager.connection is not None and not rmq_manager.connection.is_closed
    health_status["components"]["rabbitmq"] = {
        "status": "healthy" if rmq_healthy else "unhealthy",
        "connected": rmq_healthy
    }

    # Grok client kontrolü
    grok_healthy = extractor.is_connected
    health_status["components"]["grok"] = {
        "status": "healthy" if grok_healthy else "unhealthy",
        "connected": grok_healthy
    }

    # Consumer kontrolü
    consumer_running = consumer_task is not None and not consumer_task.done()
    health_status["components"]["consumer"] = {
        "status": "running" if consumer_running else "stopped",
        "active": consumer_running
    }

    # Genel durum
    all_healthy = db_healthy and rmq_healthy and grok_healthy and consumer_running
    health_status["status"] = "healthy" if all_healthy else "degraded"

    return health_status


@app.get("/metrics")
async def get_metrics():
    """
    Sistem metrikleri
    RabbitMQ ve Grok metriklerini döner
    """
    return {
        "rabbitmq": rmq_manager.get_metrics(),
        "grok": extractor.grok_client.get_metrics()
    }


@app.get("/metrics/grok")
async def get_grok_metrics():
    """
    Sadece Grok API metriklerini döner
    Token kullanımı, request sayısı, başarı oranı vb.
    """
    return extractor.grok_client.get_metrics()


# ============================================
# ARTICLE ENDPOINTS
# ============================================

@app.get("/api/articles/pending")
async def get_pending_articles(limit: int = Query(default=100, ge=1, le=1000)):
    """
    Feature extraction bekleyen haberleri getirir
    approved = 'semantik_analiz_icin_sirada' olan haberler

    Args:
        limit: Maksimum kaç haber (1-1000 arası)
    """
    try:
        haberler = await db_manager.haberleri_getir(limit=limit)
        return {
            "success": True,
            "count": len(haberler),
            "data": haberler
        }
    except Exception as e:
        logger.error(f"API --- ERROR --- Haberler getirilemedi: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/articles/{article_id}")
async def get_article(article_id: int):
    """
    Belirli bir haberi ID ile getirir

    Args:
        article_id: Haber ID
    """
    try:
        article = await db_manager.haber_id_ile_getir(article_id)

        if article:
            return {
                "success": True,
                "data": article
            }
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Article ID {article_id} bulunamadı"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API --- ERROR --- Article getirilemedi: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/articles/{article_id}/status")
async def update_article_status(article_id: int, yeni_durum: str = Query(...)):
    """
    Haberin durumunu günceller

    Args:
        article_id: Haber ID
        yeni_durum: Yeni durum ('processing', 'semantik_analizden_gecti', 'failed')
    """
    try:
        basarili = await db_manager.haber_durumunu_guncelle(
            haber_id=article_id,
            yeni_durum=yeni_durum
        )

        if basarili:
            return {
                "success": True,
                "message": f"Haber {article_id} durumu güncellendi: {yeni_durum}"
            }
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Haber ID {article_id} bulunamadı"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API --- ERROR --- Durum güncellenemedi: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# FEATURE ENDPOINTS
# ============================================

@app.get("/api/features")
async def get_all_features(
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0)
):
    """
    Tüm extracted features'ları getirir (pagination ile)

    Args:
        limit: Kaç kayıt (1-1000)
        offset: Başlangıç noktası (pagination için)
    """
    try:
        async with db_manager.pool.acquire() as conn:
            query = """
                SELECT * FROM extracted_features
                ORDER BY extraction_date DESC
                LIMIT $1 OFFSET $2
            """
            rows = await conn.fetch(query, limit, offset)
            features = [dict(row) for row in rows]

            return {
                "success": True,
                "count": len(features),
                "limit": limit,
                "offset": offset,
                "data": features
            }
    except Exception as e:
        logger.error(f"API --- ERROR --- Features getirilemedi: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/features/article/{article_id}")
async def get_features_by_article(article_id: int):
    """
    Belirli bir article'ın feature'larını getirir

    Args:
        article_id: Article ID
    """
    try:
        async with db_manager.pool.acquire() as conn:
            query = """
                SELECT * FROM extracted_features
                WHERE article_id = $1
                ORDER BY extraction_date DESC
            """
            rows = await conn.fetch(query, article_id)
            features = [dict(row) for row in rows]

            if features:
                return {
                    "success": True,
                    "article_id": article_id,
                    "count": len(features),
                    "data": features
                }
            else:
                raise HTTPException(
                    status_code=404,
                    detail=f"Article ID {article_id} için feature bulunamadı"
                )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API --- ERROR --- Features getirilemedi: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/features/category/{category_id}")
async def get_features_by_category(
        category_id: int,
        limit: int = Query(default=100, ge=1, le=1000)
):
    """
    Belirli bir kategori ID'sine göre feature'ları getirir

    Args:
        category_id: Alt kategori ID (0-13 arası)
        limit: Maksimum kaç kayıt
    """
    try:
        if not (0 <= category_id <= 13):
            raise HTTPException(
                status_code=400,
                detail="category_id 0-13 arası olmalı"
            )

        async with db_manager.pool.acquire() as conn:
            query = """
                SELECT * FROM extracted_features
                WHERE alt_kategori = $1
                ORDER BY extraction_date DESC
                LIMIT $2
            """
            rows = await conn.fetch(query, category_id, limit)
            features = [dict(row) for row in rows]

            return {
                "success": True,
                "category_id": category_id,
                "count": len(features),
                "data": features
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API --- ERROR --- Features getirilemedi: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# EXTRACTION ENDPOINTS
# ============================================

@app.post("/api/extract/{article_id}")
async def manual_extract(article_id: int):
    """
    Manuel feature extraction
    Belirtilen article_id için direkt Grok'a gönder ve database'e kaydet
    Queue kullanmaz, senkron işlem

    Args:
        article_id: İşlenecek haber ID'si
    """
    try:
        logger.info(f"API --- MANUAL EXTRACT --- Article #{article_id}")

        success = await extractor.extract_and_save(article_id)

        if success:
            return {
                "success": True,
                "message": f"Article #{article_id} işlendi ve kaydedildi",
                "article_id": article_id
            }
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Article #{article_id} işlenemedi"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API --- EXTRACT ERROR --- {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/queue/{article_id}")
async def queue_article(article_id: int):
    """Article'ı RabbitMQ queue'ya ekler"""
    try:
        logger.info(f"API --- QUEUE --- Article #{article_id} queue'ya ekleniyor")

        await rmq_manager.publish(article_id=article_id, retry_count=0)

        return {
            "success": True,
            "message": f"Article #{article_id} queue'ya eklendi",
            "article_id": article_id,
            "queue": "feature_extraction"
        }

    except Exception as e:
        logger.error(f"API --- QUEUE ERROR --- {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# BATCH OPERATIONS
# ============================================

@app.post("/api/queue/batch")
async def queue_batch_articles(article_ids: List[int]):
    """Toplu article'ları queue'ya ekler"""
    try:
        logger.info(f"API --- BATCH QUEUE --- {len(article_ids)} article queue'ya ekleniyor")

        success_count = 0
        failed_ids = []

        for article_id in article_ids:
            try:
                await rmq_manager.publish(article_id=article_id, retry_count=0)
                success_count += 1
            except Exception as e:
                logger.error(f"API --- BATCH ERROR --- Article {article_id}: {e}")
                failed_ids.append(article_id)

        return {
            "success": True,
            "total": len(article_ids),
            "queued": success_count,
            "failed": len(failed_ids),
            "failed_ids": failed_ids
        }

    except Exception as e:
        logger.error(f"API --- BATCH ERROR --- {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )