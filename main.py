from fastapi import FastAPI, HTTPException
from typing import List
from .database.database_manager import veri_havuzu_yoneticisi
import logging

app = FastAPI(title="Feature-Extraction-Microservice")
logger = logging.getLogger(__name__)

# Pool olusturma
@app.on_event("startup")
async def startup():
    """
    FastAPI baslarken verihavuzunu ac
    """
    await veri_havuzu_yoneticisi.baglanti_ac()
    logger.info("API --- BASARILI --- Verihavuzu baglantisi hazir")

# Uygulama kapanirken de havuzu temizle
@app.on_event("shutdown")
async def shutdown():
    """FastAPI kapanırken database pool'u kapat"""
    await veri_havuzu_yoneticisi.baglanti_kapat()
    logger.info("API --- TEMIZLENDI --- Database pool kapatıldı")

# Haberleri getiren endpointimiz
@app.get("/api/haberler")
async def haberler_getir(limit: int = 100):
    """
    Semantik analiz bekleyen haberleri getirir
    """
    try:
        haberler = await veri_havuzu_yoneticisi.haberleri_getir(limit=limit)
        return {
            "success": True,
            "count": len(haberler),
            "data": haberler
        }
    except Exception as e:
        logger.error(f"API --- HATA --- Haberler getirilemedi: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Haber durumunu guncelleyen endpointimiz
@app.put("/api/haber/{haber_id}/durum")
async def haber_durumu_guncelle(haber_id: int, yeni_durum: str):
    """
    Haberin durumunu günceller
    """
    try:
        basarili = await veri_havuzu_yoneticisi.haber_durumunu_guncelle(
            haber_id=haber_id,
            yeni_durum=yeni_durum
        )

        if basarili:
            return {
                "success": True,
                "message": f"Haber {haber_id} güncellendi"
            }
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Haber ID {haber_id} bulunamadı"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API --- HATA --- Durum güncellenemedi: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Saglik durumunu getir
@app.get("/health")
async def health_check():
    """Database bağlantısını kontrol et"""
    pool_aktif = veri_havuzu_yoneticisi.pool is not None
    return {
        "status": "healthy" if pool_aktif else "unhealthy",
        "database_pool": "connected" if pool_aktif else "disconnected"
    }