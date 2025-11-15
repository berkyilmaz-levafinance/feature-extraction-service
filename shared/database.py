"""
PostgreSQL veritabani islemleri (ASYNC)
Bu dosya; tum database baglantilari ve SQL islemlerini icerir

En son guncellenme tarihi: 11/11/2025
En son guncelleme yapan kisi: Berk Yilmaz
"""

# ASYNC PostgreSQL icin
import asyncio
import asyncpg
import json

# Logging icin
import logging

# Type/Date duzenlenmes
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException

from .config import config

# Logger kurulumu
logger = logging.getLogger(__name__)

class VeriHavuzuYoneticisi:
    """
    PostgreSQL veritabani islemleri icin olusturulmus class
    Verihavuzu baglantilari ve SQL islemlerini yonetir
    """

    def __init__(self):
        """
        DatabaseManager'i baslatir, baslangicta veri havuzu yok
        İlk kullanim aninda baglanti_ac() cagirilir
        """
        self.pool: Optional[asyncpg.Pool] = None
        # Race conditionu onlemek amaciyla
        self._pool_lock = asyncio.Lock()
        logger.info("DATABASE --- INIT --- DatabaseManager olusturuldu")

    async def baglanti_ac(self):
        """
        PostgreSQL baglanti havuzunu olusturur

        Baglanti sayilari:
        - Minimum: 2 baglanti (her zaman acik)
        - Maximum: 20 baglanti (cok istek gelirse maksimum bu kadar acilir)

        Returns:
            asyncpg.Pool: Baglanti havuzu

        Raises:
            Exception: Baglanti hatasi durumunda
        """

        # baglanti_ac() modu async oldugu icin birden fazla task onu cagirabilir
        # Eger kontrol olmazsa iki gorev ayni anda self.pool is None gorur
        # Iki havuz olustur, postgres'e cifte baglanti acar
        # Bunun olmamamasi icin double-checked locking

        if self.pool is not None:
            logger.debug("DATABASE --- BAGLANTI --- Pool zaten mevcut...")
            return self.pool

        # Pool yok, o zaman pool olusturmamiz lazim
        # Ama once lock alalim, ki ayni anda 2 islem olusturulamasin
        async with self._pool_lock:

            # check 1 - Biz bunu alirken baska islem olusuturuldu mu bak
            if self.pool is not None:
                logger.debug("DATABASE --- CACHE --- Pool başka task tarafindan olusuturulmus...")
                return self.pool

            # Gercekten yok, simdi olusuturalim
            try:
                logger.info("DATABASE --- BAGLANTI --- Connection pool olusturuluyor...")

                self.pool = await asyncpg.create_pool(
                    # Minimum 2 baglanti her zaman acik durmasi lazim
                    min_size=2,
                    # Su anlik deneme icin lock'u 20'ye koyuyorum
                    max_size=20,
                    host=config.vt_host,
                    port=config.vt_port,
                    database=config.vt_isim,
                    user=config.vt_kullanici,
                    password=config.vt_sifre,
                    command_timeout=60
                )

                # Loggerlari gir
                logger.info("DATABASE --- BASARILI --- Connection pool olusturuldu")
                logger.info(f"DATABASE --- INFO --- Min: 2, Max: 20 baglanti")
                return self.pool

            except Exception as e:
                logger.error(f"DATABASE --- HATA --- Pool olusturulamadi: {e}")
                raise

    async def baglanti_kapat(self):
        """
        Veri havuzunu kapatir
        """
        if self.pool:
            logger.info("DATABASE --- KAPANIYOR --- Veri havuzu kapatiliyor")
            await self.pool.close()
            self.pool = None
            logger.info("DATABASE --- KAPANDI --- Pool kapatildi")

    # Haberi ID ile getirmek extractor dosyasinda isimize yariyor
    async def haber_id_ile_getir(self, haber_id: int) -> Optional[Dict[str, Any]]:
        """
        Tek bir haberi ID ile çeker

        Args:
            haber_id: Haberin ID'si

        Returns:
            Dict: Haber bilgileri (id, title, content, source, published_date)
            None: Haber bulunamazsa
        """
        if not self.pool:
            await self.baglanti_ac()

        try:
            async with self.pool.acquire() as conn:
                query = """
                    SELECT 
                        id,
                        title,
                        content,
                        source,
                        published_date
                    FROM news
                    WHERE id = $1
                """

                row = await conn.fetchrow(query, haber_id)

                if row:
                    logger.debug(f"DATABASE --- BASARILI --- Haber ID {haber_id} çekildi")
                    return dict(row)
                else:
                    logger.warning(f"DATABASE --- UYARI --- Haber ID {haber_id} bulunamadı")
                    return None

        except Exception as e:
            logger.error(f"DATABASE --- HATA --- Haber çekme hatası (ID: {haber_id}): {e}")

    # Ilk haberleri cekerken is_duplicate kisminin False olmasi lazim
    # Approved kiminin da semantik_analiz_icin_sirada seklinde olanlari al
    async def haberleri_getir(self, limit: int = 100, max_deneme: int = 3) -> List[Dict[str, Any]]:
        """
        Feature extraction bekleyen haberleri ceker
        # YAZILIMCIYA NOT:
        Bu kisimda approved kismi toplantilarda status olarak belirledik
        daha sonrasinda degistirilebilir
        Sadece approved = 'semantik_analiz_icin_sirada' olan haberleri alir
        En yeni haberlerden baslar (published_date DESC)

        Args:
            # NOT2:
            Su an icin maksimum limiti oylesine koymus gibi oldum
            cunku saniyede 100 haber gelicek sekilde yazildi
            limit: Maksimum kac haber cekilecek (default: 100)

        Returns:
            List[Dict]: Haber listesi
        Raises:
            Exception: Veritabani hatasi
        """
        #

        # Eger havuzda conn yoksa acalim
        if not self.pool:
            await self.baglanti_ac()

        for deneme in range(max_deneme):
            try:
                # Havuzdan bir baglanti al
                async with self.pool.acquire() as conn:
                    logger.info(f"DATABASE --- SORGU --- {limit} haber cekiliyor")

                    # SQL Query
                    query = """
                        SELECT
                            id,
                            title,
                            content,
                            source,
                            published_date
                        FROM news
                        WHERE approved = 'semantik_analiz_icin_sirada'
                            AND is_duplicate = FALSE
                        ORDER BY published_date DESC
                        LIMIT $1
                    """

                    # Async olarak sorguyu calistir
                    rows = await conn.fetch(query, limit)
                    # Objeleri dicte cevir
                    haberler = [dict(row) for row in rows]
                    # Loggera kaydet
                    logger.info(f"DATABASE --- BASARILI --- {len(haberler)} haber cekildi")
                    return haberler

            except asyncpg.PostgresError as e:
                # Kacinci deneme olduguna bak, son denemeyse bitir
                if deneme == max_deneme - 1:
                    logger.error(f"DATABASE --- KRITIK HATA --- Tum denemeler basarisiz: {e}")
                    raise

                # Burasi degistirildi *Diger yazilimcilara not
                # Bu kisimda exponential backoff + jitter deniyorum
                # Deneme amaciyla yazildi, daha sonra degistirilebilir
                loop = asyncio.get_running_loop()
                jitter = (loop.time() % 0.25)
                bekleme_suresi = min(8, 2 ** deneme) + jitter
                logger.warning(
                    f"DATABASE --- HATA --- Deneme {deneme + 1} basarisiz: {e}. "
                    f"{bekleme_suresi}s sonra tekrar denenecek."
                )

                # Bekleme_suresi kadar bekle
                await asyncio.sleep(bekleme_suresi)

            except Exception as e:
                logger.error(f"DATABASE --- HATA --- Beklenmedik Hata: {e}")
                raise

    async def haber_durumunu_guncelle(
            self,
            haber_id: int,
            yeni_durum: str,
            max_deneme: int = 3,
    )-> bool:
        """
        Haberin durumunu gunceller, Queue Yoneticisi haberi RabbitMQ'ya gondermeden once
        "processing yapar", feature worker islemi tamamlyainca "semantik analizden gecti"
        olarak degistirir
        
        Tekrar deneme mekanizmasi:
            Database hatasi durumunda otomatik olarak tekrar tekrar dene
            Her denemede bekleme suresini eksponensiyel olarak arttir 
        
        Args:
            haber_id: Guncellenecek haberin ID'si
            yeni_durum: Yeni durum degeri
                    Ornek: 'processing', 'semantik_analizden_gecti', 'failed'
            max_deneme: Maksimum kac kez tekrar denenecek (default: 3)
    
        Returns:
            bool: Guncelleme basarili ise True, degilse False
        
        Raises:
            Exception: Tum denemeler basarisiz olursa
        """
        # Havuz kontrolu yap
        if not self.pool:
            await self.baglanti_ac()
                                      
        # Basarisiz olunursa tekrar denenmesi icin
        for deneme in range(max_deneme):
            try:
                # Baglanti al
                async with self.pool.acquire() as conn:
                    logger.info(
                        f"DATABASE --- GUNCELLEME --- Haber ID {haber_id} durumu guncelleniyor: {yeni_durum} "
                        f"(Deneme: {deneme + 1}/{max_deneme})"
                    )

                    # UPDATE SORGUSU
                    # approved kolonunu gunceller
                    query = """
                        UPDATE news
                        SET
                            approved = $1,
                            updated_at = NOW()
                        WHERE id = $2
                    """
                    # Sorguyu calistir
                    sonuc = await conn.execute(query, yeni_durum, haber_id)
                    #Kac satir guncellendi kontrol et
                    guncellenen_satir_sayisi = int(sonuc.split()[-1])
                    if guncellenen_satir_sayisi > 0:
                        logger.info(f"DATABASE --- BASARILI --- Haber ID {haber_id} guncellendi: {yeni_durum}")
                        return True
                    else:
                        logger.warning(f"DATABASE --- UYARI --- Haber ID {haber_id} bulunamadi")
                        return False

            except asyncpg.PostgresError as e:
                # Son deneme mi?
                if deneme == max_deneme - 1:
                    logger.error(f"DATABASE --- KRITIK HATA --- Tum denemeler basarisiz (Haber ID: {haber_id}): {e}")
                    raise

                # haberleri cekme fonksiyonunda kullandigim ayni metod
                loop = asyncio.get_running_loop()
                jitter = (loop.time() % 0.25)
                bekleme_suresi = min(8, 2 ** deneme) + jitter

                logger.warning(
                    f"DATABASE --- HATA --- Deneme {deneme + 1} basarisiz (Haber ID: {haber_id}): {e}. "
                    f"{bekleme_suresi}s sonra tekrar denenecek..."
                )
                # Bekle
                await asyncio.sleep(bekleme_suresi)

            except Exception as e:
                logger.error(f"DATABASE --- HATA --- Beklenmedik Hata (Haber ID: {haber_id}): {e}")
                raise
  
# Global instance
veri_havuzu_yoneticisi = VeriHavuzuYoneticisi()