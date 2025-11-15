"""
Prometheus Metrik Tanımları
============================

Bu modül, haber işleme sisteminin tüm Prometheus metriklerini tanımlamada kullanir

Metrik İsimlendirme Kuralı:
    {namespace}_{subsystem}_{metric_adi}_{birim}
    Örnek: app_database_query_duration_seconds

Referanslar:
    - Prometheus Best Practices: https://prometheus.io/docs/practices/naming/
    - RED Metodu: Rate, Errors, Duration
    - USE Metodu: Utilization, Saturation, Errors

Yazar: Berk Yilmaz
Son Güncelleme: 2025-11-12
"""

from prometheus_client import Counter, Histogram, Gauge, Info
import asyncpg
from typing import Optional

# Namespace ve subsystem tanimlarim
NAMESPACE = "app"

# Alt sistem tanimciyilari
SUBSYSTEM_DATABASE = "database"
SUBSYSTEM_NEWS = "news"
SUBSYSTEM_SYSTEM = "system"

# Veritabani Baglanti Havuzu Metrikleri
# PostgreSQL baglanti havuzunun saglik durumunu ve kullanimini taip etmemize yararlar

# Veritabani havuzu metrikleri
db_pool_size = Gauge(
    name="pool_size",
    documentation = (
        "Veritabani baglanti havzunun boyutu"
        "Minimum, maksimum ve mevcut havuz kapasitesini takip eder"
        "Mevcut deger maksimuma yaklasirsa baglanti doygunlugunu onlemek icin"
        "Daha sonrasinda alarm olustur"
    ),
    labelnames=["type"],
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM_DATABASE
)



