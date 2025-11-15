"""
RabbitMQ Queue Test Suite
CloudAMQP bağlantısını ve queue işlemlerini test eder
"""
import asyncio
import logging
import sys
from pathlib import Path

# Projeyi path'e ekle
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from shared.utils.rabbitmq_utils import RabbitMQManager
from shared.utils.rabbitmq_exceptions import ConnectionError, PublishError

# Logging ayarla
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_connection():
    """
    Test 1: Bağlantı testi
    CloudAMQP'ya bağlanabilir miyiz?
    """
    print("\n" + "=" * 70)
    print("TEST 1: BAGLANTI TESTI")
    print("=" * 70)

    rabbitmq = RabbitMQManager()

    try:
        print("\nCloudAMQP'ya baglaniliyor...")
        await rabbitmq.connect()
        print("BASARILI: Baglanti kuruldu")

        # Bağlantı durumunu kontrol et
        is_connected = await rabbitmq.is_connected()
        print(f"Baglanti durumu: {'AKTIF' if is_connected else 'PASIF'}")

        return True

    except ConnectionError as e:
        print(f"HATA: Baglanti hatasi - {e}")
        return False

    except Exception as e:
        print(f"HATA: Beklenmeyen hata - {e}")
        return False

    finally:
        await rabbitmq.disconnect()
        print("Baglanti kapatildi\n")


async def test_queue_setup():
    """
    Test 2: Queue oluşturma testi
    Feature extraction ve DLQ queue'ları oluşturulabilir mi?
    """
    print("\n" + "=" * 70)
    print("TEST 2: QUEUE OLUSTURMA TESTI")
    print("=" * 70)

    rabbitmq = RabbitMQManager()

    try:
        print("\nQueue'lar olusturuluyor...")
        await rabbitmq.connect()

        # Queue'ların varlığını kontrol et
        if rabbitmq.feature_queue_obj is not None:
            print(f"BASARILI: Feature queue olusturuldu - '{rabbitmq.feature_queue}'")

        if rabbitmq.dlq_queue_obj is not None:
            print(f"BASARILI: DLQ olusturuldu - '{rabbitmq.dlq_queue}'")

        return True

    except Exception as e:
        print(f"HATA: Queue olusturma hatasi - {e}")
        return False

    finally:
        await rabbitmq.disconnect()
        print("Baglanti kapatildi\n")


async def test_publish():
    """
    Test 3: Mesaj gönderme testi
    Queue'ya mesaj gönderilebilir mi?
    """
    print("\n" + "=" * 70)
    print("TEST 3: MESAJ GONDERME TESTI")
    print("=" * 70)

    rabbitmq = RabbitMQManager()

    try:
        await rabbitmq.connect()

        # Test mesajı gönder
        test_article_id = 999
        print(f"\nTest mesaji gonderiliyor (article_id: {test_article_id})...")

        success = await rabbitmq.publish(article_id=test_article_id)

        if success:
            print("BASARILI: Mesaj queue'ya eklendi")

            # Metrikleri göster
            metrics = rabbitmq.get_metrics()
            print(f"\nMetrikler:")
            print(f"  - Gonderilen mesaj: {metrics['messages_published']}")
            print(f"  - Hata sayisi: {metrics['publish_errors']}")
            print(f"  - Rate limit hit: {metrics['rate_limit_hits']}")
        else:
            print("HATA: Mesaj gonderilemedi")

        return success

    except PublishError as e:
        print(f"HATA: Publish hatasi - {e}")
        return False

    except Exception as e:
        print(f"HATA: Beklenmeyen hata - {e}")
        return False

    finally:
        await rabbitmq.disconnect()
        print("Baglanti kapatildi\n")


async def test_multiple_publish():
    """
    Test 4: Çoklu mesaj gönderme testi
    Rate limiting kontrol edilir
    """
    print("\n" + "=" * 70)
    print("TEST 4: COKLU MESAJ GONDERME TESTI (RATE LIMITING)")
    print("=" * 70)

    rabbitmq = RabbitMQManager()

    try:
        await rabbitmq.connect()

        # 3 test mesajı gönder
        test_ids = [1001, 1002, 1003]
        print(f"\n{len(test_ids)} adet test mesaji gonderiliyor...")
        print("(Rate limiting aktif - her mesaj arasi bekleme suresi var)\n")

        for article_id in test_ids:
            print(f"Mesaj gonderiliyor: article_id={article_id}")
            await rabbitmq.publish(article_id=article_id)

        # Metrikleri göster
        metrics = rabbitmq.get_metrics()
        print(f"\nBASARILI: Tum mesajlar gonderildi")
        print(f"\nMetrikler:")
        print(f"  - Toplam gonderilen: {metrics['messages_published']}")
        print(f"  - Rate limit hit: {metrics['rate_limit_hits']}")
        print(f"  - Hata sayisi: {metrics['publish_errors']}")

        return True

    except Exception as e:
        print(f"HATA: Coklu mesaj gonderme hatasi - {e}")
        return False

    finally:
        await rabbitmq.disconnect()
        print("Baglanti kapatildi\n")


async def test_metrics():
    """
    Test 5: Metrik toplama testi
    Metrikler doğru mu tutuluyor?
    """
    print("\n" + "=" * 70)
    print("TEST 5: METRIK TOPLAMA TESTI")
    print("=" * 70)

    rabbitmq = RabbitMQManager()

    try:
        await rabbitmq.connect()

        # Birkaç işlem yap
        await rabbitmq.publish(article_id=2001)
        await rabbitmq.publish(article_id=2002)

        # Metrikleri al
        metrics = rabbitmq.get_metrics()

        print("\nMevcut metrikler:")
        print(f"  - messages_published: {metrics['messages_published']}")
        print(f"  - messages_consumed: {metrics['messages_consumed']}")
        print(f"  - publish_errors: {metrics['publish_errors']}")
        print(f"  - consume_errors: {metrics['consume_errors']}")
        print(f"  - reconnections: {metrics['reconnections']}")
        print(f"  - rate_limit_hits: {metrics['rate_limit_hits']}")
        print(f"  - connected: {metrics['connected']}")
        print(f"  - timestamp: {metrics['timestamp']}")

        print("\nBASARILI: Metrikler dogru tutuluyor")
        return True

    except Exception as e:
        print(f"HATA: Metrik testi hatasi - {e}")
        return False

    finally:
        await rabbitmq.disconnect()
        print("Baglanti kapatildi\n")


async def run_all_tests():
    """
    Tüm testleri sırayla çalıştır
    """
    print("\n" + "=" * 70)
    print("RABBITMQ TEST SUITE BASLADI")
    print("=" * 70)

    results = {}

    # Test 1: Bağlantı
    results['connection'] = await test_connection()

    # Test 2: Queue setup
    results['queue_setup'] = await test_queue_setup()

    # Test 3: Mesaj gönderme
    results['publish'] = await test_publish()

    # Test 4: Çoklu mesaj
    results['multiple_publish'] = await test_multiple_publish()

    # Test 5: Metrikler
    results['metrics'] = await test_metrics()

    # Özet
    print("\n" + "=" * 70)
    print("TEST OZETI")
    print("=" * 70)

    for test_name, result in results.items():
        status = "BASARILI" if result else "BASARISIZ"
        print(f"{test_name.upper()}: {status}")

    total = len(results)
    passed = sum(1 for r in results.values() if r)

    print(f"\nToplam: {passed}/{total} test basarili")
    print("=" * 70 + "\n")

    return all(results.values())


if __name__ == "__main__":
    try:
        success = asyncio.run(run_all_tests())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest kullanici tarafindan durduruldu")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nBEKLENMEYEN HATA: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)