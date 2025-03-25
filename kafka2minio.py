#!/usr/bin/env python3
"""
Kafka to Minio Streaming Application

Bu uygulama:
1. Kafka'dan JSON formatındaki mesajları tüketir
2. Verileri işler ve günlük olarak gruplar
3. İşlenen verileri Minio (S3 uyumlu depolama) içinde Parquet formatında saklar
   ve günlük bölümleme (partitioning) uygular
"""

import os
import json
import time
import uuid
import logging
import tempfile
import threading
import ssl
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import signal

from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# JKS (Java KeyStore) dosyalarını okumak için gerekli kütüphane 
# pip install pyjks cryptography
try:
    import jks
    from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
    JKS_AVAILABLE = True
except ImportError:
    JKS_AVAILABLE = False
    logging.warning("jks ve cryptography kütüphaneleri yüklü değil. JKS truststore/keystore kullanılamayacak. "
                  "Yüklemek için: pip install pyjks cryptography")

# Logging yapılandırması
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Çevresel değişkenler
# Bu değişkenlerin ortam değişkenlerinden alınması daha iyi bir yöntemdir
# Eksik olduğu için burada ekliyoruz
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "10.240.49.15:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "xOpk7SNDOdCcPPukQRIh")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "klfyJRGq6xd7EJpiBeXUyfbyxv2dsnh18fzIZoDa")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "vendor3")
SECURE = os.getenv("SECURE", "False").lower() in ["true", "1", "yes"]

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "lkfkkonp01.aknet.akb:9093,lkfkkonp02.aknet.akb:9093,lkfkkonp03.aknet.akb:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "atmclientlog")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "kafka-minio-group")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL")  # None, SSL, SASL_PLAINTEXT, SASL_SSL
KAFKA_SSL_CHECK_HOSTNAME = os.getenv("KAFKA_SSL_CHECK_HOSTNAME", "False").lower() in ["true", "1", "yes"]
KAFKA_SSL_CAFILE = os.getenv("KAFKA_SSL_CAFILE", None)
KAFKA_SSL_TRUSTSTORE_LOCATION = os.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION", "/tmp/Kafka_Connection/kafka.broker.truststore.jks")
KAFKA_SSL_TRUSTSTORE_PASSWORD = os.getenv("KAFKA_SSL_TRUSTSTORE_PASSWORD", "D-Bo2P4E5v~1")
KAFKA_SSL_KEYSTORE_LOCATION = os.getenv("KAFKA_SSL_KEYSTORE_LOCATION", None)
KAFKA_SSL_KEYSTORE_PASSWORD = os.getenv("KAFKA_SSL_KEYSTORE_PASSWORD", "D-Bo2P4E5v~1")
KAFKA_SSL_KEY_PASSWORD = os.getenv("KAFKA_SSL_KEY_PASSWORD", None)
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", None)  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", None)
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", None)

# İşlem yapılandırmaları
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "60"))  # 60 saniye
ERROR_TOLERANCE = int(os.getenv("ERROR_TOLERANCE", "5"))  # 5 ardışık hata sonrası durdur
PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "snappy")  # snappy, gzip, brotli, zstd
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))  # Maksimum yeniden deneme sayısı


class KafkaToMinioProcessor:
    """Kafka'dan veri tüketen ve Minio'ya Parquet formatında yazan sınıf."""

    def __init__(self):
        """İşlemci nesnesini başlat ve bağlantıları kur."""
        self.consumer = None
        self.minio_client = None
        self.buffer = []
        self.buffer_lock = threading.Lock()
        self.last_flush_time = time.time()
        self.running = False
        self.flush_thread = None
        self.temp_ssl_files = []
        self.shutdown_event = threading.Event()
        
        # Sinyal işleyicilerini ayarla
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Minio bucket kontrolü ve oluşturma
        self._setup_minio()

    def _signal_handler(self, signum, frame):
        """Sinyal yakalayıcı - temiz bir şekilde kapatma için."""
        logger.info(f"Sinyal alındı: {signum}, uygulama durdurulacak")
        self.shutdown_event.set()
        self.stop()

    def _setup_minio(self):
        """Minio bağlantısını oluştur ve gerekirse bucket'ı oluştur."""
        try:
            self.minio_client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=SECURE
            )

            # Bucket'ın var olup olmadığını kontrol et
            if not self.minio_client.bucket_exists(MINIO_BUCKET):
                self.minio_client.make_bucket(MINIO_BUCKET)
                logger.info(f"Bucket oluşturuldu: {MINIO_BUCKET}")
            else:
                logger.info(f"Bucket zaten mevcut: {MINIO_BUCKET}")

        except S3Error as e:
            logger.error(f"Minio hatası: {e}")
            raise

    def connect_kafka(self):
        """Kafka consumer bağlantısını kur ve güvenlik yapılandırmalarını uygula."""
        try:
            # Temel yapılandırma ayarları
            config = {
                'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
                'auto_offset_reset': KAFKA_AUTO_OFFSET_RESET,
                'group_id': KAFKA_GROUP_ID,
                'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
                'enable_auto_commit': True,
                'auto_commit_interval_ms': 5000,  # 5 saniyede bir offset commit
                'session_timeout_ms': 30000,
                'max_poll_interval_ms': 600000  # 10 dakika maksimum işleme süresi
            }

            # Güvenlik protokolünü ayarla
            if KAFKA_SECURITY_PROTOCOL:
                config['security_protocol'] = KAFKA_SECURITY_PROTOCOL
                logger.info(f"Kafka güvenlik protokolü ayarlandı: {KAFKA_SECURITY_PROTOCOL}")

                # SSL/TLS yapılandırması
                if KAFKA_SECURITY_PROTOCOL in ['SSL', 'SASL_SSL']:
                    config['ssl_check_hostname'] = KAFKA_SSL_CHECK_HOSTNAME
                    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                    ssl_context.check_hostname = KAFKA_SSL_CHECK_HOSTNAME
                    ssl_context.verify_mode = ssl.CERT_NONE if not KAFKA_SSL_CHECK_HOSTNAME else ssl.CERT_REQUIRED
                    config['ssl_context'] = ssl_context

                    # Geçici dosya referanslarını saklamak için liste
                    self.temp_ssl_files = []

                    # CA sertifikası veya truststore kullanma konfigürasyonu
                    if KAFKA_SSL_CAFILE:
                        config['ssl_cafile'] = KAFKA_SSL_CAFILE
                        logger.info(f"Kafka SSL CA file ayarlandı: {KAFKA_SSL_CAFILE}")
                    elif KAFKA_SSL_TRUSTSTORE_LOCATION and KAFKA_SSL_TRUSTSTORE_PASSWORD:
                        # JKS truststore kullanımı için gerekli kütüphane kontrolü
                        logger.info(f"Truststore ayarlanıyor")
                        if not JKS_AVAILABLE:
                            raise ImportError("JKS truststore kullanımı için 'pyjks' ve 'cryptography' kütüphaneleri gereklidir.")

                        # JKS truststore'u oku ve PEM formatına dönüştür
                        ks = jks.KeyStore.load(KAFKA_SSL_TRUSTSTORE_LOCATION, KAFKA_SSL_TRUSTSTORE_PASSWORD)

                        # Geçici bir PEM dosyası oluştur
                        ca_temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
                        self.temp_ssl_files.append(ca_temp_file.name)

                        for alias, certificate in ks.certs.items():
                            logger.info(f"Sertifika ekleniyor: {alias}")
                            ca_temp_file.write(certificate.cert)

                        ca_temp_file.close()

                        # PEM dosya yolunu configürasyona ekle
                        config['ssl_cafile'] = ca_temp_file.name
                        logger.info(f"Kafka SSL truststore PEM formatına dönüştürüldü: {ca_temp_file.name}")

                    # Keystore yapılandırması (client sertifikası)
                    if KAFKA_SSL_KEYSTORE_LOCATION and KAFKA_SSL_KEYSTORE_PASSWORD:
                        # JKS keystore kullanımı için gerekli kütüphane kontrolü
                        if not JKS_AVAILABLE:
                            raise ImportError("JKS keystore kullanımı için 'pyjks' ve 'cryptography' kütüphaneleri gereklidir.")

                        # JKS keystore'dan client sertifikası ve anahtarı çıkar
                        ks = jks.KeyStore.load(KAFKA_SSL_KEYSTORE_LOCATION, KAFKA_SSL_KEYSTORE_PASSWORD)

                        # Anahtar parolası (varsa)
                        key_password = KAFKA_SSL_KEY_PASSWORD or KAFKA_SSL_KEYSTORE_PASSWORD

                        # Private key ve sertifikayı PEM formatına dönüştür
                        for alias, private_key in ks.private_keys.items():
                            # Private key için geçici dosya
                            key_temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.key')
                            self.temp_ssl_files.append(key_temp_file.name)
                            key_temp_file.write(private_key.pem.encode('utf-8'))
                            key_temp_file.close()

                            # Sertifika için geçici dosya
                            cert_temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.cert')
                            self.temp_ssl_files.append(cert_temp_file.name)
                            cert_temp_file.write(private_key.cert_chain[0].encode('utf-8'))
                            cert_temp_file.close()

                            config['ssl_certfile'] = cert_temp_file.name
                            config['ssl_keyfile'] = key_temp_file.name

                            logger.info(f"Kafka SSL client sertifikası ve anahtarı ayarlandı")
                            break  # İlk private key'i kullan

                # SASL yapılandırması
                if KAFKA_SECURITY_PROTOCOL in ['SASL_PLAINTEXT', 'SASL_SSL']:
                    if KAFKA_SASL_MECHANISM:
                        config['sasl_mechanism'] = KAFKA_SASL_MECHANISM

                        if KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD:
                            if KAFKA_SASL_MECHANISM == 'PLAIN':
                                config['sasl_plain_username'] = KAFKA_SASL_USERNAME
                                config['sasl_plain_password'] = KAFKA_SASL_PASSWORD
                            else:  # SCRAM-SHA-256 veya SCRAM-SHA-512
                                config['sasl_username'] = KAFKA_SASL_USERNAME
                                config['sasl_password'] = KAFKA_SASL_PASSWORD

                            logger.info(f"Kafka SASL kimlik doğrulama mekanizması ayarlandı: {KAFKA_SASL_MECHANISM}")

            # Kafka Consumer'ı oluştur
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                **config
            )
            logger.info(f"Kafka Consumer başarıyla bağlandı: {KAFKA_TOPIC}")
        except Exception as e:
            logger.error(f"Kafka bağlantı hatası: {e}")
            self._cleanup_temp_files()
            raise

    def _cleanup_temp_files(self):
        """Oluşturulan geçici SSL dosyalarını temizle."""
        if hasattr(self, 'temp_ssl_files'):
            for file_path in self.temp_ssl_files:
                try:
                    if os.path.exists(file_path):
                        os.unlink(file_path)
                        logger.debug(f"Geçici SSL dosyası silindi: {file_path}")
                except Exception as e:
                    logger.warning(f"Geçici dosya silinirken hata oluştu: {e}")

    def _process_message(self, message):
        """
        Kafka'dan gelen JSON formatındaki ATM log mesajını işler.

        Args:
            message: Kafka'dan gelen ham mesaj
    
        Returns:
            İşlenmiş kayıt (dictionary) veya işleme hatası durumunda None
        """
        try:
            # Gelen mesajın kendisi zaten JSON
            record = message.value
    
            # message alanı da ayrı bir JSON string olabilir, bunu da ayrıştıralım
            if 'message' in record and isinstance(record['message'], str):
                try:
                    # İç message'ı da JSON olarak ayrıştır
                    inner_message = json.loads(record['message'])
    
                    # İç mesajı ana kayda ekle (opsiyonel)
                    record['parsed_message'] = inner_message
    
                    # Timestamp alanlarını standartlaştır
                    if '@timestamp' in record:
                        # Ana timestamp'i kullan
                        timestamp_str = record['@timestamp']
                        record['timestamp'] = timestamp_str
                    elif 'transaction' in inner_message and 'timestamp' in inner_message['transaction']:
                        # İç mesajdaki timestamp'i kullan
                        timestamp_str = inner_message['transaction']['timestamp']
                        record['timestamp'] = timestamp_str
                    else:
                        # Geçerli zamanı kullan
                        record['timestamp'] = datetime.now().isoformat() + 'Z'
                except json.JSONDecodeError as e:
                    logger.warning(f"İç mesaj JSON ayrıştırma hatası: {e}. Orijinal mesaj korunuyor.")
                    # timestamp yoksa ekle
                    if '@timestamp' in record:
                        record['timestamp'] = record['@timestamp']
                    else:
                        record['timestamp'] = datetime.now().isoformat() + 'Z'
            elif '@timestamp' in record:
                # message alanı yoksa ama @timestamp varsa
                record['timestamp'] = record['@timestamp']
            else:
                # Hiçbir timestamp alanı yoksa
                record['timestamp'] = datetime.now().isoformat() + 'Z'
    
            # Veriyi düzleştir (opsiyonel, büyük iç içe yapılar için faydalı olabilir)
            # Bu adımı ihtiyaca göre etkinleştirebilirsiniz
            # record = self._flatten_record(record)
    
            return record
        except Exception as e:
            logger.error(f"Mesaj işleme hatası: {e}")
            return None

    def start(self):
        """İşlemeyi başlat."""
        if self.consumer is None:
            self.connect_kafka()

        self.running = True
        self.shutdown_event.clear()

        # Periyodik yazma iş parçacığını başlat
        self.flush_thread = threading.Thread(target=self._periodic_flush)
        self.flush_thread.daemon = True
        self.flush_thread.start()

        logger.info("Veri işleme başladı")

        try:
            # Ana mesaj işleme döngüsü
            consecutive_errors = 0
            processed_count = 0

            for message in self.consumer:
                if self.shutdown_event.is_set() or not self.running:
                    break

                try:
                    # Mesajı işle
                    record = self._process_message(message)

                    if record is not None:
                        # Mesajı tampon belleğe ekle
                        with self.buffer_lock:
                            self.buffer.append(record)

                        processed_count += 1
                        if processed_count % 1000 == 0:
                            logger.info(f"{processed_count} mesaj işlendi")

                        # Tampon boyutu kontrol et
                        if len(self.buffer) >= BATCH_SIZE:
                            self._flush_buffer()

                        # Başarılı işlem sonrası hata sayacını sıfırla
                        consecutive_errors = 0
                    else:
                        logger.warning("Mesaj işlenemedi ve atlandı")

                except Exception as e:
                    logger.error(f"Mesaj işleme hatası: {e}, mesaj: {message}")
                    consecutive_errors += 1

                    # Belirli sayıda art arda hata olursa işlemi durdur
                    if consecutive_errors >= ERROR_TOLERANCE:
                        logger.critical(f"{ERROR_TOLERANCE} adet art arda mesaj işleme hatası. Uygulama durduruluyor.")
                        break

        except KeyboardInterrupt:
            logger.info("Kullanıcı tarafından durduruldu")
        except Exception as e:
            logger.error(f"Kafka tüketim hatası: {e}")
        finally:
            self.stop()

    def stop(self):
        """İşlemeyi durdur ve kaynakları temizle."""
        logger.info("İşleme durduruluyor...")
        self.running = False
        self.shutdown_event.set()

        # Tampon belleği son kez yaz ve kaybı önle
        if self.buffer:
            retry_count = 0
            while self.buffer and retry_count < MAX_RETRY_ATTEMPTS:
                try:
                    self._flush_buffer()
                    break  # Başarılı yazma işlemi
                except Exception as e:
                    retry_count += 1
                    logger.warning(f"Son veri yazmada hata: {e}, Deneme {retry_count}/{MAX_RETRY_ATTEMPTS}")
                    time.sleep(2)  # Tekrar denemeden önce bekle
            
            if self.buffer:
                # Son çare olarak verileri dosyaya kaydet
                backup_file = f"kafka_buffer_backup_{int(time.time())}.json"
                try:
                    with open(backup_file, 'w') as f:
                        json.dump(self.buffer, f)
                    logger.warning(f"Yazılamayan {len(self.buffer)} kayıt {backup_file} dosyasına yedeklendi.")
                except Exception as e:
                    logger.error(f"Yedek dosya oluşturma hatası: {e}. {len(self.buffer)} kayıt kayboldu!")

        # Kafka consumer'ı kapat
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka bağlantısı kapatıldı")

        # Flush thread'inin tamamlanmasını bekle
        if self.flush_thread and self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)

        # Geçici SSL dosyalarını temizle
        self._cleanup_temp_files()

        logger.info("İşleme durduruldu")

    def _periodic_flush(self):
        """Belirli aralıklarla tampon belleği boşaltan iş parçacığı."""
        while self.running and not self.shutdown_event.is_set():
            current_time = time.time()
            if current_time - self.last_flush_time >= FLUSH_INTERVAL:
                with self.buffer_lock:
                    if self.buffer:  # Tampon boş değilse
                        self._flush_buffer()
                    self.last_flush_time = current_time

            # CPU kullanımını azaltmak için kısa bir uyku
            time.sleep(1)

    def _flush_buffer(self):
        """Tampondaki verileri Minio'ya Parquet formatında yaz."""
        with self.buffer_lock:
            if not self.buffer:
                return

            # Tampondaki verileri kopyala, ama henüz tamponu temizleme
            # Başarılı yazma işleminden sonra tamponu temizleyeceğiz
            current_data = self.buffer.copy()

        try:
            # Verileri dataframe'e dönüştür
            df = pd.DataFrame(current_data)

            # Zaman damgasını datetime'a dönüştür
            if 'timestamp' in df.columns:
                if df['timestamp'].dtype == 'object':
                    try:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                    except Exception as e:
                        logger.warning(f"Dataframe timestamp dönüştürme hatası: {e}")
                        # Farklı bir yaklaşım dene
                        try:
                            df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
                        except:
                            logger.warning("ISO8601 formatı ile dönüştürme başarısız")
                            # Son bir deneme daha
                            try:
                                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                                # NaT değerlerini şu anki zamanla doldur
                                df['timestamp'] = df['timestamp'].fillna(pd.Timestamp.now())
                            except:
                                logger.error("Timestamp dönüştürülemedi, işleme devam edilemiyor")
                                return
            else:
                # Timestamp kolonu yoksa şu anki zamanı ekle
                df['timestamp'] = pd.Timestamp.now()

            # Tarih, saat ve dakika kolonları ekle - partition için kullanılacak
            df['date'] = df['timestamp'].dt.date
            df['hour'] = df['timestamp'].dt.hour
            df['minute'] = df['timestamp'].dt.minute

            # Tarih/saat/dakika bazında grupla ve her birim için bir Parquet dosyası oluştur
            for date_group, date_df in df.groupby('date'):
                for hour_group, hour_df in date_df.groupby('hour'):
                    for minute_group, minute_df in hour_df.groupby('minute'):
                        # Partition dizin yapısı oluştur: data/date=YYYY-MM-DD/hour=HH/minute=MM/
                        self._write_parquet_to_minio(minute_df, date_group, hour_group, minute_group)

            logger.info(f"{len(current_data)} mesaj işlendi ve dakikalık partitionlara bölünerek Minio'ya yazıldı")
            
            # Başarılı yazma sonrası tamponu temizle
            with self.buffer_lock:
                # current_data içindeki tüm kayıtları buffer'dan çıkar
                # buffer'da başka thread'ler tarafından eklenen yeni kayıtlar olabilir
                # dolayısıyla buffer = [] yapmak yerine, bildiğimiz kayıtları çıkarıyoruz
                if current_data:
                    # İşlenmiş kayıtları tamponda ara ve çıkar
                    ids_to_remove = set(id(item) for item in current_data)
                    self.buffer = [item for item in self.buffer if id(item) not in ids_to_remove]
                    
        except Exception as e:
            logger.error(f"Buffer flush hatası: {e}")
            # Hatada bile verileri kaybetmemek için tampona geri koy
            # Buffer'ı baştan temizlemediğimiz için burada eklemeye gerek yok
            
            # Kritik hata durumunda yeniden deneme
            retry_count = 0
            while retry_count < MAX_RETRY_ATTEMPTS - 1:  # Bir deneme zaten yapıldı
                retry_count += 1
                logger.warning(f"Buffer flush yeniden deneniyor. Deneme {retry_count+1}/{MAX_RETRY_ATTEMPTS}")
                time.sleep(5)  # Beş saniye bekle
                try:
                    # Batch'i daha küçük parçalara bölelim
                    chunk_size = len(current_data) // 2
                    if chunk_size < 10:  # Çok küçük parçalara bölme
                        chunk_size = 10
                    
                    # Daha küçük parçalar halinde yazma işlemi
                    for i in range(0, len(current_data), chunk_size):
                        chunk = current_data[i:i+chunk_size]
                        chunk_df = pd.DataFrame(chunk)
                        if 'timestamp' in chunk_df.columns:
                            chunk_df['timestamp'] = pd.to_datetime(chunk_df['timestamp'], errors='coerce')
                            chunk_df['date'] = chunk_df['timestamp'].dt.date
                            chunk_df['hour'] = chunk_df['timestamp'].dt.hour
                            chunk_df['minute'] = chunk_df['timestamp'].dt.minute
                            
                            for date_group, date_df in chunk_df.groupby('date'):
                                for hour_group, hour_df in date_df.groupby('hour'):
                                    for minute_group, minute_df in hour_df.groupby('minute'):
                                        self._write_parquet_to_minio(minute_df, date_group, hour_group, minute_group)
                    
                    logger.info(f"Parçalı yazma başarılı oldu. {len(current_data)} kayıt işlendi.")
                    
                    # Başarılı yazma sonrası tamponu güncelle
                    with self.buffer_lock:
                        ids_to_remove = set(id(item) for item in current_data)
                        self.buffer = [item for item in self.buffer if id(item) not in ids_to_remove]
                    break  # Başarılı oldu, döngüden çık
                except Exception as e2:
                    logger.error(f"Parçalı yazma denemesi başarısız: {e2}")

    def _write_parquet_to_minio(self, df, date, hour, minute):
        """Parquet dosyasını oluştur ve Minio'ya yükle."""
        # Başarı durumunu izlemek için değişken
        success = False
        temp_file = None
        
        try:
            # Partition kolonlarını temizle - artık gerekli değil
            if 'date' in df.columns and 'hour' in df.columns and 'minute' in df.columns:
                df = df.drop(['date', 'hour', 'minute'], axis=1)
            
            # Geçerli dataframe kontrolü - boş dataframe veya tüm kolonlar NaN ise işleme devam etme
            if df.empty or df.isna().all().all():
                logger.warning("Boş veya tüm değerleri NaN olan dataframe, işlem atlanıyor.")
                return True  # Başarılı kabul et
            
            # Veritipi koruması iyileştirildi - string dönüşümü yerine daha esnek bir yaklaşım kullanılıyor
            # NaN değerleri uygun tiplerle doldur, sayısal kolon verileri korunsun
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('')
                elif pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(0)
                elif pd.api.types.is_datetime64_dtype(df[col]):
                    df[col] = df[col].fillna(pd.Timestamp.now())
                else:
                    df[col] = df[col].fillna('')
            
            # delete=False kullanarak dosyayı açıyoruz ki hata durumunda dosyayı inceleyebilelim
            temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
            temp_file_path = temp_file.name
            temp_file.close()  # Dosyayı kapatarak PyArrow'un yazabilmesini sağla
            
            try:
                # PyArrow tablosu oluştur ve dosyaya yaz
                table = pa.Table.from_pandas(df)
                pq.write_table(
                    table,
                    temp_file_path,
                    compression=PARQUET_COMPRESSION
                )
                
                # Minio'ya yükleme
                date_str = date.strftime("%Y-%m-%d")
                object_name = f"data/date={date_str}/hour={hour:02d}/minute={minute:02d}/{uuid.uuid4()}.parquet"
                
                # Yeniden deneme mekanizması
                retry_count = 0
                while retry_count < MAX_RETRY_ATTEMPTS:
                    try:
                        self.minio_client.fput_object(
                            MINIO_BUCKET,
                            object_name,
                            temp_file_path
                        )
                        logger.info(f"Parquet dosyası yazıldı: {object_name}, {len(df)} satır")
                        success = True
                        break  # Başarılı oldu, döngüden çık
                    except Exception as e:
                        retry_count += 1
                        logger.warning(f"Minio yazma hatası: {e}, Deneme {retry_count}/{MAX_RETRY_ATTEMPTS}")
                        if retry_count >= MAX_RETRY_ATTEMPTS:
                            raise  # Yeniden deneme sayısı aşıldı, hatayı yukarı ilet
                        time.sleep(2)  # Tekrar denemeden önce bekle
            finally:
                # Başarı durumu ne olursa olsun, geçici dosyayı temizle
                try:
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
                except Exception as e:
                    logger.warning(f"Geçici dosya silinirken hata: {e}")
            
            return success
                        
        except Exception as e:
            logger.error(f"Parquet yazma hatası: {e}")
            # Kritik hata durumunda veriyi kaydetme
            try:
                error_backup_file = f"parquet_error_data_{int(time.time())}.json"
                df.to_json(error_backup_file, orient='records')
                logger.warning(f"Hata durumunda veri {error_backup_file} dosyasına kaydedildi")
            except Exception as backup_err:
                logger.error(f"Hata yedeği oluşturulamadı: {backup_err}")
            
            raise  # Hatayı yukarı ilet, geri dönüş mekanizması için


def main():
    """Ana fonksiyon."""
    logger.info("Kafka to Minio veri akış uygulaması başlatılıyor")
    logger.info(f"Yapılandırma: KAFKA_TOPIC={KAFKA_TOPIC}, MINIO_BUCKET={MINIO_BUCKET}")

    processor = KafkaToMinioProcessor()

    try:
        processor.connect_kafka()
        processor.start()
    except KeyboardInterrupt:
        logger.info("Uygulama kullanıcı tarafından sonlandırıldı")
    except Exception as e:
        logger.error(f"Uygulama hatası: {e}")
    finally:
        processor.stop()
        logger.info("Uygulama durduruldu")

if __name__ == "__main__":
    main()
