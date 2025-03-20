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

# Kafka ve Minio yapılandırması
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "atm_transactions")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "atm-data-processor")
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest")

# Kafka SSL/TLS güvenlik yapılandırması
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SSL_CHECK_HOSTNAME = os.environ.get("KAFKA_SSL_CHECK_HOSTNAME", "True").lower() == "true"

# Kafka truststore yapılandırması
KAFKA_SSL_CAFILE = os.environ.get("KAFKA_SSL_CAFILE", None)  # CA sertifikası dosya yolu
KAFKA_SSL_TRUSTSTORE_LOCATION = os.environ.get("KAFKA_SSL_TRUSTSTORE_LOCATION", None)
KAFKA_SSL_TRUSTSTORE_PASSWORD = os.environ.get("KAFKA_SSL_TRUSTSTORE_PASSWORD", None)

# Kafka keystore yapılandırması
KAFKA_SSL_KEYSTORE_LOCATION = os.environ.get("KAFKA_SSL_KEYSTORE_LOCATION", None)
KAFKA_SSL_KEYSTORE_PASSWORD = os.environ.get("KAFKA_SSL_KEYSTORE_PASSWORD", None)
KAFKA_SSL_KEY_PASSWORD = os.environ.get("KAFKA_SSL_KEY_PASSWORD", None)

# Kafka kimlik doğrulama yapılandırması
KAFKA_SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM", None)  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
KAFKA_SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME", None)
KAFKA_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD", None)

# Minio yapılandırması
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "atm-data")
SECURE = os.environ.get("MINIO_SECURE", "False").lower() == "true"

# Processing yapılandırması
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10000"))  # Kaç mesaj toplandıktan sonra parquet'e yazılacak
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL", "60"))  # Saniye cinsinden yazma sıklığı
COMPRESSION = os.environ.get("COMPRESSION", "snappy")  # Sıkıştırma formatı: snappy, zstd, lz4
ERROR_TOLERANCE = int(os.environ.get("ERROR_TOLERANCE", "100"))  # Art arda gelen hata sayısı toleransı

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
        
        # Minio bucket kontrolü ve oluşturma
        self._setup_minio()
        
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
                'max_poll_interval_ms': 300000  # 5 dakika maksimum işleme süresi
            }
            
            # Güvenlik protokolünü ayarla
            if KAFKA_SECURITY_PROTOCOL:
                config['security_protocol'] = KAFKA_SECURITY_PROTOCOL
                logger.info(f"Kafka güvenlik protokolü ayarlandı: {KAFKA_SECURITY_PROTOCOL}")
                
                # SSL/TLS yapılandırması
                if KAFKA_SECURITY_PROTOCOL in ['SSL', 'SASL_SSL']:
                    config['ssl_check_hostname'] = KAFKA_SSL_CHECK_HOSTNAME
                    
                    # Geçici dosya referanslarını saklamak için liste
                    self.temp_ssl_files = []
                    
                    # CA sertifikası veya truststore kullanma konfigürasyonu
                    if KAFKA_SSL_CAFILE:
                        config['ssl_cafile'] = KAFKA_SSL_CAFILE
                        logger.info(f"Kafka SSL CA file ayarlandı: {KAFKA_SSL_CAFILE}")
                    elif KAFKA_SSL_TRUSTSTORE_LOCATION and KAFKA_SSL_TRUSTSTORE_PASSWORD:
                        # JKS truststore kullanımı için gerekli kütüphane kontrolü
                        if not JKS_AVAILABLE:
                            raise ImportError("JKS truststore kullanımı için 'pyjks' ve 'cryptography' kütüphaneleri gereklidir.")
                        
                        # JKS truststore'u oku ve PEM formatına dönüştür
                        ks = jks.KeyStore.load(KAFKA_SSL_TRUSTSTORE_LOCATION, KAFKA_SSL_TRUSTSTORE_PASSWORD)
                        
                        # Geçici bir PEM dosyası oluştur
                        ca_temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
                        self.temp_ssl_files.append(ca_temp_file.name)
                        
                        for alias, certificate in ks.certs.items():
                            ca_temp_file.write(certificate.cert.encode('utf-8'))
                        
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
                            # Şifreli private key'i çöz
                            pk_data = private_key.pkey
                            pk_password = key_password.encode('utf-8') if key_password else None
                            
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
    
    def start(self):
        """İşlemeyi başlat."""
        if self.consumer is None:
            self.connect_kafka()
        
        self.running = True
        
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
                if not self.running:
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
        
        # Tampon belleği son kez yaz
        if self.buffer:
            self._flush_buffer()
        
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
        while self.running:
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
            
            # Tampondaki verileri kopyala ve tamponu temizle
            current_data = self.buffer.copy()
            self.buffer = []
        
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
        
        except Exception as e:
            logger.error(f"Buffer flush hatası: {e}")
            # Hatada bile verileri kaybetmemek için tampona geri koy
            with self.buffer_lock:
                self.buffer.extend(current_data)
            # Kısa bir süre bekle ve tekrar dene
            time.sleep(5)
    
    def _write_parquet_to_minio(self, df, date, hour, minute):
        """
        DataFrame'i Parquet formatında Minio'ya yaz.
        
        Args:
            df: Yazılacak pandas DataFrame
            date: Tarih (partition için)
            hour: Saat (partition için)
            minute: Dakika (partition için)
        """
        try:
            # DataFrame'den gereksiz partition kolonlarını çıkar (opsiyonel)
            if 'date' in df.columns and 'hour' in df.columns and 'minute' in df.columns:
                df = df.drop(['date', 'hour', 'minute'], axis=1)
            
            # Geçici dosya oluştur
            with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
                # DataFrame'i Parquet olarak yaz
                table = pa.Table.from_pandas(df)
                pq.write_table(
                    table, 
                    temp_file.name,
                    compression=COMPRESSION
                )
                
                # Minio'ya yükle
                date_str = date.strftime("%Y-%m-%d")
                object_name = f"data/date={date_str}/hour={hour:02d}/minute={minute:02d}/{uuid.uuid4()}.parquet"
                
                self.minio_client.fput_object(
                    MINIO_BUCKET,
                    object_name,
                    temp_file.name
                )
                
                logger.info(f"Parquet dosyası yazıldı: {object_name}, {len(df)} satır")
                
        except Exception as e:
            logger.error(f"Parquet yazma hatası: {e}")

def main():
    """Ana fonksiyon."""
    logger.info("Kafka to Minio veri akış uygulaması başlatılıyor")
    
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
