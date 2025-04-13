#!/usr/bin/env python3

"""
FTP to Minio Streaming Application

Bu uygulama:
1. FTP mount'dan veri dosyalarını alır
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
import glob
import re
import ssl
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import signal
import sys

from minio import Minio
from minio.error import S3Error

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pyarrow import flight
from pyarrow.flight import FlightClient
import requests

# Logging yapılandırması
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Çevresel değişkenler
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "10.240.49.15:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "xOpk7SNDOdCcPPukQRIh")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "klfyJRGq6xd7EJpiBeXUyfbyxv2dsnh18fzIZoDa")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "vendor3")
SECURE = os.getenv("SECURE", "False").lower() in ["true", "1", "yes"]

# FTP bağlantı bilgileri
FTP_MOUNT_DIR = os.getenv("FTP_MOUNT_DIR", "/mnt/dataroidsftp/Dataroid")
FTP_APP_ID = os.getenv("FTP_APP_ID", "APCJFDXO")

# İşlem yapılandırmaları
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "1020"))  # 60 saniye
ERROR_TOLERANCE = int(os.getenv("ERROR_TOLERANCE", "5"))  # 5 ardışık hata sonrası durdur
PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "snappy")  # snappy, gzip, brotli, zstd
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))  # Maksimum yeniden deneme sayısı

# Dremio bağlantı bilgileri
LOCATION = "grpc://10.240.48.99:31898"
URI = "http://10.240.48.99:31248/apiv2/login"
USERNAME = "admin"
PASSWORD = "adminadmin2025"

def get_token(uri=URI, username=USERNAME, password=PASSWORD):
    """Retrieve PAT Token from Dremio."""
    payload = {"userName": username, "password": password}
    headers = {"Content-Type": "application/json"}
    print(payload)
    try:
        response = requests.post(uri, json=payload, headers=headers)
        print(response)
        response.raise_for_status()
        return response.json().get("token")
    except requests.exceptions.RequestException as e:
        print(f"Error obtaining token: {e}")
        return None

def setup_client(location=LOCATION, token=None):
    """Set up Arrow Flight client with authorization headers."""
    client = FlightClient(location=location, disable_server_verification=False)
    headers = [(b"authorization", f"bearer {token}".encode("utf-8"))] if token else []
    return client, headers

def execute_query(client, query, headers):
    """Execute a query using Arrow Flight Client."""
    try:
        options = flight.FlightCallOptions(headers=headers)
        flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
        results = client.do_get(flight_info.endpoints[0].ticket, options)
        return results.read_all()
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

class FTPToMinioProcessor:
    """FTP mount'dan veri dosyalarını alıp Minio'ya Parquet formatında yazan sınıf."""

    def __init__(self, client, headers):
        """İşlemci nesnesini başlat ve bağlantıları kur."""
        self.minio_client = None
        self.buffer = []
        self.buffer_lock = threading.Lock()
        self.last_flush_time = time.time()
        self.running = False
        self.flush_thread = None
        self.shutdown_event = threading.Event()
        self.client = client
        self.headers = headers
        
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
        """Minio bağlantısını oluştur ve gerekirse bucket'i oluştur."""
        try:
            self.minio_client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=SECURE
            )

            # Bucket'in var olup olmadığını kontrol et
            if not self.minio_client.bucket_exists(MINIO_BUCKET):
                self.minio_client.make_bucket(MINIO_BUCKET)
                logger.info(f"Bucket oluşturuldu: {MINIO_BUCKET}")
            else:
                logger.info(f"Bucket zaten mevcut: {MINIO_BUCKET}")

        except S3Error as e:
            logger.error(f"Minio hatası: {e}")
            raise

    def get_date_params(self, date_str=None):
        """Tarih parametrelerini oluştur. date_str formatı: YYYY-MM-DD"""
        if date_str:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        else:
            date_obj = datetime.now() - timedelta(days=1)  # Varsayılan olarak dünün verisi
        
        return {
            'date_year': date_obj.strftime('%Y'),
            'date_month': date_obj.strftime('%m'),
            'date_day': date_obj.strftime('%d'),
            'date_full': date_obj.strftime('%Y-%m-%d')
        }

    def find_ftp_files(self, date_str=None):
        """Belirtilen tarihe göre FTP mount dizinindeki dosyaları bul."""
        date_params = self.get_date_params(date_str)
        
        # FTP mount path oluştur (dataroidsftp/Dataroid/appId=APCJFDXO/year=$date_year/month=$date_month/day=$date_day)
        ftp_path = os.path.join(
            FTP_MOUNT_DIR, 
            f"appId={FTP_APP_ID}", 
            f"year={date_params['date_year']}", 
            f"month={date_params['date_month']}", 
            f"day={date_params['date_day']}"
        )
        
        logger.info(f"FTP dosyaları aranıyor: {ftp_path}")
        
        # Dizinin var olup olmadığını kontrol et
        if not os.path.exists(ftp_path):
            logger.warning(f"Belirtilen dizin mevcut değil: {ftp_path}")
            return []
        
        # Belirtilen dizindeki tüm dosyaları bul (.gz uzantılı olanlar)
        files = glob.glob(os.path.join(ftp_path, "*.gz"))
        
        logger.info(f"{len(files)} adet .gz dosyası bulundu")
        return files, date_params['date_full']

    def process_file(self, file_path):
        """GZIP dosyasını işle ve içeriği JSON formatında çıkar."""
        try:
            import gzip
            records = []
            
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        # Her satırı JSON olarak ayrıştır
                        record = json.loads(line)
                        
                        # timestamp alanı ekleyerek standardize et
                        if '@timestamp' in record:
                            record['timestamp'] = record['@timestamp']
                        elif 'clientCreationDate' in record:
                            record['timestamp'] = record['clientCreationDate']
                        else:
                            record['timestamp'] = datetime.now().isoformat() + 'Z'
                        
                        records.append(record)
                        
                        # Tampon dolduğunda flush yap
                        if len(records) >= BATCH_SIZE:
                            with self.buffer_lock:
                                self.buffer.extend(records)
                            self._flush_buffer()
                            records = []  # Tamponu temizle
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Geçersiz JSON satırı: {e}")
                        continue
            
            # Kalan kayıtları tampona ekle
            if records:
                with self.buffer_lock:
                    self.buffer.extend(records)
            
            return len(records)
            
        except Exception as e:
            logger.error(f"Dosya işleme hatası ({file_path}): {e}")
            return 0

    def start(self, date_str=None):
        """İşlemeyi başlat."""
        self.running = True
        self.shutdown_event.clear()

        # Periyodik yazma iş parçacığını başlat
        self.flush_thread = threading.Thread(target=self._periodic_flush)
        self.flush_thread.daemon = True
        self.flush_thread.start()

        # FTP dosyalarını bul
        files, date_full = self.find_ftp_files(date_str)
        
        if not files:
            logger.warning("İşlenecek dosya bulunamadı.")
            self.stop()
            return

        logger.info(f"{date_full} tarihi için veri işleme başladı, {len(files)} dosya işlenecek")

        try:
            # Çoklu iş parçacığı ile dosyaları paralel işle
            with ThreadPoolExecutor(max_workers=min(os.cpu_count(), 8)) as executor:
                # İşlenen dosya sayısını takip et
                future_to_file = {executor.submit(self.process_file, file): file for file in files}
                
                total_processed = 0
                for future in ThreadPoolExecutor.as_completed(future_to_file):
                    file = future_to_file[future]
                    try:
                        count = future.result()
                        total_processed += count
                        logger.info(f"Dosya işlendi: {os.path.basename(file)}, {count} kayıt")
                    except Exception as e:
                        logger.error(f"Dosya işleme hatası: {e}")
            
            # Son flush işlemini gerçekleştir
            self._flush_buffer()
            
            logger.info(f"Toplam {total_processed} kayıt işlendi ve Minio'ya yazıldı")

        except KeyboardInterrupt:
            logger.info("Kullanıcı tarafından durduruldu")
        except Exception as e:
            logger.error(f"İşleme hatası: {e}")
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
                backup_file = f"ftp_buffer_backup_{int(time.time())}.json"
                try:
                    with open(backup_file, 'w') as f:
                        json.dump(self.buffer, f)
                    logger.warning(f"Yazılamayan {len(self.buffer)} kayıt {backup_file} dosyasına yedeklendi.")
                except Exception as e:
                    logger.error(f"Yedek dosya oluşturma hatası: {e}. {len(self.buffer)} kayıt kayboldu!")

        # Flush thread'inin tamamlanmasını bekle
        if self.flush_thread and self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)

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
           
            # Datetime timestamp'i "YYYY-MM-DD HH:MM:SS" formatında string'e dönüştür
            df['timestamp_str'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
           
            # Eğer @timestamp alanı varsa, onu da dönüştür
            if '@timestamp' in df.columns:
                try:
                    # Önce datetime'a çevir
                    df['@timestamp'] = pd.to_datetime(df['@timestamp'], errors='coerce')
                    # String formatına dönüştür
                    df['@timestamp_str'] = df['@timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    # NaN değerleri boş string ile değiştir
                    df['@timestamp_str'] = df['@timestamp_str'].fillna('')
                except Exception as e:
                    logger.warning(f"@timestamp dönüştürme hatası: {e}")
   
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
                ids_to_remove = set(id(item) for item in current_data)
                self.buffer = [item for item in self.buffer if id(item) not in ids_to_remove]
                   
        except Exception as e:
            logger.error(f"Buffer flush hatası: {e}")
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
                            # String formatına dönüştür
                            chunk_df['timestamp_str'] = chunk_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                           
                            # Eğer @timestamp alanı varsa, onu da dönüştür
                            if '@timestamp' in chunk_df.columns:
                                try:
                                    chunk_df['@timestamp'] = pd.to_datetime(chunk_df['@timestamp'], errors='coerce')
                                    chunk_df['@timestamp_str'] = chunk_df['@timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                                    chunk_df['@timestamp_str'] = chunk_df['@timestamp_str'].fillna('')
                                except Exception as e_ts:
                                    logger.warning(f"Chunk @timestamp dönüştürme hatası: {e_ts}")
                           
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

    def copy_parquet_to_dremio(self, object_name):
        """COPY Parquet file into Dremio table."""
        start_time = time.time()
        try:
            sql_copy = f"""
            COPY INTO MINIO.vendor3.data.senaryo_1_par
            FROM '@MINIO/vendor3/{object_name}'
            FILE_FORMAT 'parquet';
            """
            print(sql_copy)
            execute_query(self.client, sql_copy, self.headers)
            elapsed_time = time.time() - start_time
            print(f"Parquet files copied into Dremio table in {elapsed_time:.2f} seconds.")
            return elapsed_time
        except Exception as e:
            print(f"Error copying Parquet files to Dremio: {e}")
            sys.exit(1)    

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
            
            # NaN değerleri boş string ile değiştir
            for col in df.columns:
                df[col] = df[col].fillna('')
                df[col] = df[col].astype(str)

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
                        self.copy_parquet_to_dremio(object_name)
                        self.minio_client.remove_object(
                                MINIO_BUCKET,
                                object_name
                        )
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
    logger.info("FTP to Minio veri akış uygulaması başlatılıyor")
    
    # Komut satırı parametrelerini oku
    import argparse
    parser = argparse.ArgumentParser(description='FTP dosyalarını Minio\'ya aktarma')
    parser.add_argument('--date', help='İşlenecek dosyaların tarihi (YYYY-MM-DD formatında)', required=False)
    args = parser.parse_args()
    
    # Dremio token al
    token = "dki7rsmoa0ln7j7mn1286p3eat"  # Doğrudan token tanımlama (Güvenli değil, sadece örnek amaçlıdır)
    if not token:
        token = get_token()
        if not token:
            print("Failed to authenticate with Dremio.")
            sys.exit(1)

    # Set up Arrow Flight client with headers
    client, headers = setup_client(token=token)

    processor = FTPToMinioProcessor(client, headers)

    try:
        processor.start(args.date)
    except KeyboardInterrupt:
        logger.info("Uygulama kullanıcı tarafından sonlandırıldı")
    except Exception as e:
        logger.error(f"Uygulama hatası: {e}")
    finally:
        processor.stop()
        logger.info("Uygulama durduruldu")

if __name__ == "__main__":
    main()