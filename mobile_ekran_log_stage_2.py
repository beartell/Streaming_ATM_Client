#!/usr/bin/env python3

"""
Spark SQL'i Dremio SQL'e Dönüştürme ve Uygulama

Bu script:
1. Dremio'ya bağlanır 
2. paste-2.txt'deki Spark SQL işlemlerini Dremio SQL'e çevirir
3. Her adımı sırasıyla Dremio'da uygular
"""

import os
import time
import logging
import sys
from datetime import datetime, timedelta

import requests
from pyarrow import flight
from pyarrow.flight import FlightClient

# Logging yapılandırması
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Dremio bağlantı bilgileri
LOCATION = os.getenv("DREMIO_LOCATION", "grpc://10.240.48.99:31898")
URI = os.getenv("DREMIO_URI", "http://10.240.48.99:31248/apiv2/login")
USERNAME = os.getenv("DREMIO_USERNAME", "admin")
PASSWORD = os.getenv("DREMIO_PASSWORD", "adminadmin2025")

def get_token(uri=URI, username=USERNAME, password=PASSWORD):
    """Dremio'dan PAT Token alır."""
    payload = {"userName": username, "password": password}
    headers = {"Content-Type": "application/json"}
    logger.info(f"Dremio'ya bağlanılıyor: {uri}")
    
    try:
        response = requests.post(uri, json=payload, headers=headers)
        response.raise_for_status()
        token = response.json().get("token")
        logger.info("Token başarıyla alındı")
        return token
    except requests.exceptions.RequestException as e:
        logger.error(f"Token alma hatası: {e}")
        return None

def setup_client(location=LOCATION, token=None):
    """Arrow Flight client'ı yetkilendirme başlıklarıyla ayarlar."""
    logger.info(f"Flight Client kuruluyor: {location}")
    client = FlightClient(location=location, disable_server_verification=True)
    headers = [(b"authorization", f"bearer {token}".encode("utf-8"))] if token else []
    return client, headers

def execute_query(client, query, headers, description=None, log_success=True):
    """Arrow Flight Client kullanarak sorgu çalıştırır."""
    if description:
        logger.info(f"Sorgu çalıştırılıyor: {description}")
    
    start_time = time.time()
    try:
        options = flight.FlightCallOptions(headers=headers)
        flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)
        results = client.do_get(flight_info.endpoints[0].ticket, options)
        result_data = results.read_all()
        
        end_time = time.time()
        if log_success:
            logger.info(f"Sorgu başarıyla tamamlandı. Süre: {end_time - start_time:.2f} saniye")
        return result_data
    except Exception as e:
        logger.error(f"Sorgu çalıştırma hatası: {e}")
        logger.error(f"Hatalı sorgu: {query}")
        return None

def create_source_tables_if_not_exists(client, headers):
    """Gerekli kaynak tabloları oluşturur."""
    logger.info("Kaynak tabloların kontrolü yapılıyor")
    
    # Minio kaynak için tablolar
    create_minio_source_query = """
    CREATE SOURCE IF NOT EXISTS MINIO (
        type => 'S3',
        connection => {
            accessKey => 'xOpk7SNDOdCcPPukQRIh',
            bucket => 'vendor3',
            endpoint => '10.240.49.15:9000',
            extractHosts => false,
            regionOrEndpointFlag => 'endpoint',
            secure => false,
            secretKey => 'klfyJRGq6xd7EJpiBeXUyfbyxv2dsnh18fzIZoDa'
        }
    );
    """
    
    execute_query(client, create_minio_source_query, headers, 
                 "Minio kaynak oluşturma", log_success=True)
    
    # senaryo_1_par tablosu oluşturma
    create_senaryo_table_query = """
    CREATE TABLE IF NOT EXISTS MINIO.vendor3.data.senaryo_1_par AS 
    SELECT * FROM (VALUES(1)) WHERE 1=0;
    """
    
    execute_query(client, create_senaryo_table_query, headers, 
                 "senaryo_1_par tablosu oluşturma", log_success=True)
    
    # impala_mobil_ekran_logu_stg tablosu oluşturma
    create_stg_table_query = """
    CREATE TABLE IF NOT EXISTS MINIO.vendor3.impala_mobillog.impala_mobil_ekran_logu_stg (
        log_text STRING
    );
    """
    
    execute_query(client, create_stg_table_query, headers, 
                 "impala_mobil_ekran_logu_stg tablosu oluşturma", log_success=True)

def step2_unzip_and_load_stg(client, headers):
    """
    Adım 2: FTP ortamındaki dosyalar zipli olması nedeniyle öncelikle hdfs ortamındaki 
    veriye unzip işlemi uygulanır ve STG tablosuna yüklenir.
    
    Spark kodu:
    val read_gzip = spark.read.option("compression","gzip").text("/user/mujdat/MOBIL/MOBIL_EKRAN_LOGU_STG_ZIP")
    read_gzip.write.mode("overwrite").insertInto("impala_mobillog.impala_mobil_ekran_logu_stg")
    """
    logger.info("Adım 2: GZIP verilerini STG tablosuna yükleme")
    
    # Dremio SQL'de gzip dosyalarını direkt okuma
    read_and_load_query = """
    -- Önce tabloyu temizle
    DELETE FROM MINIO.vendor3.impala_mobillog.impala_mobil_ekran_logu_stg WHERE 1=1;
    
    -- Parquet dosyalarından log_text kolonunu al
    INSERT INTO MINIO.vendor3.impala_mobillog.impala_mobil_ekran_logu_stg (log_text)
    SELECT 
        CAST(parsed_message AS VARCHAR) AS log_text
    FROM 
        MINIO.vendor3.data.senaryo_1_par
    WHERE 
        parsed_message IS NOT NULL;
    """
    
    execute_query(client, read_and_load_query, headers, 
                 "GZIP verilerini STG tablosuna yükleme", log_success=True)

def step3_create_partitioned_table(client, headers):
    """
    Adım 3: STG tablosundan veriyi partitionlı tabloya aktarır.
    
    Spark kodu:
    val data = spark.sql("select log_text,count(*) as adet, 
                         substr(get_json_object(log_text, '$.clientCreationDate'),1,10) as tarih 
                         from impala_mobillog.impala_mobil_ekran_logu_stg 
                         group by log_text,substr(get_json_object(log_text, '$.clientCreationDate'),1,10) 
                         distribute by tarih")
    data.repartition((data.count / 6500000).toInt + 1).write.mode("overwrite")
        .insertInto("impala_mobillog.impala_mobil_ekran_logu_parquet_partitioned")
    """
    logger.info("Adım 3: Partitionlı tablo oluşturma ve veri aktarımı")
    
    # Önce partitionlı tablo oluştur
    create_partitioned_table_query = """
    CREATE TABLE IF NOT EXISTS MINIO.vendor3.impala_mobillog.impala_mobil_ekran_logu_parquet_partitioned (
        log_text STRING,
        adet BIGINT,
        tarih STRING
    )
    PARTITION BY (tarih);
    """
    
    execute_query(client, create_partitioned_table_query, headers, 
                 "Partitionlı tablo oluşturma", log_success=True)
    
    # STG tablosundan veriyi partitionlı tabloya aktar
    load_partitioned_query = """
    -- Önce tabloyu temizle
    DELETE FROM MINIO.vendor3.impala_mobillog.impala_mobil_ekran_logu_parquet_partitioned WHERE 1=1;
    
    -- Verileri grupla ve yükle
    INSERT INTO MINIO.vendor3.impala_mobillog.impala_mobil_ekran_logu_parquet_partitioned (log_text, adet, tarih)
    SELECT 
        log_text,
        COUNT(*) AS adet, 
        SUBSTR(JSON_VALUE(log_text, '$.clientCreationDate'), 1, 10) AS tarih
    FROM 
        MINIO.vendor3.impala_mobillog.impala_mobil_ekran_logu_stg
    WHERE 
        JSON_VALUE(log_text, '$.clientCreationDate') IS NOT NULL
    GROUP BY 
        log_text, 
        SUBSTR(JSON_VALUE(log_text, '$.clientCreationDate'), 1, 10)
    DISTRIBUTE BY tarih;
    """
    
    execute_query(client, load_partitioned_query, headers, 
                 "Verileri partitionlı tabloya aktarma", log_success=True)

def step4_transform_to_structured(client, headers):
    """
    Adım 4: Partitionlı tutulan tablodaki log_text kolonunun parse edilip,
    transformasyonlar uygulandıktan sonra structured formatta bir tabloya yazılır.
    
    Bu adım çok uzun bir SQL sorgusuna sahip, Dremio özelinde optimize edilecek.
    """
    logger.info("Adım 4: Structured tablo oluşturma ve transformasyon")
    
    # Structured tablo oluştur
    create_structured_table_query = """
    CREATE TABLE IF NOT EXISTS MINIO.vendor3.impala_mobillog.mobil_ekran_logu_structured (
        customerid STRING,
        atr_duration DOUBLE,
        atr_entertime TIMESTAMP,
        atr_exittime TIMESTAMP,
        atr_viewclass STRING,
        atr_viewid STRING,
        atr_viewlabel STRING,
        atr_viewdisplayname STRING,
        clientcreationdate TIMESTAMP,
        cs_duration DOUBLE,
        cs_sessionid STRING,
        cs_startdatetime STRING,
        cs_stopdatetime STRING,
        eventid STRING,
        eventname STRING,
        eventdisplayname STRING,
        dp_platform STRING,
        dp_appversionname STRING,
        atr_classname STRING,
        atr_label STRING,
        atr_componentid STRING,
        message_str STRING,
        enter_time STRING,
        exit_time STRING,
        duration DOUBLE,
        enter_time_hour INTEGER,
        cs_start_hour INTEGER,
        tarih_str STRING,
        tarih STRING
    )
    PARTITION BY (tarih);
    """
    
    execute_query(client, create_structured_table_query, headers, 
                 "Structured tablo oluşturma", log_success=True)
    
    # Geçici view oluştur - temptable benzeri
    create_temp_view_query = """
    CREATE OR REPLACE VIEW MINIO.vendor3.impala_mobillog.temptable AS
    SELECT 
        SUBSTR(JSON_VALUE(d.log_text, '$.clientCreationDate'), 1, 10) AS tarih,
        JSON_VALUE(d.log_text, '$.customerId') AS customerid,
        CAST(JSON_VALUE(d.log_text, '$.attributes.duration') AS DOUBLE) AS atr_duration,
        CAST(REPLACE(SUBSTR(JSON_VALUE(d.log_text, '$.attributes.enterTime'), 1, 23), 'T', ' ') AS TIMESTAMP) AS atr_entertime,
        CAST(REPLACE(SUBSTR(JSON_VALUE(d.log_text, '$.attributes.exitTime'), 1, 23), 'T', ' ') AS TIMESTAMP) AS atr_exittime,
        JSON_VALUE(d.log_text, '$.attributes.viewClass') AS atr_viewclass,
        JSON_VALUE(d.log_text, '$.attributes.viewId') AS atr_viewid,
        JSON_VALUE(d.log_text, '$.attributes.viewLabel') AS atr_viewlabel,
        JSON_VALUE(d.log_text, '$.attributes._viewDisplayName') AS atr_viewdisplayname,
        CAST(REPLACE(SUBSTR(JSON_VALUE(d.log_text, '$.clientCreationDate'), 1, 23), 'T', ' ') AS TIMESTAMP) AS clientcreationdate,
        CAST(JSON_VALUE(d.log_text, '$.clientSession.duration') AS DOUBLE) AS cs_duration,
        JSON_VALUE(d.log_text, '$.clientSession.sessionId') AS cs_sessionid,
        SUBSTR(CAST(CAST(JSON_VALUE(d.log_text, '$.clientSession.startDateTime') AS BIGINT) / 1000 AS TIMESTAMP), 1, 23) AS cs_startdatetime,
        SUBSTR(CAST(CAST(JSON_VALUE(d.log_text, '$.clientSession.stopDateTime') AS BIGINT) / 1000 AS TIMESTAMP), 1, 23) AS cs_stopdatetime,
        JSON_VALUE(d.log_text, '$.eventId') AS eventid,
        JSON_VALUE(d.log_text, '$._eventName') AS eventname,
        JSON_VALUE(d.log_text, '$._eventDisplayName') AS eventdisplayname,
        JSON_VALUE(d.log_text, '$.deviceProperty.platform') AS dp_platform,
        JSON_VALUE(d.log_text, '$.deviceProperty.appVersionName') AS dp_appVersionName,
        JSON_VALUE(d.log_text, '$.attributes.className') AS atr_className,
        JSON_VALUE(d.log_text, '$.attributes.label') AS atr_label,
        JSON_VALUE(d.log_text, '$.attributes.componentId') AS atr_componentId,
        JSON_VALUE(d.log_text, '$.attributes.dataroidEventKey_string') AS dataroidEventKey,
        JSON_VALUE(d.log_text, '$.attributes._inappMessageName') AS inappMessageName,
        JSON_VALUE(d.log_text, '$.attributes.Message_string') AS Message_string,
        SUBSTR(CAST(CAST(JSON_VALUE(d.log_text, '$.receivedAt') AS BIGINT) / 1000 AS TIMESTAMP), 1, 23) AS receivedAt,
        HOUR(COALESCE(
            SUBSTR(CAST(CAST(JSON_VALUE(d.log_text, '$.clientSession.startDateTime') AS BIGINT) / 1000 AS TIMESTAMP), 1, 23),
            CAST(REPLACE(SUBSTR(JSON_VALUE(d.log_text, '$.clientCreationDate'), 1, 23), 'T', ' ') AS TIMESTAMP)
        )) AS cs_start_hour,
        ROW_NUMBER() OVER(
            PARTITION BY 
                SUBSTR(JSON_VALUE(d.log_text, '$.clientCreationDate'), 1, 10),
                JSON_VALUE(d.log_text, '$.customerId'),
                JSON_VALUE(d.log_text, '$.eventId')
            ORDER BY 
                CAST(JSON_VALUE(d.log_text, '$.receivedAt') AS BIGINT)
        ) AS rn
    FROM 
        MINIO.vendor3.impala_mobillog.impala_mobil_ekran_logu_stg d
    WHERE 
        JSON_VALUE(d.log_text, '$.customerId') IS NOT NULL
        AND JSON_VALUE(d.log_text, '$._eventDisplayName') IN ('viewStop', 'clientSessionStop', 'inappMessageClose', 'pushNotificationTapped', 'buttonClick');
    """
    
    execute_query(client, create_temp_view_query, headers, 
                 "Geçici view oluşturma", log_success=True)
    
    # Ana yapılandırılmış tabloyu doldur
    insert_structured_query = """
    -- Önce tabloyu temizle
    DELETE FROM MINIO.vendor3.impala_mobillog.mobil_ekran_logu_structured WHERE 1=1;
    
    -- Verileri transforme et ve yükle
    INSERT INTO MINIO.vendor3.impala_mobillog.mobil_ekran_logu_structured
    WITH d_log_trans AS (
        SELECT 
            tarih,
            customerid,
            atr_duration,
            atr_entertime,
            atr_exittime,
            atr_viewclass,
            atr_viewid,
            atr_viewlabel,
            atr_viewdisplayname,
            clientcreationdate,
            cs_duration,
            cs_sessionid,
            cs_startdatetime,
            cs_stopdatetime,
            eventid,
            eventname,
            eventdisplayname,
            dp_platform,
            dp_appversionname,
            atr_classname,
            atr_label,
            atr_componentid,
            CASE 
                WHEN eventdisplayname = 'pushNotificationTapped' THEN dataroidEventKey 
                WHEN eventdisplayname = 'inappMessageClose' THEN inappMessageName 
            END AS message_str,
            receivedat,
            cs_start_hour,
            CASE 
                WHEN eventdisplayname = 'clientSessionStop' THEN cs_startdatetime 
                WHEN eventdisplayname IN ('inappMessageClose', 'pushNotificationTapped') THEN COALESCE(cs_startdatetime, clientcreationdate, receivedat) 
                ELSE atr_entertime 
            END AS enter_time,
            CASE 
                WHEN eventdisplayname = 'clientSessionStop' THEN cs_stopdatetime 
                WHEN eventdisplayname IN ('inappMessageClose', 'pushNotificationTapped') THEN COALESCE(cs_startdatetime, clientcreationdate, receivedat) 
                ELSE atr_exittime 
            END AS exit_time,
            HOUR(CASE 
                WHEN eventdisplayname = 'clientSessionStop' THEN CAST(cs_startdatetime AS TIMESTAMP)
                WHEN eventdisplayname IN ('inappMessageClose', 'pushNotificationTapped') THEN COALESCE(CAST(cs_startdatetime AS TIMESTAMP), clientcreationdate, CAST(receivedat AS TIMESTAMP)) 
                ELSE atr_entertime 
            END) AS enter_time_hour,
            CASE 
                WHEN eventdisplayname = 'clientSessionStop' THEN cs_duration 
                ELSE atr_duration 
            END AS duration
        FROM 
            MINIO.vendor3.impala_mobillog.temptable cs
        WHERE 
            cs.eventdisplayname = 'clientSessionStop' 
            AND rn = 1
            AND NOT EXISTS (
                SELECT 1 
                FROM MINIO.vendor3.impala_mobillog.temptable v 
                WHERE v.eventdisplayname = 'viewStop' 
                AND cs.customerid = v.customerid 
                AND cs.atr_viewid = v.atr_viewid 
                AND cs.cs_sessionid = v.cs_sessionid 
                AND cs.tarih = v.tarih 
                AND v.rn = 1
            )
            
        UNION ALL
        
        SELECT 
            tarih,
            customerid,
            atr_duration,
            atr_entertime,
            atr_exittime,
            atr_viewclass,
            atr_viewid,
            atr_viewlabel,
            atr_viewdisplayname,
            clientcreationdate,
            cs_duration,
            cs_sessionid,
            cs_startdatetime,
            cs_stopdatetime,
            eventid,
            eventname,
            eventdisplayname,
            dp_platform,
            dp_appversionname,
            atr_classname,
            atr_label,
            atr_componentid,
            CASE 
                WHEN eventdisplayname = 'pushNotificationTapped' THEN dataroidEventKey 
                WHEN eventdisplayname = 'inappMessageClose' THEN inappMessageName 
            END AS message_str,
            receivedat,
            cs_start_hour,
            CASE 
                WHEN eventdisplayname IN ('clientSessionStop', 'buttonClick') THEN cs_startdatetime 
                WHEN eventdisplayname IN ('inappMessageClose', 'pushNotificationTapped') THEN COALESCE(clientcreationdate, receivedat) 
                ELSE atr_entertime 
            END AS enter_time,
            CASE 
                WHEN eventdisplayname IN ('clientSessionStop', 'buttonClick') THEN cs_stopdatetime 
                WHEN eventdisplayname IN ('inappMessageClose', 'pushNotificationTapped') THEN COALESCE(clientcreationdate, receivedat) 
                ELSE atr_exittime 
            END AS exit_time,
            HOUR(CASE 
                WHEN eventdisplayname IN ('clientSessionStop', 'buttonClick') THEN CAST(cs_startdatetime AS TIMESTAMP)  
                WHEN eventdisplayname IN ('inappMessageClose', 'pushNotificationTapped') THEN COALESCE(clientcreationdate, CAST(receivedat AS TIMESTAMP)) 
                ELSE atr_entertime 
            END) AS enter_time_hour,
            CASE 
                WHEN eventdisplayname = 'clientSessionStop' THEN cs_duration 
                ELSE atr_duration 
            END AS duration
        FROM 
            MINIO.vendor3.impala_mobillog.temptable d
        WHERE 
            d.eventdisplayname <> 'clientSessionStop'
            AND d.rn = 1
    )
    SELECT 
        customerid,
        atr_duration,
        atr_entertime,
        atr_exittime,
        atr_viewclass,
        atr_viewid,
        atr_viewlabel,
        atr_viewdisplayname,
        clientcreationdate,
        cs_duration,
        cs_sessionid,
        cs_startdatetime,
        cs_stopdatetime,
        eventid,
        eventname,
        eventdisplayname,
        dp_platform,
        dp_appversionname,
        atr_classname,
        atr_label,
        atr_componentid,
        message_str,
        enter_time,
        exit_time,
        COALESCE(duration, 0) AS duration,
        enter_time_hour,
        cs_start_hour,
        tarih AS tarih_str,
        tarih
    FROM 
        d_log_trans;
    """
    
    execute_query(client, insert_structured_query, headers, 
                 "Yapılandırılmış tabloyu doldurma", log_success=True)

def run_workflow(client, headers, run_step=None):
    """Tüm adımları çalıştırır ya da belirtilen adımı çalıştırır."""
    logger.info("Dremio SQL dönüşüm iş akışı başlatılıyor")
    
    # Kaynak tabloları oluştur
    create_source_tables_if_not_exists(client, headers)
    
    # Adım 2: GZIP verilerini STG tablosuna yükle
    if run_step is None or run_step == 2:
        step2_unzip_and_load_stg(client, headers)
    
    # Adım 3: Partitionlı tablo oluştur ve veri aktar
    if run_step is None or run_step == 3:
        step3_create_partitioned_table(client, headers)
    
    # Adım 4: Structured tablo oluştur ve transformasyon yap
    if run_step is None or run_step == 4:
        step4_transform_to_structured(client, headers)
    
    logger.info("Dremio SQL dönüşüm iş akışı tamamlandı")

def main():
    """Ana fonksiyon."""
    # Komut satırı argümanları
    import argparse
    parser = argparse.ArgumentParser(description='Spark SQL\'i Dremio SQL\'e dönüştürme ve uygulama')
    parser.add_argument('--step', type=int, choices=[2, 3, 4], help='Çalıştırılacak adım numarası')
    parser.add_argument('--token', help='Dremio API token (varsa)')
    args = parser.parse_args()
    
    # Token al
    token = args.token if args.token else get_token()
    if not token:
        logger.error("Dremio kimlik doğrulama başarısız!")
        sys.exit(1)
    
    # Client oluştur
    client, headers = setup_client(token=token)
    
    try:
        # İş akışını çalıştır
        run_workflow(client, headers, args.step)
    except KeyboardInterrupt:
        logger.info("Kullanıcı tarafından sonlandırıldı")
    except Exception as e:
        logger.error(f"Uygulama hatası: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()