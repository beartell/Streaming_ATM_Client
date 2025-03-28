    def _write_parquet_to_minio(self, df, date, hour, minute):
        """Parquet dosyasını oluştur ve Minio'ya yükle. Tüm alanları string'e dönüştür."""
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
            
            # ÖNEMLİ DEĞİŞİKLİK: Tüm sütunları string'e dönüştür
            for col in df.columns:
                # NaN değerleri boş string ile dolduralım
                df[col] = df[col].fillna('')
                # Tüm değerleri string'e dönüştür
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
                #date_str = date.strftime("%Y-%m-%d")
                #object_name = f"data/date={date_str}/hour={hour:02d}/minute={minute:02d}/{uuid.uuid4()}.parquet"
                object_name = f"data/{uuid.uuid4()}.parquet"

                # Yeniden deneme mekanizması
                retry_count = 0
                while retry_count < MAX_RETRY_ATTEMPTS:
                    try:
                        self.minio_client.fput_object(
                            MINIO_BUCKET,
                            object_name,
                            temp_file_path
                        )
                        logger.info(f"Parquet dosyası yazıldı: {object_name}, {len(df)} satır (tüm alanlar string olarak)")
                        self.copy_parquet_to_dremio(object_name)
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
