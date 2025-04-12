#!/usr/bin/env python
# coding: utf-8

# # Scraping Ulasan Gojek dari Google Play Store
# Script ini mengumpulkan ulasan aplikasi Gojek dari Google Play Store menggunakan library google-play-scraper

# Import library yang dibutuhkan
import pandas as pd
import numpy as np
import time
from google_play_scraper import Sort, reviews
import json
import datetime
import os

# Fungsi untuk scraping ulasan Gojek
def scrape_gojek_reviews(count=100, lang='id', country='id', sort=Sort.NEWEST):
    """
    Melakukan scraping ulasan Gojek dari Google Play Store
    
    Parameters:
    - count: Jumlah ulasan yang akan diambil
    - lang: Bahasa ulasan
    - country: Negara
    - sort: Urutan ulasan (NEWEST, RATING, RELEVANCE)
    
    Returns:
    - DataFrame berisi ulasan
    """
    print(f"Memulai scraping {count} ulasan Gojek...")
    
    # Package ID untuk aplikasi Gojek di Google Play Store
    app_id = 'com.gojek.app'
    
    # Inisialisasi list untuk menyimpan hasil
    all_reviews = []
    
    # Variabel untuk pagination
    continuation_token = None
    
    # Loop untuk mengambil ulasan dengan pagination
    while len(all_reviews) < count:
        # Tentukan batch size per request
        batch_size = min(100, count - len(all_reviews))
        
        try:
            # Scraping ulasan
            result, continuation_token = reviews(
                app_id,
                lang=lang,
                country=country,
                sort=sort,
                count=batch_size,
                continuation_token=continuation_token
            )
            
            # Tambahkan hasil ke list
            all_reviews.extend(result)
            
            # Tampilkan progress
            print(f"Berhasil mengambil {len(all_reviews)} dari {count} ulasan...")
            
            # Cek apakah masih ada ulasan yang bisa diambil
            if continuation_token is None:
                print("Tidak ada ulasan lagi yang bisa diambil.")
                break
            
            # Delay untuk menghindari rate limiting
            time.sleep(2)
            
        except Exception as e:
            print(f"Error saat scraping: {e}")
            break
    
    # Konversi ke DataFrame
    df = pd.DataFrame(all_reviews)
    
    # Rename kolom agar sesuai analisis (hanya jika kolom tersebut ada)
    column_mapping = {
        'content': 'content',
        'score': 'score',
        'reviewId': 'reviewId',
        'userName': 'userName',
        'thumbsUpCount': 'thumbsUpCount',
        'reviewCreatedVersion': 'appVersion',
        'at': 'reviewDate'
    }
    
    # Hanya rename kolom yang ada di DataFrame
    valid_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=valid_columns)
    
    # Konversi timestamp ke string format jika kolom ada
    if 'reviewDate' in df.columns:
        df['reviewDate'] = pd.to_datetime(df['reviewDate']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"Scraping selesai. Total {len(df)} ulasan berhasil diambil.")
    return df

# Fungsi utama
def main():
    review_count = 5000
    
    try:
        df_reviews = scrape_gojek_reviews(count=review_count)
        
        # Pastikan tidak ada kolom duplikat dengan cara sederhana
        cols = list(df_reviews.columns)
        if len(cols) != len(set(cols)):
            print("Mendeteksi kolom duplikat, memperbaiki nama kolom...")
            new_cols = []
            seen = {}
            for i, col in enumerate(cols):
                if col in seen:
                    seen[col] += 1
                    new_cols.append(f"{col}_{seen[col]}")
                else:
                    seen[col] = 0
                    new_cols.append(col)
            df_reviews.columns = new_cols
        
        # Reset index untuk keamanan
        df_reviews = df_reviews.reset_index(drop=True)
        
        print("\nInformasi Dataset:")
        print(df_reviews.info())
        print("\nContoh data:")
        print(df_reviews.head())
        
        print("\nDistribusi skor:")
        print(df_reviews['score'].value_counts().sort_index())
        
        # Simpan ke file dengan metode paling dasar
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        
        # Simpan CSV dengan cara manual
        csv_filename = f'gojek_reviews_{review_count}_{timestamp}.csv'
        try:
            # Mencoba metode simpan dasar
            with open(csv_filename, 'w', encoding='utf-8') as f:
                # Tulis header
                f.write(','.join(df_reviews.columns) + '\n')
                
                # Tulis data baris per baris
                for _, row in df_reviews.iterrows():
                    values = [str(val).replace(',', ';').replace('\n', ' ').replace('\r', ' ') 
                              if val is not None else '' for val in row]
                    f.write(','.join(values) + '\n')
            
            print(f"\nData berhasil disimpan ke {csv_filename}")
        except Exception as e:
            print(f"Gagal menyimpan CSV secara manual: {e}")
        
        # Simpan JSON dengan cara manual
        json_filename = f'gojek_reviews_{review_count}_{timestamp}.json'
        try:
            # Konversi ke list of dicts dan simpan manual
            records = df_reviews.to_dict('records')
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            
            print(f"Data berhasil disimpan ke {json_filename}")
        except Exception as e:
            print(f"Gagal menyimpan JSON secara manual: {e}")
            
            # Coba alternatif penyimpanan JSON baris per baris
            try:
                with open(json_filename, 'w', encoding='utf-8') as f:
                    for record in records:
                        f.write(json.dumps(record, ensure_ascii=False) + '\n')
                print(f"Data berhasil disimpan ke {json_filename} dengan format JSONL")
            except Exception as e2:
                print(f"Semua metode penyimpanan JSON gagal: {e2}")
        
    except Exception as e:
        print(f"Error dalam proses utama: {e}")


# Jalankan jika file ini dieksekusi langsung
if __name__ == "__main__":
    main()