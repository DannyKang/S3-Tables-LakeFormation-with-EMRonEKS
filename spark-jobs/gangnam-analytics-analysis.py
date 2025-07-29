#!/usr/bin/env python3
"""
Gangnam Analytics Analysis - 강남구 데이터 분석
Lake Formation FGAC 데모용 - 강남구 데이터만 접근 (~3,000건)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """S3 Tables(Iceberg) 지원 Spark 세션 생성"""
    import os
    table_bucket = os.getenv('TABLE_BUCKET_NAME', 'seoul-bike-demo-2025')
    
    return SparkSession.builder \
        .appName("GangnamAnalytics-RegionalAccess-Analysis") \
        .config("spark.sql.catalog.s3tables_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tables_catalog.catalog-impl", "org.apache.iceberg.aws.s3.S3TablesCatalog") \
        .config("spark.sql.catalog.s3tables_catalog.warehouse", f"s3://{table_bucket}/") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def main():
    print("=== Gangnam Analytics Analysis 시작 ===")
    print("역할: 강남구 분석가 - 강남구 데이터만 접근")
    
    spark = create_spark_session()
    
    try:
        # S3 Tables에서 데이터 읽기
        print("\n1. S3 Tables에서 데이터 로드 중...")
        spark_conf = spark.sparkContext.getConf()
        namespace = spark_conf.get('spark.app.s3tables.namespace', 'bike_db')
        table_name = spark_conf.get('spark.app.s3tables.table', 'bike_rental_data')
        
        print(f"   네임스페이스: {namespace}")
        print(f"   테이블명: {table_name}")
        
        df = spark.read.table(f"s3tables_catalog.{namespace}.{table_name}")
        
        total_records = df.count()
        print(f"✅ Lake Formation 필터링 후 레코드 수: {total_records:,}건")
        print("🔒 접근 제한: 강남구 데이터만 (개인정보 birth_year 제외)")
        
        # 데이터 스키마 확인 (접근 가능한 컬럼만)
        print("\n2. 접근 가능한 데이터 스키마:")
        df.printSchema()
        
        # 강남구 데이터 확인
        print(f"\n3. 지역 분포 확인:")
        district_check = df.groupBy("district").count().orderBy(desc("count"))
        district_check.show()
        
        # 정거장별 이용 현황
        print(f"\n4. 강남구 정거장별 이용 현황 (상위 10개):")
        station_stats = df.groupBy("station_name", "station_id") \
                          .agg(count("*").alias("이용횟수"),
                               avg("usage_min").alias("평균_이용시간"),
                               avg("distance_meter").alias("평균_이동거리")) \
                          .orderBy(desc("이용횟수")) \
                          .limit(10)
        
        station_stats.show(truncate=False)
        
        # 성별 분포 (접근 가능)
        print(f"\n5. 성별 이용 분포:")
        gender_stats = df.groupBy("gender") \
                         .agg(count("*").alias("이용횟수"),
                              avg("usage_min").alias("평균_이용시간"),
                              avg("distance_meter").alias("평균_이동거리")) \
                         .orderBy(desc("이용횟수"))
        
        gender_stats.show()
        
        # 사용자 유형별 분포
        print(f"\n6. 사용자 유형별 분포:")
        user_type_stats = df.groupBy("user_type") \
                            .agg(count("*").alias("이용횟수"),
                                 avg("usage_min").alias("평균_이용시간")) \
                            .orderBy(desc("이용횟수"))
        
        user_type_stats.show()
        
        # 이용 시간대별 패턴
        print(f"\n7. 시간대별 이용 패턴:")
        hourly_pattern = df.withColumn("hour", hour("rental_date")) \
                           .groupBy("hour") \
                           .count() \
                           .orderBy("hour")
        
        hourly_pattern.show(24)
        
        # 이용 시간 분석
        print(f"\n8. 이용 시간 통계:")
        usage_stats = df.select(
            avg("usage_min").alias("평균_이용시간"),
            min("usage_min").alias("최소_이용시간"),
            max("usage_min").alias("최대_이용시간"),
            expr("percentile_approx(usage_min, 0.5)").alias("중앙값_이용시간")
        )
        
        usage_stats.show()
        
        # 이동 거리 분석
        print(f"\n9. 이동 거리 통계:")
        distance_stats = df.select(
            avg("distance_meter").alias("평균_이동거리"),
            min("distance_meter").alias("최소_이동거리"),
            max("distance_meter").alias("최대_이동거리"),
            expr("percentile_approx(distance_meter, 0.5)").alias("중앙값_이동거리")
        )
        
        distance_stats.show()
        
        # 요일별 이용 패턴
        print(f"\n10. 요일별 이용 패턴:")
        weekday_pattern = df.withColumn("weekday", date_format("rental_date", "EEEE")) \
                            .groupBy("weekday") \
                            .agg(count("*").alias("이용횟수"),
                                 avg("usage_min").alias("평균_이용시간")) \
                            .orderBy(desc("이용횟수"))
        
        weekday_pattern.show()
        
        print(f"\n=== Gangnam Analytics Analysis 완료 ===")
        print(f"✅ 분석 완료: {total_records:,}건 (강남구만)")
        print(f"🔒 권한: 강남구 데이터만 접근 (birth_year 컬럼 제외)")
        print(f"📊 역할: 강남구 지역 특화 분석 및 서비스 기획")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
