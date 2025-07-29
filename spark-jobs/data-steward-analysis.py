#!/usr/bin/env python3
"""
Data Steward Analysis - 전체 데이터 분석
Lake Formation FGAC 데모용 - 전체 100,000건 데이터 접근
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """S3 Tables Lake Formation FGAC 지원 Spark 세션 생성"""
    return SparkSession.builder \
        .appName("DataSteward-FullAccess-Analysis") \
        .getOrCreate()

def main():
    print("=== Data Steward Analysis 시작 ===")
    print("역할: 데이터 관리자 - 전체 데이터 접근 권한")
    
    spark = create_spark_session()
    
    try:
        # S3 Tables Lake Formation FGAC를 통해 데이터 읽기
        print("\n1. S3 Tables Lake Formation FGAC를 통해 데이터 로드 중...")
        namespace = "bike_db"
        table_name = "bike_rental_data"
        
        print(f"   네임스페이스: {namespace}")
        print(f"   테이블명: {table_name}")
        print(f"   카탈로그: {namespace}.{table_name} (Glue Catalog with Iceberg JAR)")
        
        # S3 Tables 카탈로그를 통해 데이터 읽기 (네이티브 S3 Tables 지원)
        df = spark.read.table(f"{namespace}.{table_name}")
        
        total_records = df.count()
        print(f"✅ 총 레코드 수: {total_records:,}건")
        
        # 데이터 스키마 확인
        print("\n2. 데이터 스키마:")
        df.printSchema()
        
        # 기본 통계
        print(f"\n3. 기본 통계 정보:")
        print(f"   • 고유 대여 ID: {df.select('rental_id').distinct().count():,}")
        print(f"   • 고유 정거장: {df.select('station_id').distinct().count():,}")
        print(f"   • 고유 구: {df.select('district').distinct().count()}")
        
        # 구별 분포 (상위 10개)
        print(f"\n4. 구별 분포 (상위 10개):")
        district_stats = df.groupBy("district") \
                           .agg(count("*").alias("count"),
                                avg("usage_min").alias("avg_usage"),
                                avg("distance_meter").alias("avg_distance")) \
                           .orderBy(desc("count")) \
                           .limit(10)
        
        district_stats.show(truncate=False)
        
        # 성별 분포
        print(f"\n5. 성별 분포:")
        gender_stats = df.groupBy("gender") \
                         .agg(count("*").alias("count"),
                              avg("usage_min").alias("avg_usage")) \
                         .orderBy(desc("count"))
        
        gender_stats.show()
        
        # 연령대별 분포 (개인정보 접근 가능)
        print(f"\n6. 연령대별 분포:")
        age_group_df = df.withColumn("age_group", 
                                   when(col("birth_year") >= 2005, "10대")
                                   .when(col("birth_year") >= 1995, "20대")
                                   .when(col("birth_year") >= 1985, "30대")
                                   .when(col("birth_year") >= 1975, "40대")
                                   .when(col("birth_year") >= 1965, "50대")
                                   .otherwise("60대+"))
        
        age_stats = age_group_df.groupBy("age_group") \
                               .agg(count("*").alias("count"),
                                    avg("usage_min").alias("avg_usage"),
                                    avg("distance_meter").alias("avg_distance")) \
                               .orderBy("age_group")
        
        age_stats.show()
        
        # 대여 시간 통계
        print(f"\n7. 대여 시간 통계:")
        usage_stats = df.select(
            avg("usage_min").alias("평균_대여시간"),
            min("usage_min").alias("최소_대여시간"),
            max("usage_min").alias("최대_대여시간"),
            expr("percentile_approx(usage_min, 0.5)").alias("중앙값_대여시간")
        )
        
        usage_stats.show()
        
        # 이동 거리 통계
        print(f"\n8. 이동 거리 통계:")
        distance_stats = df.select(
            avg("distance_meter").alias("평균_이동거리"),
            min("distance_meter").alias("최소_이동거리"),
            max("distance_meter").alias("최대_이동거리"),
            expr("percentile_approx(distance_meter, 0.5)").alias("중앙값_이동거리")
        )
        
        distance_stats.show()
        
        # 시간대별 이용 패턴
        print(f"\n9. 시간대별 이용 패턴:")
        hourly_pattern = df.withColumn("hour", hour("rental_date")) \
                           .groupBy("hour") \
                           .count() \
                           .orderBy("hour")
        
        hourly_pattern.show(24)
        
        # 데이터 품질 검증
        print(f"\n10. 데이터 품질 검증:")
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        print("컬럼별 NULL 값 개수:")
        null_counts.show()
        
        # 이상치 탐지
        print(f"\n11. 이상치 탐지:")
        outliers = df.filter(
            (col("usage_min") > 480) |  # 8시간 이상
            (col("distance_meter") > 30000) |  # 30km 이상
            (col("usage_min") < 0) |  # 음수 시간
            (col("distance_meter") < 0)  # 음수 거리
        )
        
        outlier_count = outliers.count()
        print(f"이상치 개수: {outlier_count:,}건 ({(outlier_count/total_records)*100:.2f}%)")
        
        if outlier_count > 0:
            print("이상치 샘플:")
            outliers.select("rental_id", "usage_min", "distance_meter", "district").show(5)
        
        print(f"\n=== Data Steward Analysis 완료 ===")
        print(f"✅ 분석 완료: {total_records:,}건")
        print(f"🔑 권한: 전체 데이터 접근 (개인정보 포함)")
        print(f"📊 역할: 데이터 품질 관리 및 거버넌스")
        print(f"🗂️ 카탈로그: S3 Tables (s3tablesbucket)")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
