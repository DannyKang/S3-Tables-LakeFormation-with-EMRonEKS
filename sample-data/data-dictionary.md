# 서울시 자전거 대여 데이터 사전

## 테이블 정보
- **테이블명**: seoul-bike-rental-data-202506.bike_rental_data
- **형식**: Apache Iceberg (S3 Tables)
- **데이터 기간**: 2025년 6월
- **총 레코드 수**: 150,000건 (샘플: 50건)

## 컬럼 정의

| 컬럼명 | 데이터 타입 | 설명 | 예시 | 민감도 |
|--------|-------------|------|------|--------|
| `rental_id` | STRING | 대여 고유 식별자 | R001, R002 | 일반 |
| `station_id` | STRING | 정거장 고유 식별자 | ST001, ST002 | 일반 |
| `station_name` | STRING | 정거장명 | 강남역 1번출구 | 일반 |
| `district` | STRING | 행정구역 | 강남구, 종로구, 중구 | 일반 |
| `rental_date` | TIMESTAMP | 대여 시작 시간 | 2025-06-01 08:30:00 | 일반 |
| `return_date` | TIMESTAMP | 반납 시간 | 2025-06-01 09:15:00 | 일반 |
| `user_type` | STRING | 사용자 유형 | 정기, 일반, 관광객 | 일반 |
| `age_group` | STRING | 연령대 | 20대, 30대, 40대, 50대+ | 민감 |
| `gender` | STRING | 성별 | M, F | 민감 |
| `rental_duration` | INTEGER | 대여 시간(분) | 45, 30, 60 | 일반 |
| `payment_amount` | DECIMAL | 결제 금액(원) | 1500, 1000, 2000 | 금융정보 |
| `user_id` | STRING | 사용자 고유 식별자 | U001, U002 | 개인정보 |

## 데이터 분포

### 지역별 분포
- **강남구**: 30건 (60%)
- **종로구**: 1건 (2%)
- **중구**: 3건 (6%)
- **기타 구**: 16건 (32%)

### 연령대별 분포
- **20대**: 25건 (50%)
- **30대**: 15건 (30%)
- **40대**: 7건 (14%)
- **50대+**: 3건 (6%)

### 사용자 유형별 분포
- **정기**: 17건 (34%)
- **일반**: 17건 (34%)
- **관광객**: 16건 (32%)

## 역할별 접근 권한

### 🔑 LF_DataStewardRole
- **접근 범위**: 전체 데이터
- **접근 컬럼**: 전체 12개 컬럼
- **필터링**: 없음

### 🔑 LF_GangnamAnalyticsRole
- **접근 범위**: district = '강남구'
- **접근 컬럼**: 11개 (user_id 제외)
- **필터링**: Row-level (강남구만)

### 🔑 LF_OperationRole
- **접근 범위**: 전체 데이터
- **접근 컬럼**: 8개 (운영 관련만)
- **제외 컬럼**: payment_amount, user_id, age_group, gender

### 🔑 LF_MarketingPartnerRole (NEW!)
- **접근 범위**: district = '강남구' AND age_group = '20대'
- **접근 컬럼**: 9개 (마케팅 관련)
- **제외 컬럼**: payment_amount, user_id, rental_duration
- **필터링**: Multi-dimensional (강남구 + 20대)

## 샘플 쿼리

### Data Steward (전체 접근)
```sql
SELECT district, age_group, count(*) as rental_count, 
       sum(payment_amount) as total_revenue
FROM bike_rental_data 
GROUP BY district, age_group
ORDER BY total_revenue DESC;
```

### Gangnam Analytics (강남구만)
```sql
SELECT station_name, user_type, count(*) as rental_count,
       avg(rental_duration) as avg_duration
FROM bike_rental_data 
-- Lake Formation이 자동으로 district = '강남구' 필터 적용
GROUP BY station_name, user_type
ORDER BY rental_count DESC;
```

### Operation (운영 데이터만)
```sql
SELECT district, station_name, count(*) as rental_count,
       avg(rental_duration) as avg_duration
FROM bike_rental_data 
GROUP BY district, station_name
ORDER BY rental_count DESC;
-- payment_amount, user_id 컬럼 접근 불가
```

### Marketing Partner (강남구 20대만)
```sql
SELECT station_name, user_type, gender, count(*) as target_count
FROM bike_rental_data 
-- Lake Formation이 자동으로 district = '강남구' AND age_group = '20대' 필터 적용
GROUP BY station_name, user_type, gender
ORDER BY target_count DESC;
```

## 데이터 생성 규칙

이 샘플 데이터는 다음 규칙으로 생성되었습니다:

1. **지역 분포**: 강남구 중심 (실제 분석 시나리오 반영)
2. **시간 분포**: 2025년 6월 1일-4일 (4일간)
3. **연령 분포**: 20대 중심 (마케팅 시나리오 반영)
4. **결제 금액**: 1,000원-2,000원 (실제 자전거 대여 요금 반영)
5. **대여 시간**: 30-60분 (일반적인 이용 패턴 반영)
