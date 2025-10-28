"""
DART 전체 기업 코드 목록 다운로드
종목코드 기반 정확 매칭으로 고유번호 추가
"""
import os
import requests
import zipfile
import io
import pandas as pd
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

print("=" * 60)
print("DART 전체 기업 코드 다운로드")
print("=" * 60)

# API 키 로드
load_dotenv()
API_KEY = os.getenv('DART_API_KEY')

if not API_KEY:
    print("❌ 오류: .env 파일에서 DART_API_KEY를 찾을 수 없습니다")
    exit(1)

print("✓ API 키 로드 완료")

# DART API에서 전체 기업 코드 다운로드
print("\n📥 DART 서버에서 기업 코드 목록 다운로드 중...")

url = "https://opendart.fss.or.kr/api/corpCode.xml"
params = {'crtfc_key': API_KEY}

try:
    response = requests.get(url, params=params, timeout=60)
    
    if response.status_code != 200:
        print(f"❌ 오류: HTTP {response.status_code}")
        exit(1)
    
    print("✓ 다운로드 완료")
    
    # ZIP 파일 압축 해제
    print("\n📦 ZIP 파일 압축 해제 중...")
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    xml_data = zip_file.read('CORPCODE.xml')
    print("✓ 압축 해제 완료")
    
    # XML 파싱
    print("\n🔍 XML 파싱 중...")
    root = ET.fromstring(xml_data)
    
    # 데이터 추출
    companies = []
    
    for corp in root.findall('list'):
        corp_code = corp.find('corp_code').text
        corp_name = corp.find('corp_name').text
        stock_code = corp.find('stock_code').text
        modify_date = corp.find('modify_date').text
        
        companies.append({
            'corp_code': corp_code,
            'corp_name': corp_name,
            'stock_code': stock_code if stock_code and stock_code.strip() else '',
            'modify_date': modify_date
        })
    
    print(f"✓ 총 {len(companies):,}개 기업 정보 추출")
    
    # DataFrame 생성
    df = pd.DataFrame(companies)
    
    # 1. 전체 기업 목록 저장
    all_file = 'data/dart_all_companies.csv'
    df.to_csv(all_file, index=False, encoding='utf-8-sig')
    print(f"\n✓ 전체 목록 저장: {all_file}")
    print(f"  - 총 {len(df):,}개 기업")
    
    # 2. 상장사만 필터링 (종목코드가 있는 기업)
    listed_df = df[df['stock_code'] != ''].copy()
    listed_file = 'data/dart_listed_companies.csv'
    listed_df.to_csv(listed_file, index=False, encoding='utf-8-sig')
    print(f"\n✓ 상장사 목록 저장: {listed_file}")
    print(f"  - 총 {len(listed_df):,}개 기업")
    
    # 3. 통계 출력
    print("\n" + "=" * 60)
    print("📊 통계")
    print("=" * 60)
    print(f"전체 기업:    {len(df):,}개")
    print(f"상장 기업:    {len(listed_df):,}개")
    print(f"비상장 기업:  {len(df) - len(listed_df):,}개")
    
    # 4. 기존 companies.csv와 매칭 (종목코드 기반)
    print("\n" + "=" * 60)
    print("🔍 기존 companies.csv 분석 (종목코드 기반 매칭)")
    print("=" * 60)
    
    try:
        old_df = pd.read_csv('data/companies.csv', dtype={'corp_code': str})  # 문자열로 읽기
        print(f"기존 파일: {len(old_df)}개 기업")
        
        # corp_code 컬럼 확인
        if 'corp_code' in old_df.columns:
            stock_col = 'corp_code'  # 실제로는 종목코드
            print("  ℹ️ 'corp_code' 컬럼을 종목코드로 사용")
        elif 'stock_code' in old_df.columns:
            stock_col = 'stock_code'
            print("  ℹ️ 'stock_code' 컬럼 사용")
        else:
            print("  ❌ 종목코드 컬럼을 찾을 수 없습니다")
            exit(1)
        
        # 종목코드 6자리로 제로 패딩
        old_df[stock_col] = old_df[stock_col].astype(str).str.strip().str.zfill(6)
        print("  ✓ 종목코드를 6자리로 변환 완료")
        
        # 종목코드로 정확 매칭
        print("\n🔗 종목코드 기반 매칭 중...")
        matched_companies = []
        not_matched = []
        
        for idx, row in old_df.iterrows():
            corp_name = row['corp_name']
            stock_code = str(row[stock_col]).strip()
            
            # DART 목록에서 종목코드로 검색 (정확히 일치)
            found = df[df['stock_code'] == stock_code]
            
            if len(found) > 0:
                dart_info = found.iloc[0]
                matched_companies.append({
                    'corp_name': dart_info['corp_name'],
                    'corp_code': dart_info['corp_code'],
                    'stock_code': dart_info['stock_code']
                })
                
                # 회사명이 다른 경우 알림
                if corp_name != dart_info['corp_name']:
                    print(f"  ℹ️ 회사명 차이: '{corp_name}' → '{dart_info['corp_name']}'")
            else:
                not_matched.append({
                    'corp_name': corp_name,
                    'stock_code': stock_code
                })
                print(f"  ✗ 미발견: {corp_name} (종목: {stock_code})")
        
        print(f"\n✓ 매칭 성공: {len(matched_companies)}개")
        print(f"✗ 매칭 실패: {len(not_matched)}개")
        
        # 실패한 기업 상세 분석
        if not_matched:
            print(f"\n" + "=" * 60)
            print("❌ 매칭 실패한 기업 분석")
            print("=" * 60)
            
            for item in not_matched:
                name = item['corp_name']
                stock = item['stock_code']
                
                # 회사명으로라도 찾아보기
                name_search = df[df['corp_name'].str.contains(name, na=False, regex=False)]
                
                print(f"\n{name} (종목: {stock})")
                
                if len(name_search) > 0:
                    print(f"  💡 이름으로는 발견됨:")
                    for _, found_corp in name_search.head(3).iterrows():
                        print(f"     - {found_corp['corp_name']} (종목: {found_corp['stock_code']}, 고유: {found_corp['corp_code']})")
                else:
                    print(f"  ⚠️ DART 목록에 없음 (상장폐지 가능성)")
        
        # 6. 매칭된 데이터로 새 CSV 생성
        print("\n" + "=" * 60)
        print("💾 새 companies_fixed.csv 생성")
        print("=" * 60)
        
        if matched_companies:
            fixed_df = pd.DataFrame(matched_companies)
            fixed_file = 'data/companies_fixed.csv'
            fixed_df.to_csv(fixed_file, index=False, encoding='utf-8-sig')
            
            print(f"✓ 새 파일 저장: {fixed_file}")
            print(f"  - 총 {len(fixed_df)}개 기업 (고유번호 포함)")
            
            # 샘플 출력
            print(f"\n📋 샘플 (처음 10개):")
            print(fixed_df.head(10).to_string(index=False))
        else:
            print("❌ 매칭된 기업이 없습니다")
        
    except FileNotFoundError:
        print("⚠️ 기존 companies.csv 파일이 없습니다")
    
    print("\n" + "=" * 60)
    print("✅ 완료!")
    print("=" * 60)
    print("\n생성된 파일:")
    print("  1. data/dart_all_companies.csv      - 전체 기업 목록")
    print("  2. data/dart_listed_companies.csv   - 상장사만")
    print("  3. data/companies_fixed.csv         - 매칭된 기업 + 고유번호")
    print("\n💡 Tip:")
    print("  - companies_fixed.csv를 data_collector.py에서 사용하세요")
    print("  - 매칭 실패한 기업은 종목코드를 확인하거나 상장폐지 여부를 체크하세요")
    
except Exception as e:
    print(f"\n❌ 오류 발생: {e}")
    import traceback
    traceback.print_exc()