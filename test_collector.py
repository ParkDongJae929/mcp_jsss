"""
DART 수집기 테스트 - 문제 진단용
"""
import os
import sys
from pathlib import Path

print("=" * 60)
print("DART 수집기 진단 시작")
print("=" * 60)

# 1. Python 버전 확인
print(f"\n1. Python 버전: {sys.version}")

# 2. 현재 작업 디렉토리
print(f"\n2. 현재 디렉토리: {os.getcwd()}")

# 3. 필요한 파일 존재 확인
files_to_check = [
    '.env',
    'data/companies.csv',
    'data/progress.json',
]

print(f"\n3. 필수 파일 확인:")
for file in files_to_check:
    exists = Path(file).exists()
    status = "✓" if exists else "✗"
    print(f"   {status} {file}")

# 4. 라이브러리 임포트 테스트
print(f"\n4. 라이브러리 임포트 테스트:")

try:
    import pandas as pd
    print("   ✓ pandas")
except ImportError as e:
    print(f"   ✗ pandas - {e}")

try:
    import dart_fss as dart
    print("   ✓ dart-fss")
except ImportError as e:
    print(f"   ✗ dart-fss - {e}")

try:
    from dotenv import load_dotenv
    print("   ✓ python-dotenv")
except ImportError as e:
    print(f"   ✗ python-dotenv - {e}")

try:
    from tqdm import tqdm
    print("   ✓ tqdm")
except ImportError as e:
    print(f"   ✗ tqdm - {e}")

# 5. .env 파일 로드 테스트
print(f"\n5. API 키 로드 테스트:")
try:
    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv('DART_API_KEY')
    if api_key:
        print(f"   ✓ API 키 로드 성공 (길이: {len(api_key)}자)")
        print(f"   키 앞 10자: {api_key[:10]}...")
    else:
        print("   ✗ API 키를 찾을 수 없습니다")
        print("   .env 파일에 'DART_API_KEY=...' 형식으로 작성되어 있는지 확인하세요")
except Exception as e:
    print(f"   ✗ 오류: {e}")

# 6. companies.csv 읽기 테스트
print(f"\n6. companies.csv 읽기 테스트:")
try:
    import pandas as pd
    df = pd.read_csv('data/companies.csv')
    print(f"   ✓ 파일 읽기 성공")
    print(f"   총 {len(df)}개 기업")
    print(f"   컬럼: {list(df.columns)}")
    print(f"\n   첫 3개 기업:")
    for idx, row in df.head(3).iterrows():
        print(f"     - {row['corp_name']} ({row['corp_code']})")
except Exception as e:
    print(f"   ✗ 오류: {e}")

# 7. DART API 연결 테스트
print(f"\n7. DART API 연결 테스트:")
try:
    import dart_fss as dart
    from dotenv import load_dotenv
    
    load_dotenv()
    api_key = os.getenv('DART_API_KEY')
    dart.set_api_key(api_key)
    
    print("   API 키 설정 완료, 테스트 검색 시도...")
    
    # 삼성전자로 테스트
    corp_list = dart.corp.find_by_corp_name('삼성전자')
    
    if corp_list:
        corp = corp_list[0]
        print(f"   ✓ API 연결 성공!")
        print(f"   테스트 결과: {corp.corp_name} ({corp.corp_code})")
    else:
        print("   ✗ API 응답이 비어있습니다")
        
except Exception as e:
    print(f"   ✗ API 연결 실패: {e}")
    print(f"   오류 타입: {type(e).__name__}")

print("\n" + "=" * 60)
print("진단 완료!")
print("=" * 60)
print("\n위 결과를 확인하고 ✗ 표시된 항목을 해결하세요.")