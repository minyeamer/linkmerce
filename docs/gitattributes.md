# Git 줄바꿈(Line Ending) 관리 가이드

이 문서는 윈도우(CRLF)와 맥(LF) 환경에서 협업할 때 발생하는 줄바꿈 경고 및 파일 변환 문제를 해결하는 절차를 정리합니다. 프로젝트의 원본 소스코드는 맥/리눅스 표준인 **LF**로 유지하는 것을 원칙으로 합니다.

## 1. 프로젝트 설정 (.gitattributes)
프로젝트 루트 폴더에 `.gitattributes` 파일을 생성하고 아래 내용을 추가합니다. 이 설정은 Git이 텍스트 파일을 자동으로 감지하여 원본을 LF로 관리하도록 강제합니다.

```bash
# 모든 텍스트 파일은 원본을 LF로 유지하고, 작업환경에 따라 자동 변환
* text=auto eol=lf
```

## 2. OS별 Git 전역 설정
각 환경의 터미널에서 아래 명령어를 실행하여 Git이 줄바꿈을 처리하는 방식을 설정합니다.

- **윈도우(Windows):** 작업 시 CRLF로 보여주고, 커밋 시 LF로 변환
  ```cmd
  git config --global core.autocrlf true
  ```
- **맥(macOS/Linux):** 작업 및 커밋 시 모두 LF 유지
  ```bash
  git config --global core.autocrlf input
  ```

## 3. 기존 파일 갱신 절차 (주의 필요)
이미 프로젝트에 `CRLF`로 등록된 파일들을 `LF`로 한꺼번에 변경하고 싶을 때 실행합니다. **주의: 이 작업은 모든 파일이 변경사항(Modified)으로 표시될 수 있습니다.**

1. **인덱스 초기화**: 현재 Git이 추적 중인 캐시를 비웁니다. (실제 파일 삭제 안 됨)
   ```cmd
   git rm --cached -r .
   ```
2. **인덱스 재구성**: `.gitattributes` 규칙에 따라 파일을 다시 읽어들입니다.
   ```cmd
   git add .
   ```
3. **변경사항 확인**: `git status`로 변경 내용을 확인합니다. 원치 않는 파일이 대량으로 포함되었다면 `git reset`으로 되돌릴 수 있습니다.

## 4. 문제 해결 (Troubleshooting)
- **Warning: LF will be replaced by CRLF...**: Git 설정과 실제 파일의 줄바꿈이 다를 때 발생합니다. 위 설정을 완료하면 더 이상 나타나지 않습니다.
- **모든 파일이 Modified로 뜰 때**: `git reset`을 실행하여 스테이징을 해제하고 필요한 파일만 다시 `git add` 하세요.
