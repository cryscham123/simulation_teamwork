## 프로젝트 구조
### 해당 branch에서의 구조.



simpy/
├── data/                        # Data Schema
│
├── simulation/                  # 기존 simpy_core : simulaton 핵심 모듈
│   ├── __init__.py
│   ├── machine.py               # Machine 클래스 : PM 속성 추가
│   ├── scheduler.py             # Scheduler 클래스
│   ├── job.py                   # Job 클래스
│   └── data_loader.py           # 데이터 로딩 유틸리티
│
├── algorithms/
│   ├── __init__.py
│   ├── base.py                  # Algorithm 추상 클래스
│   │
│   ├── stage1/                  # PM/고장 X
│   │   ├── __init__.py
│   │   ├── rule_based.py        # Rule
│   │   └── ga.py                # 추후 구현 예정
│   │
│   └── stage2/                  # PM/고장 O
│       ├── __init__.py
│       ├── rule_based_pm.py     # Rule
│       └── ga_integrated.py     # 추후 구현 예정
│
├── requirements.txt
└── README.md
