#!/usr/bin/env python3
"""Turtle Investment Framework - PDF Preprocessor Configuration.

Contains configuration constants and settings for PDF preprocessing.
"""

from typing import Dict, List, Tuple

# Encoding constants
DEFAULT_ENCODING = 'utf-8'

# Garbled text detection thresholds
GARBLED_THRESHOLD = 0.30  # 30% or more garbled characters
GARBLED_PAGE_RATIO = 0.30  # 30% or more garbled pages

# Extraction defaults
DEFAULT_BUFFER_PAGES = 1
DEFAULT_MAX_CHARS = 4000

# Section keywords for extraction
SECTION_KEYWORDS: Dict[str, Dict[str, List[str]]] = {
    # 第一部分：分析前置准备与财报真实性校验 对应模块
    "AUDIT": {
        "include": ["审计报告", "审计意见", "标准无保留意见", "关键审计事项", "保留意见", "否定意见", "无法表示意见", "带强调事项段", "会计师事务所", "签字注册会计师"],
        "exclude": ["主营业务", "经营情况", "行业分析", "未来规划", "财务报表项目注释"],
        "mandatory": ["审计意见", "关键审计事项", "会计师事务所"]
    },
    "BIZ_OVERVIEW": {
        "include": ["公司业务概要", "主营业务与产品", "主要产品及用途", "经营模式", "行业地位", "核心竞争力", "行业发展阶段", "主要业务", "公司简介", "业务概要", "商业模式", "盈利模式"],
        "exclude": ["财务报表附注", "营业收入明细", "营业成本构成", "产能产量", "在建工程", "行业发展趋势", "风险因素"],
        "mandatory": ["主营业务", "经营模式", "核心竞争力", "行业地位"]
    },
    "GLOSSARY": {
        "include": ["释义", "名词释义", "术语释义", "专用术语", "缩略语", "主要名词释义", "专业术语"],
        "exclude": ["主营业务", "经营情况", "财务数据", "行业分析", "风险因素"],
        "mandatory": ["释义", "名词释义", "术语释义"]
    },
    "IMPORTANT": {
        "include": ["重要事项", "会计政策变更", "会计估计变更", "重大关联交易", "重大担保", "重大诉讼", "重大仲裁", "承诺事项", "合规处罚", "行政处罚", "监管函", "整改情况", "重大资产处置", "股权激励"],
        "exclude": ["主营业务明细", "营业收入", "营业成本", "产能产量", "行业分析", "财务报表项目注释"],
        "mandatory": ["会计政策变更", "重大关联交易", "重大诉讼", "行政处罚", "承诺事项"]
    },
    # 第二部分：核心财务三维度深度分析 对应模块
    "BIZ": {
        "include": ["营业收入分产品", "主营业务分产品", "营业收入分行业", "主营业务分行业", "营业收入分地区", "主营业务分地区", "营业成本构成", "分产品毛利率", "主营业务收入构成", "主营业务成本", "收入成本分析", "分部报告", "经营分部", "营业收入构成", "营业成本分析", "毛利率变动"],
        "exclude": ["产能", "产量", "销量", "在建工程", "行业分析", "风险因素", "未来规划", "客户集中度", "供应商集中度"],
        "mandatory": ["分产品营收", "分地区营收", "营业成本", "毛利率", "主营业务收入构成"]
    },
    "CAP": {
        "include": ["产能产量", "产能利用率", "产销率", "产量销量", "主要产品产量", "生产能力", "产能情况", "生产量销售量", "产能建设", "新增产能", "产能投放", "开工率"],
        "exclude": ["主营业务收入", "分产品营收", "在建工程预算", "行业分析", "客户", "供应商", "营业收入", "营业成本"],
        "mandatory": ["产能利用率", "产销率", "产量", "销量", "产能情况"]
    },
    "CUS": {
        "include": ["前五名客户", "前五大客户", "主要客户", "客户集中度", "前五名供应商", "前五大供应商", "主要供应商", "供应商集中度", "客户销售额", "供应商采购额", "前五大客户销售额占比", "前五大供应商采购额占比"],
        "exclude": ["主营业务", "产能", "在建工程", "行业分析", "风险因素", "营业收入明细", "营业成本构成"],
        "mandatory": ["前五名客户", "前五名供应商", "客户集中度", "供应商集中度"]
    },
    "INV": {
        "include": ["存货分类", "存货明细", "存货构成", "存货库龄", "存货跌价准备", "原材料库存", "库存商品", "在产品", "发出商品", "存货减值", "存货周转率", "存货跌价计提比例"],
        "exclude": ["主营业务", "营收", "产能", "客户", "行业分析", "应收账款", "固定资产"],
        "mandatory": ["存货分类", "存货跌价准备", "存货库龄", "存货构成"]
    },
    "AR": {
        "include": ["应收账款账龄", "应收账款按账龄", "应收账款坏账", "应收账款计提", "应收票据账龄", "应收票据坏账", "其他应收款账龄", "应收款项账龄", "坏账准备", "应收账款余额", "应收账款明细", "坏账计提比例", "应收票据明细"],
        "exclude": ["存货", "主营业务", "产能", "在建工程", "行业分析", "递延所得税", "预付账款"],
        "mandatory": ["应收账款账龄", "坏账准备", "应收票据", "应收账款余额"]
    },
    "DEBT": {
        "include": ["短期借款明细", "长期借款明细", "借款利率", "借款到期", "有息负债", "银行借款", "信用借款", "抵押借款", "保证借款", "借款担保", "应付债券", "一年内到期的非流动负债", "融资成本", "负债到期期限"],
        "exclude": ["营收", "成本", "存货", "应收", "产能", "行业分析", "主营业务", "应付账款"],
        "mandatory": ["有息负债", "借款利率", "到期期限", "短期借款", "长期借款"]
    },
    "CIP": {
        "include": ["在建工程明细", "在建工程项目", "在建工程预算", "工程进度", "在建工程转固", "固定资产明细", "固定资产构成", "固定资产增减", "资本开支", "投资项目", "固定资产折旧", "累计折旧", "固定资产减值准备"],
        "exclude": ["主营业务", "营收", "产能产量", "客户", "行业分析", "借款", "应收账款"],
        "mandatory": ["在建工程项目", "工程进度", "固定资产明细", "资本开支"]
    },
    "P2": {
        "include": ["所有权或使用权受限资产", "受限资产", "使用受限的资产", "所有权受限", "使用权受到限制", "受限的货币资金", "受到限制的资产", "所有權或使用權受限資產", "受限資產", "资产抵押", "资产质押"],
        "exclude": ["主营业务", "营收", "成本", "行业分析", "经营情况", "未来规划"],
        "mandatory": ["受限资产", "所有权或使用权受限资产", "受限的货币资金"]
    },
    "P4": {
        "include": ["关联方交易", "关联交易", "关联方及关联交易", "关联方关系及其交易", "重大关联交易", "关联方资金往来", "关联担保", "关联方资金占用", "关联方往来", "關聯方交易", "關聯交易", "关联方清单"],
        "exclude": ["主营业务", "营收", "成本", "行业分析", "经营情况", "未来规划"],
        "mandatory": ["关联方交易", "关联交易", "关联担保", "关联方资金往来"]
    },
    "P6": {
        "include": ["或有负债", "或有事项", "未决诉讼", "重大诉讼", "对外担保", "承诺及或有事项", "承诺和或有负债", "资本承诺", "或有負債", "或有事項", "担保情况"],
        "exclude": ["主营业务", "营收", "成本", "行业分析", "经营情况", "未来规划"],
        "mandatory": ["或有事项", "未决诉讼", "对外担保", "承诺事项"]
    },
    "P13": {
        "include": ["非经常性损益项目及金额", "非经常性损益合计", "非经常性损益", "非经常性损益明细", "非经常性损益项目", "扣除非经常性损益", "非經常性損益", "非经常性损益对净利润的影响"],
        "exclude": ["主营业务", "营收", "成本", "行业分析", "经营情况", "未来规划"],
        "mandatory": ["非经常性损益", "扣除非经常性损益", "非经常性损益明细"]
    },
    # 第三部分：分行业差异化分析+管理层讨论与分析 对应模块
    "MDA_INDUSTRY": {
        "include": ["行业分析", "行业现状", "行业发展趋势", "行业竞争格局", "市场环境", "宏观经济", "行业周期", "供需格局", "行业政策", "行业集中度", "行业壁垒"],
        "exclude": ["主营业务明细", "财务报表项目注释", "产能", "在建工程", "风险因素", "未来规划", "经营回顾"],
        "mandatory": ["行业分析", "行业竞争格局", "行业发展趋势", "行业周期"]
    },
    "MDA_OPERATION": {
        "include": ["经营情况分析", "经营回顾", "经营成果", "主营业务分析", "收入分析", "成本分析", "利润分析", "财务状况分析", "营收变动原因", "成本变动原因", "利润变动原因", "期间费用分析"],
        "exclude": ["行业分析", "未来规划", "风险因素", "产能建设", "在建工程", "财务报表项目注释"],
        "mandatory": ["经营情况分析", "主营业务分析", "收入成本分析", "经营成果"]
    },
    "MDA_OUTLOOK": {
        "include": ["未来发展规划", "发展战略", "发展展望", "经营计划", "投资计划", "产能规划", "发展战略", "未来经营目标", "项目建设计划", "市场拓展计划"],
        "exclude": ["历史经营数据", "行业分析", "财务报表", "风险因素", "主营业务明细", "财务报表项目注释"],
        "mandatory": ["未来发展规划", "经营计划", "发展战略"]
    },
    "MDA_RISK": {
        "include": ["风险因素", "风险提示", "可能面对的风险", "风险与对策", "主要风险", "经营风险", "财务风险", "政策风险", "行业风险", "市场风险", "宏观经济风险"],
        "exclude": ["主营业务", "营收", "成本", "产能", "未来规划", "行业分析", "经营回顾"],
        "mandatory": ["风险因素", "经营风险", "财务风险", "政策风险"]
    },
    "SUB": {
        "include": ["主要控股参股公司分析", "主要子公司及对公司净利润的影响", "主要控股参股公司情况", "控股子公司情况", "在子公司中的权益", "在其他主体中的权益", "纳入合并范围的主体", "子公司经营情况", "子公司营收", "子公司净利润"],
        "exclude": ["母公司财务明细", "行业分析", "主营业务", "风险因素", "未来规划"],
        "mandatory": ["主要控股参股公司", "子公司经营情况", "在子公司中的权益"]
    },
    "INDUSTRY_EXTRA": {
        "include": ["环保投入", "安全生产", "节能减排", "环保设施", "排污许可", "安全投入", "环保支出", "绿色生产", "研发投入", "研发费用", "专利数量", "行业专项指标", "特许经营权", "门店数量", "用户数量", "单店盈利", "土储情况"],
        "exclude": ["主营业务", "财务明细", "行业分析", "经营回顾", "风险因素", "未来规划"],
        "mandatory": ["环保投入", "安全生产", "研发投入", "行业专项数据"]
    }
}

# Section extraction configuration
SECTION_EXTRACT_CONFIG: Dict[str, Dict[str, int]] = {
    "AUDIT": {"buffer_pages": 3, "max_chars": 8000},
    "BIZ_OVERVIEW": {"buffer_pages": 3, "max_chars": 10000},
    "GLOSSARY": {"buffer_pages": 2, "max_chars": 5000},
    "IMPORTANT": {"buffer_pages": 4, "max_chars": 12000},
    "BIZ": {"buffer_pages": 3, "max_chars": 8000},
    "CAP": {"buffer_pages": 2, "max_chars": 6000},
    "CUS": {"buffer_pages": 2, "max_chars": 5000},
    "INV": {"buffer_pages": 2, "max_chars": 6000},
    "AR": {"buffer_pages": 2, "max_chars": 6000},
    "DEBT": {"buffer_pages": 2, "max_chars": 6000},
    "CIP": {"buffer_pages": 3, "max_chars": 8000},
    "MDA_INDUSTRY": {"buffer_pages": 3, "max_chars": 10000},
    "MDA_OPERATION": {"buffer_pages": 3, "max_chars": 10000},
    "MDA_OUTLOOK": {"buffer_pages": 2, "max_chars": 6000},
    "MDA_RISK": {"buffer_pages": 2, "max_chars": 6000},
    "SUB": {"buffer_pages": 3, "max_chars": 8000},
    "INDUSTRY_EXTRA": {"buffer_pages": 2, "max_chars": 5000},
}

# Zone markers for section identification
ZONE_MARKERS: List[Tuple[str, str]] = [
    (r"第[一二三四五六七八九十百]+节\s*重要提示", "INTRO_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*公司简介", "INTRO_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*管理层讨论与分析", "MDA_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*经营情况讨论与分析", "MDA_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*公司治理", "GOVERNANCE_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*财务报告", "FIN_ZONE"),
    (r"第[一二三四五六七八九十百]+节\s*会计数据", "FIN_ZONE"),
    (r"[四五六]\s*[、.．]\s*重要会计政策", "POLICY_ZONE"),
    (r"七\s*[、.．]\s*合并财务报表项目注释", "NOTES_ZONE"),
    (r"[一二三四五六七八九十]+[、.．]\s*补充资料", "SUPPLEMENT_ZONE"),
    (r"[一二三四五六七八九十百]+[、.．\ss]*重要提示", "INTRO_ZONE"),
    (r"[一二三四五六七八九十百]+[、.．\s\s]*公司简介", "INTRO_ZONE"),
    (r"[一二三四五六七八九十百]+[、.．\s\s]*管理层讨论与分析", "MDA_ZONE"),
    (r"[一二三四五六七八九十百]+[、.．\s\s]*经营情况讨论与分析", "MDA_ZONE"),
    (r"[一二三四五六七八九十百]+[、.．\s\s]*财务报告", "FIN_ZONE"),
]

# Section title binding for precise extraction
SECTION_TITLE_BINDING: Dict[str, List[str]] = {
    "MDA_INDUSTRY": ["行业发展概况", "行业分析", "行业格局", "市场环境"],
    "MDA_OPERATION": ["主营业务分析", "经营情况讨论与分析", "经营成果分析"],
    "MDA_OUTLOOK": ["未来发展展望", "经营计划", "发展战略", "未来规划"],
    "MDA_RISK": ["风险因素", "风险提示", "可能面对的风险"],
}

# Section zone preferences
SECTION_ZONE_PREFERENCES: Dict[str, Dict[str, List[str]]] = {
    "AUDIT": {"prefer": ["FIN_ZONE"], "avoid": ["MDA_ZONE"]},
    "BIZ_OVERVIEW": {"prefer": ["MDA_ZONE", "INTRO_ZONE"], "avoid": ["NOTES_ZONE"]},
    "GLOSSARY": {"prefer": ["INTRO_ZONE"], "avoid": ["NOTES_ZONE", "FIN_ZONE"]},
    "IMPORTANT": {"prefer": ["MDA_ZONE", "NOTES_ZONE"], "avoid": []},
    "BIZ": {"prefer": ["NOTES_ZONE", "MDA_ZONE"], "avoid": ["POLICY_ZONE"]},
    "CAP": {"prefer": ["MDA_ZONE", "NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "CUS": {"prefer": ["NOTES_ZONE", "MDA_ZONE"], "avoid": ["POLICY_ZONE"]},
    "INV": {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "AR":  {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "DEBT": {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "CIP": {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P2":  {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P4":  {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P6":  {"prefer": ["NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "P13": {"prefer": ["SUPPLEMENT_ZONE", "NOTES_ZONE"], "avoid": ["POLICY_ZONE"]},
    "MDA_INDUSTRY": {"prefer": ["MDA_ZONE"], "avoid": []},
    "MDA_OPERATION": {"prefer": ["MDA_ZONE"], "avoid": []},
    "MDA_OUTLOOK": {"prefer": ["MDA_ZONE"], "avoid": []},
    "MDA_RISK": {"prefer": ["MDA_ZONE"], "avoid": []},
    "SUB": {"prefer": ["NOTES_ZONE", "MDA_ZONE"], "avoid": ["POLICY_ZONE"]},
    "INDUSTRY_EXTRA": {"prefer": ["MDA_ZONE", "NOTES_ZONE"], "avoid": []},
}

# Section order for output
SECTION_ORDER = [
    "AUDIT",
    "BIZ_OVERVIEW",
    "GLOSSARY",
    "IMPORTANT",
    "BIZ",
    "CAP",
    "CUS",
    "INV",
    "AR",
    "DEBT",
    "CIP",
    "P2",
    "P4",
    "P6",
    "P13",
    "MDA_INDUSTRY",
    "MDA_OPERATION",
    "MDA_OUTLOOK",
    "MDA_RISK",
    "SUB",
    "INDUSTRY_EXTRA",
]

# Module extraction priority
EXTRACT_PRIORITY = [
    "AUDIT", "BIZ_OVERVIEW", "BIZ", "CAP", "CUS", "INV", "AR", "DEBT", "CIP",
    "MDA_INDUSTRY", "MDA_OPERATION", "MDA_OUTLOOK", "MDA_RISK",
    "P2", "P4", "P6", "P13", "SUB", "IMPORTANT", "GLOSSARY", "INDUSTRY_EXTRA"
]

# TOC hints mapping
TOC_HINTS = {
    "AUDIT": ["审计报告"],
    "BIZ_OVERVIEW": ["公司业务概要", "公司简介"],
    "GLOSSARY": ["释义"],
    "IMPORTANT": ["重要事项"],
    "BIZ": ["经营情况讨论与分析", "主营业务分析"],
    "CAP": ["经营情况讨论与分析", "产能分析"],
    "CUS": ["经营情况讨论与分析", "客户供应商分析"],
    "INV": ["合并财务报表项目注释", "存货"],
    "AR": ["合并财务报表项目注释", "应收账款"],
    "DEBT": ["合并财务报表项目注释", "借款"],
    "CIP": ["合并财务报表项目注释", "在建工程"],
    "P2": ["合并财务报表项目注释", "受限资产"],
    "P4": ["关联方及关联交易"],
    "P6": ["或有事项"],
    "P13": ["非经常性损益"],
    "MDA_INDUSTRY": ["管理层讨论与分析", "行业分析"],
    "MDA_OPERATION": ["管理层讨论与分析", "经营分析"],
    "MDA_OUTLOOK": ["管理层讨论与分析", "发展规划"],
    "MDA_RISK": ["管理层讨论与分析", "风险因素"],
    "SUB": ["在子公司中的权益"],
    "INDUSTRY_EXTRA": ["社会责任报告", "环保信息"],
}

# AR section exclusion terms
AR_EXCLUDE_TERMS = ["递延所得税", "预付款项", "预付账款", "其他应收"]

# Directory names for output
OUTPUT_DIR_NAME = "pdf_sections"
JSON_FILENAME = "pdf_sections.json"
MD_FILENAME = "pdf_sections.md"