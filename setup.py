#!/usr/bin/env python3
"""
qzcli 安装脚本
"""

from setuptools import find_packages, setup

setup(
    name="qzcli",
    version="0.4.14",
    description="启智平台任务管理 CLI 工具",
    author="openveo3",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "requests>=2.28",
        "rich>=13.0",
        "prompt_toolkit>=3.0",
        "mcp>=1.0,<2.0",
        "PySocks>=1.7",
    ],
    extras_require={
        # 可视化看板（qzcli dashboard）依赖，不进核心 install_requires
        "dashboard": [
            "streamlit>=1.30",
            "plotly>=5.0",
            "pandas>=2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "qzcli=qzcli.cli:main",
            "qzcli-mcp=qzcli.mcp_server:main",
        ],
    },
)
