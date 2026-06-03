# -*- coding: utf-8 -*-
"""
业务脚本根包：按 AData 官方「数据字典」模块划分子目录。

在线总览：https://adata.30006124.xyz/

使用前请安装本仓库中的 adata 库，例如在仓库根目录执行：

    pip install -e ./adata

之后在业务代码中统一使用 ``import adata``，并按子包对应的命名空间调用
（例如行情用 ``adata.stock.market``，与文档中的 ``stock.market`` 一致）。

相关目录：``server/``（平台 API）、``integrations/``（企微等）、``strategies/``（回测与验证）。
"""
