"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

domain 路由提供序号8需要的关联结果/检查点快照专用接口，必须先于通用
资源路由注册，否则 /api/{module}/{entry_id} 会先匹配掉固定子路径。
"""
from __future__ import annotations

from app.routers import domain, resource

ROUTERS = [domain, resource]
