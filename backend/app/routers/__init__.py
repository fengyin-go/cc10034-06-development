"""业务路由汇总：事务包专用路由先注册，通用资源路由兜底动态模块路径。"""
from __future__ import annotations

from app.routers import change_event as change_event_router
from app.routers import resource

ROUTERS = [change_event_router, resource]
