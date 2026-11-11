from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI

from yuxi.bootstrap.api import close_nebula_connection, shutdown_component, startup
from yuxi.infrastructure.postgres.manager import pg_manager
from yuxi.infrastructure.redis.queue import close_queue_clients


@asynccontextmanager
async def lifespan(app: FastAPI):
    """确保 startup 任意阶段失败时仍执行已取得资源的补偿清理。"""

    app.state.startup_complete = False
    app.state.startup_components = {}
    try:
        await startup(app)
        async with AsyncExitStack() as stack:
            knowledge_mcp = getattr(app.state, "knowledge_mcp", None)
            if knowledge_mcp is not None:
                await stack.enter_async_context(knowledge_mcp.manager.run())
            yield
    finally:
        app.state.startup_complete = False
        await shutdown_component("queue_clients", close_queue_clients)
        await shutdown_component("nebula", close_nebula_connection)
        await shutdown_component("postgres", pg_manager.close)
