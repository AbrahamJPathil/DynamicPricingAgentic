from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes.kpi import router as kpi_router

app = FastAPI(
    title="Dynamic Pricing API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    kpi_router,
    prefix="/api",
    tags=["KPIs"]
)

@app.get("/")
def root():
    return {
        "message": "Dynamic Pricing API is running",
        "docs": "/docs"
    }