from fastapi import APIRouter, HTTPException
from supabase import create_client
from dotenv import load_dotenv
import os

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_KEY")
)

router = APIRouter()


@router.get("/kpis")
def get_kpis():
    try:
        response = (
            supabase
            .table("kpi_values")
            .select("*")
            .execute()
        )

        return {
            "success": True,
            "count": len(response.data),
            "data": response.data
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.get("/kpis/{sku_id}")
def get_kpi_by_sku(sku_id: str):
    try:
        response = (
            supabase
            .table("kpi_values")
            .select("*")
            .eq("sku_id", sku_id)
            .execute()
        )

        return {
            "success": True,
            "data": response.data
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )