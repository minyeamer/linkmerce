from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.routers.triggers import router as trigger_router


app = FastAPI(title="LinkMerce Airflow Trigger")
templates = Jinja2Templates(directory=Path(__file__).resolve().parents[1] / "templates")

app.include_router(trigger_router)


def render_trigger_page(
        request: Request,
        dag_id: str,
        dag_name: str | None,
        api_path: str,
) -> HTMLResponse:
    display_name = dag_name.strip() if dag_name and dag_name.strip() else dag_id
    return templates.TemplateResponse(
        request = request,
        name = "trigger.html",
        context = {
            "dag_id": dag_id,
            "display_name": display_name,
            "api_path": api_path,
        },
    )


@app.get("/health")
def health() -> dict:
    return {"ok": True, "service": "airflow-trigger"}


@app.get("/trigger", response_class=HTMLResponse)
def trigger_page(
        request: Request,
        dag_id: str = Query(..., description="실행할 Airflow Dag ID"),
        dag_name: str | None = Query(None, description="화면에 표시할 작업 이름"),
) -> HTMLResponse:
    return render_trigger_page(request, dag_id, dag_name, "/api/trigger")


@app.get("/trigger/gsheets", response_class=HTMLResponse)
def trigger_gsheets_page(
        request: Request,
        task_ids: str = Query(..., description="쉼표로 구분한 gsheets task_id 목록"),
        dag_name: str | None = Query(None, description="화면에 표시할 작업 이름"),
) -> HTMLResponse:
    return render_trigger_page(request, task_ids, dag_name, "/api/trigger/gsheets")


@app.get("/trigger/gsheets/{target}", response_class=HTMLResponse)
def trigger_gsheets_target_page(
        request: Request,
        target: str,
        dag_name: str | None = Query(None, description="화면에 표시할 작업 이름"),
) -> HTMLResponse:
    return render_trigger_page(request, target, dag_name, f"/api/trigger/gsheets/{target}")
