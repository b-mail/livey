"""
라이브 데모 수정 에이전트 — 오케스트레이터

수정 요청 1건 → A·B 두 레인 병렬 실행 → 프리뷰 URL 2개 반환.

동작 모드
- mock (기본): variants/의 변형 HTML을 서빙하고 단계 진행을 시뮬레이션한다.
- daytona: DAYTONA_API_KEY 환경변수가 있으면 진짜 샌드박스를 띄운다.

실행: uvicorn orchestrator:app --port 8787
"""
import asyncio
import os
import re
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

BASE = Path(__file__).resolve().parent
USE_DAYTONA = bool(os.getenv("DAYTONA_API_KEY"))

app = FastAPI(title="Livey — 라이브 데모 수정 에이전트")
app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")

# ── 레인 정의 ────────────────────────────────────────────────────────────
# delays: mock 모드에서 각 단계(생성→기동→서버→준비)의 소요 초 시뮬레이션.
STAGES = ["변형 생성", "샌드박스 기동", "서버 실행", "준비 완료"]

LANES = {
    "a": {
        "label": "A안",
        "approach": "보수적",
        "prompt_style": "요청을 최소 변경으로 반영한다. 기존 구조를 유지한다.",
        "delays": [1.4, 1.9, 1.2, 0.5],
    },
    "b": {
        "label": "B안",
        "approach": "과감형",
        "prompt_style": "요청 의도를 확장 해석해 정보를 더한다.",
        "delays": [1.8, 2.6, 1.7, 0.9],
    },
}

# 채팅 전송 순서대로 쓰는 mock 변형. 1번째 요청 = 차트, 2번째 요청 = 필터.
ROUNDS = {
    1: {
        "done": (
            "파트너별 정산액을 차트로 보는 두 가지 안이 준비되었습니다. "
            "<b>A안은 파이 차트</b>, <b>B안은 도넛 차트</b>입니다. "
            "상단의 <b>[원본 · A안 · B안]</b>으로 전환해 비교하세요."
        ),
        "lanes": {
            "a": {
                "summary": "파트너별 정산액을 파이 차트로 보여줍니다.",
                "explanation": (
                    "요청을 비중 비교로 해석해, 파트너별 정산액을 파이 차트로 그렸습니다. "
                    "조각이 중심까지 이어져 누가 큰지 한눈에 보이고, "
                    "옆에 금액·비중 범례와 합계를 두었습니다. "
                    "기존 정산 표와 레이아웃은 그대로 두었습니다."
                ),
            },
            "b": {
                "summary": "파트너별 정산액을 도넛 차트로 보여줍니다.",
                "explanation": (
                    "같은 비중을 도넛으로 그렸습니다. "
                    "가운데에 총 정산액과 파트너 수를 넣어 전체 규모를 먼저 보고, "
                    "조각으로 구성을 확인하도록 했습니다. "
                    "범례·합계와 기존 표는 유지했습니다."
                ),
            },
        },
    },
    2: {
        "done": (
            "정산 내역을 거르는 두 가지 안이 준비되었습니다. "
            "<b>A안은 파트너 칩 필터</b>, <b>B안은 툴바 필터 + 합계 행</b>입니다. "
            "상단의 <b>[원본 · A안 · B안]</b>으로 전환해 비교하세요."
        ),
        "lanes": {
            "a": {
                "summary": "정산 내역을 파트너 칩으로 거를 수 있게 했습니다.",
                "explanation": (
                    "표 위에 파트너 칩을 두어, 탭하듯 한 곳만 골라 볼 수 있습니다. "
                    "기존 표 구조와 열은 그대로이고, 선택한 파트너의 거래만 남습니다. "
                    "한 파트너를 빠르게 확인하는 용도에 맞습니다."
                ),
            },
            "b": {
                "summary": "표 위 툴바로 거르고, 아래에 합계 행을 넣었습니다.",
                "explanation": (
                    "파트너와 기간을 고르는 툴바를 표 위에 두고, "
                    "걸러진 결과의 거래액·수수료·정산액 합계를 맨 아래 행에 표시합니다. "
                    "여러 조건으로 좁히면서 합계까지 같이 보는 정산 마감 용도입니다."
                ),
            },
        },
    },
}

# ── 상태 (메모리 only) ───────────────────────────────────────────────────
STATE = {
    "running": False,
    "request": None,
    "started_at": None,
    "round": 0,          # 채팅 전송 횟수. 1 → 요청1, 2 → 요청2
    "lanes": {},
    "demo_html": None,   # 이번 페이지 세션의 업로드 HTML. 새로고침(GET /) 시 지운다
    "demo_name": None,
}


def current_round():
    return max(1, min(STATE.get("round") or 1, max(ROUNDS)))


def reset_lanes():
    extra = ROUNDS[current_round()]["lanes"]
    STATE["lanes"] = {
        lane_id: {
            "id": lane_id,
            "label": cfg["label"],
            "approach": cfg["approach"],
            "summary": extra[lane_id]["summary"],
            "explanation": extra[lane_id]["explanation"],
            "stage": -1,          # -1: 대기, 0..3: STAGES 인덱스
            "url": None,
            "error": None,
            "elapsed": None,
        }
        for lane_id, cfg in LANES.items()
    }


def reset_session():
    """콘솔을 새로고침하면 업로드 데모와 요청 순서를 처음부터 다시 시작한다."""
    STATE.update(running=False, request=None, started_at=None, round=0,
                 demo_html=None, demo_name=None)
    reset_lanes()


reset_lanes()


# ── 변형 생성 ────────────────────────────────────────────────────────────
async def generate_variant(lane_id: str, request_text: str) -> str:
    rnd = current_round()
    return (BASE / "variants" / str(rnd) / f"{lane_id}.html").read_text(encoding="utf-8")


# ── 샌드박스 배포 ────────────────────────────────────────────────────────
async def deploy_mock(lane_id: str, html: str) -> str:
    """mock: 로컬 프리뷰 URL."""
    return f"http://localhost:8787/preview/{current_round()}/{lane_id}"


async def deploy_daytona(lane_id: str, html: str) -> str:
    """파일 하나를 올리고 정적 서버를 띄운 뒤 프리뷰 URL을 얻는다."""
    from daytona import Daytona  # pip install daytona

    def _run() -> str:
        daytona = Daytona()  # DAYTONA_API_KEY 환경변수 사용
        sandbox = daytona.create()
        sandbox.fs.upload_file(html.encode("utf-8"), "site/index.html")
        sandbox.process.execute_command(
            "nohup python3 -m http.server 8000 --directory site >/dev/null 2>&1 &"
        )
        return sandbox.get_preview_link(8000).url

    return await asyncio.to_thread(_run)


# ── 레인 실행: 생성 → 기동 → 서버 → 준비 ────────────────────────────────
async def run_lane(lane_id: str, request_text: str):
    lane = STATE["lanes"][lane_id]
    cfg = LANES[lane_id]
    t0 = time.monotonic()
    try:
        # 0) 변형 생성
        lane["stage"] = 0
        html = await generate_variant(lane_id, request_text)
        if not USE_DAYTONA:
            await asyncio.sleep(cfg["delays"][0])

        # 1) 샌드박스 기동  2) 서버 실행
        lane["stage"] = 1
        if USE_DAYTONA:
            lane["stage"] = 2
            url = await deploy_daytona(lane_id, html)
        else:
            await asyncio.sleep(cfg["delays"][1])
            lane["stage"] = 2
            await asyncio.sleep(cfg["delays"][2])
            url = await deploy_mock(lane_id, html)

        # 3) 준비 완료
        await asyncio.sleep(0 if USE_DAYTONA else cfg["delays"][3])
        lane["url"] = url
        lane["stage"] = 3
        lane["elapsed"] = round(time.monotonic() - t0, 1)
    except Exception as e:  # 한 레인이 실패해도 다른 레인은 계속 진행한다
        lane["error"] = str(e)
        lane["elapsed"] = round(time.monotonic() - t0, 1)


async def run_all(request_text: str):
    await asyncio.gather(*(run_lane(lid, request_text) for lid in LANES))
    STATE["running"] = False


# ── API ──────────────────────────────────────────────────────────────────
class RunRequest(BaseModel):
    request: str


class DemoUpload(BaseModel):
    html: str
    name: str | None = None


def start_on_overview(html: str) -> str:
    """원본 데모는 대시보드(overview)에서 연다. 정산 내역 시작값을 한 곳만 바꾼다."""
    return re.sub(
        r"""(let\s+current\s*=\s*)(['"])settlements\2""",
        r"\1\2overview\2",
        html,
        count=1,
    )


@app.post("/api/demo")
async def api_demo(body: DemoUpload):
    """드래그 앤 드롭으로 받은 단일 HTML 데모를 등록한다."""
    STATE["demo_html"] = start_on_overview(body.html)
    STATE["demo_name"] = body.name
    return {"ok": True, "name": body.name}


@app.post("/api/run")
async def api_run(body: RunRequest):
    if STATE["running"]:
        raise HTTPException(409, "이미 실행 중입니다.")
    STATE["round"] = min((STATE.get("round") or 0) + 1, max(ROUNDS))
    reset_lanes()
    STATE.update(running=True, request=body.request, started_at=time.time())
    asyncio.get_event_loop().create_task(run_all(body.request))
    return {"ok": True, "mode": "daytona" if USE_DAYTONA else "mock"}


@app.get("/api/state")
async def api_state():
    return {
        "running": STATE["running"],
        "request": STATE["request"],
        "stages": STAGES,
        "mode": "daytona" if USE_DAYTONA else "mock",
        "lanes": list(STATE["lanes"].values()),
        "round": STATE["round"],
        "done": ROUNDS[current_round()]["done"] if STATE["round"] else None,
        "demo": {"loaded": STATE["demo_html"] is not None, "name": STATE["demo_name"]},
    }


# ── 정적 서빙: 콘솔 / 원본 데모 앱 / (mock) 프리뷰 ───────────────────────
@app.get("/")
async def console():
    reset_session()
    return FileResponse(BASE / "static" / "index.html")


@app.get("/demo")
async def demo():
    # 드래그 앤 드롭으로 업로드된 데모만 서빙한다
    if STATE["demo_html"] is None:
        raise HTTPException(404, "업로드된 데모가 없습니다.")
    return HTMLResponse(start_on_overview(STATE["demo_html"]))


@app.get("/preview/{round_id}/{lane_id}")
async def preview(round_id: int, lane_id: str):
    f = BASE / "variants" / str(round_id) / f"{lane_id}.html"
    if lane_id not in LANES or round_id not in ROUNDS or not f.exists():
        raise HTTPException(404)
    return FileResponse(f)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8787)
