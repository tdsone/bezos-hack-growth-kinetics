import modal

image = (
    modal.Image.debian_slim().pip_install("fastapi[standard]", "orjson")
)

app = modal.App("paper2dataset")

MINUTES = 1

@app.function(image=image)
@modal.concurrent(max_inputs=100)
@modal.asgi_app()
def fastapi_app():
    """ASGI FastAPI application with CORS enabled."""
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.middleware.gzip import GZipMiddleware
    from fastapi.responses import ORJSONResponse
    from pydantic import BaseModel
    from typing import List

    app = FastAPI(title="paper2dataset", default_response_class=ORJSONResponse)

    # --- CORS CONFIGURATION ---
    allowed_origins = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://bezos-hack-growth-kinetics.vercel.app"
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.add_middleware(GZipMiddleware, minimum_size=1024)

    class GenerateRequest(BaseModel):
        dois: List[str]

    class FilterResult(BaseModel):
        doi: str
        hasGrowthRate: bool

    class FilterResponse(BaseModel):
        results: List[FilterResult]

    @app.post("/filter", response_model=FilterResponse)
    async def filter(req: GenerateRequest):
        def mock_has_growth_rate(doi: str) -> bool:
            total = 0
            for ch in doi:
                total += ord(ch)
            return abs(total) % 2 == 0

        results: List[FilterResult] = [
            FilterResult(doi=doi, hasGrowthRate=mock_has_growth_rate(doi))
            for doi in req.dois
        ]
        return FilterResponse(results=results)

    return app