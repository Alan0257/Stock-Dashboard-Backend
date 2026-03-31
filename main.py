"""
股票儀表板後端 API v3
- httpx + proxy=None 直連 Yahoo Finance，繞過 Render 系統 proxy
- 路由: GET /health  GET /api/quote/{code}  GET /api/quotes  GET|PUT /api/watchlist  POST /api/watchlist/add  DELETE /api/watchlist/remove
"""

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import asyncio, time, os, json, logging
from typing import Optional
import httpx
import firebase_admin
from firebase_admin import credentials, auth, firestore

logging.basicConfig(level=logging.INFO)

def init_firebase():
    if firebase_admin._apps:
        return
    raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if raw:
        firebase_admin.initialize_app(credentials.Certificate(json.loads(raw)))
    else:
        kp = os.path.join(os.path.dirname(__file__), "serviceAccountKey.json")
        if os.path.exists(kp):
            firebase_admin.initialize_app(credentials.Certificate(kp))

init_firebase()

app = FastAPI(title="Stock Dashboard API", version="3.0.0")
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS,
                   allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

_cache: dict = {}
def cache_get(key, ttl=60):
    i = _cache.get(key)
    return i["data"] if i and (time.time()-i["ts"]) < ttl else None
def cache_set(key, data):
    _cache[key] = {"ts": time.time(), "data": data}

INDEX_MAP = {"TWII":"^TWII","TAIEX":"^TWII","N225":"^N225","KOSPI":"^KS11",
             "HSI":"^HSI","SPX":"^GSPC","DJI":"^DJI","IXIC":"^IXIC","TPEX":"^TPEX"}

def to_yf(code):
    u = code.upper()
    if u in INDEX_MAP: return INDEX_MAP[u]
    if code.replace(".","").isdigit() or (len(code)>=4 and code[:4].isdigit()): return code+".TW"
    return u

_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://finance.yahoo.com/",
}

async def _fetch(code: str) -> dict:
    symbol = to_yf(code)
    cached = cache_get(f"q:{code}")
    if cached: return cached
    transport = httpx.AsyncHTTPTransport(proxy=None)
    for host in ["query1","query2"]:
        url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
        try:
            async with httpx.AsyncClient(transport=transport, timeout=20, follow_redirects=True) as c:
                r = await c.get(url, headers=_HDR)
            logging.info(f"Yahoo {host}/{symbol} => {r.status_code}")
            if r.status_code != 200: continue
            d = r.json()
            res = (d.get("chart") or {}).get("result") or []
            if not res: continue
            q = res[0]["indicators"]["quote"][0]
            def clean(l): return [v for v in l if v is not None]
            closes = clean(q.get("close",[]))
            if not closes: continue
            last, prev = closes[-1], (closes[-2] if len(closes)>=2 else closes[-1])
            chg = last-prev; chgp = chg/prev*100 if prev else 0
            opens=clean(q.get("open",[])); highs=clean(q.get("high",[])); lows=clean(q.get("low",[])); vols=clean(q.get("volume",[]))
            r2 = lambda x: round(x,2)
            result = {"code":code,"symbol":symbol,"price":r2(last),
                      "open":r2(opens[-1] if opens else last),"high":r2(highs[-1] if highs else last),
                      "low":r2(lows[-1] if lows else last),"volume":int(vols[-1] if vols else 0),
                      "change":r2(chg),"change_pct":r2(chgp),"updated_at":datetime.now().isoformat()}
            cache_set(f"q:{code}", result)
            return result
        except Exception as e:
            logging.warning(f"_fetch {host}/{symbol}: {e}")
    raise HTTPException(status_code=404, detail=f"找不到 {code} 的報價")

@app.get("/")
def root(): return {"status":"ok","version":"3.0.0","time":datetime.now().isoformat()}

@app.get("/health")
def health(): return {"status":"healthy"}

@app.get("/api/quote/{code}")
async def get_quote(code: str): return await _fetch(code)

@app.get("/api/quotes")
async def get_quotes(codes: str):
    cl = [c.strip() for c in codes.split(",") if c.strip()]
    if len(cl) > 30: raise HTTPException(400, "最多30支")
    results = await asyncio.gather(*[_fetch(c) for c in cl], return_exceptions=True)
    return {"quotes":[r if not isinstance(r,Exception) else {"code":cl[i],"error":str(r)} for i,r in enumerate(results)],
            "count":len(cl),"updated_at":datetime.now().isoformat()}

async def verify_token(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401,"Missing Authorization")
    try: return auth.verify_id_token(authorization.split(" ",1)[1])
    except Exception as e: raise HTTPException(401, str(e))

DEFAULT_GROUPS = [
    {"name":"台股核心","stocks":["2330","2454","2317","2308","3008"]},
    {"name":"國際指數","stocks":["N225","KOSPI","HSI","SPX"]},
    {"name":"美國科技","stocks":["NVDA","AAPL","MSFT","TSLA"]},
    {"name":"自訂","stocks":["2382","006208"]},
]

@app.get("/api/watchlist")
async def get_watchlist(user: dict = Depends(verify_token)):
    uid = user["uid"]
    try:
        db = firestore.client()
        doc = db.collection("watchlists").document(uid).get()
        if doc.exists: return doc.to_dict()
        default = {"uid":uid,"groups":DEFAULT_GROUPS,"updated_at":datetime.now().isoformat()}
        db.collection("watchlists").document(uid).set(default)
        return default
    except Exception as e: raise HTTPException(500, str(e))

@app.put("/api/watchlist")
async def update_watchlist(body: dict, user: dict = Depends(verify_token)):
    uid = user["uid"]
    try:
        db = firestore.client()
        body["uid"]=uid; body["updated_at"]=datetime.now().isoformat()
        db.collection("watchlists").document(uid).set(body)
        return {"status":"ok"}
    except Exception as e: raise HTTPException(500, str(e))

@app.post("/api/watchlist/add")
async def add_stock(body: dict, user: dict = Depends(verify_token)):
    uid = user["uid"]; code = body.get("code","").strip().upper(); grp = body.get("group","自訂")
    if not code: raise HTTPException(400,"code不能為空")
    try:
        db = firestore.client(); ref = db.collection("watchlists").document(uid)
        doc = ref.get(); data = doc.to_dict() if doc.exists else {"uid":uid,"groups":DEFAULT_GROUPS}
        groups = data.get("groups",[])
        target = next((g for g in groups if g["name"]==grp), None)
        if not target: target={"name":grp,"stocks":[]}; groups.append(target)
        if code not in target["stocks"]: target["stocks"].append(code)
        data["groups"]=groups; data["updated_at"]=datetime.now().isoformat()
        ref.set(data); return {"status":"ok","code":code,"group":grp}
    except Exception as e: raise HTTPException(500, str(e))

@app.delete("/api/watchlist/remove")
async def remove_stock(body: dict, user: dict = Depends(verify_token)):
    uid = user["uid"]; code = body.get("code","").strip().upper()
    try:
        db = firestore.client(); ref = db.collection("watchlists").document(uid)
        doc = ref.get()
        if not doc.exists: return {"status":"ok"}
        data = doc.to_dict()
        for g in data.get("groups",[]): g["stocks"]=[s for s in g["stocks"] if s.upper()!=code]
        data["updated_at"]=datetime.now().isoformat(); ref.set(data)
        return {"status":"ok","removed":code}
    except Exception as e: raise HTTPException(500, str(e))
