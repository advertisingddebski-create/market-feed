#!/usr/bin/env python3
"""Pobiera świece OHLC kilku instrumentów z Yahoo Finance i zapisuje je jako
zwarte pliki JSON dla artefaktu „Pulpit sesji NY” (wykresy na telefonie).

Uruchamia go workflow `.github/workflows/market-feed.yml` (co 5 minut w oknie
sesji NY), a wynik trafia na osobną gałąź `market-feed` repozytorium, którą
artefakt czyta konektorem GitHub. Tylko biblioteka standardowa Pythona.

Użycie:
    python3 fetch_feed.py --out KATALOG            # pobranie z Yahoo
    python3 fetch_feed.py --out KATALOG --sample   # dane syntetyczne (test)
    python3 fetch_feed.py --selftest               # testy transformacji

Układ wyjścia (KATALOG = korzeń gałęzi `market-feed`):
    feed/index.json                 — lista instrumentów, ostatnie ceny, czasy
    feed/<ID>/intraday.json         — tf 1m (ostatnie 36 h) i 5m (7 dni)
    feed/<ID>/history.json          — tf 60m (90 dni) i 1d (3 lata)

Świeca: [t, o, h, l, c, v] — t = początek świecy, sekundy UTC (epoch);
ceny zaokrąglone do `dec` miejsc instrumentu; v = 0 gdy brak wolumenu.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# Instrumenty: id używane przez artefakt, symbol Yahoo, zapasowy symbol,
# liczba miejsc po przecinku, rodzaj (do opisu w artefakcie).
BUILTIN_INSTRUMENTS = [
    {"id": "SPX", "name": "S&P 500", "yahoo": "^GSPC", "fallback": None, "dec": 2, "kind": "indeks (sesja NY 9:30–16:00 ET)"},
    {"id": "ES", "name": "S&P 500 futures (ES)", "yahoo": "ES=F", "fallback": None, "dec": 2, "kind": "kontrakt (prawie 24 h)"},
    {"id": "XAUUSD", "name": "Złoto (GC futures)", "yahoo": "GC=F", "fallback": "XAUUSD=X", "dec": 2, "kind": "kontrakt COMEX GC (Yahoo, ok. 10 min opóźnienia)"},
    {"id": "XAGUSD", "name": "Srebro (SI futures)", "yahoo": "SI=F", "fallback": "XAGUSD=X", "dec": 3, "kind": "kontrakt COMEX SI (Yahoo, ok. 10 min opóźnienia)"},
    {"id": "USDPLN", "name": "USDPLN", "yahoo": "USDPLN=X", "fallback": None, "dec": 4, "kind": "forex"},
    {"id": "EURPLN", "name": "EURPLN", "yahoo": "EURPLN=X", "fallback": None, "dec": 4, "kind": "forex"},
    {"id": "DXY", "name": "Indeks dolara DXY", "yahoo": "DX-Y.NYB", "fallback": None, "dec": 3, "kind": "indeks ICE"},
]

INSTRUMENTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "instruments.json")
ID_RE = __import__("re").compile(r"^[A-Z0-9]{1,16}$")


def load_instruments(path: str = INSTRUMENTS_FILE) -> list[dict]:
    """Lista instrumentów z instruments.json; bez pliku — lista wbudowana.
    Sprawdza pola, unikalność id i format id (klucz rysunków w artefakcie)."""
    if not os.path.exists(path):
        log(f"brak {path} — lista wbudowana ({len(BUILTIN_INSTRUMENTS)})")
        return BUILTIN_INSTRUMENTS
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    items = data.get("instruments") if isinstance(data, dict) else data
    if not isinstance(items, list) or not items:
        raise SystemExit(f"{path}: brak listy 'instruments'")
    seen: set[str] = set()
    out = []
    for i, inst in enumerate(items):
        for k in ("id", "name", "yahoo", "dec"):
            if k not in inst:
                raise SystemExit(f"{path}: instrument #{i} bez pola '{k}'")
        if not ID_RE.match(str(inst["id"])):
            raise SystemExit(f"{path}: id '{inst['id']}' — dozwolone A-Z, 0-9, do 16 znaków")
        if inst["id"] in seen:
            raise SystemExit(f"{path}: powtórzone id '{inst['id']}'")
        seen.add(inst["id"])
        out.append({
            "id": inst["id"], "name": str(inst["name"]), "short": str(inst.get("short") or inst["id"]),
            "yahoo": str(inst["yahoo"]), "fallback": inst.get("fallback") or None, "dec": int(inst["dec"]),
            "group": str(inst.get("group") or "inne"), "kind": str(inst.get("kind") or ""),
            "sample_base": float(inst.get("sample_base") or 100.0),
        })
    return out


INSTRUMENTS = BUILTIN_INSTRUMENTS  # podmieniane w main() przez load_instruments()

# (interwał Yahoo, ile sekund wstecz, do którego pliku, nazwa tf w artefakcie)
REQUESTS = [
    ("1m", 36 * 3600, "intraday", "1m"),
    ("5m", 7 * 86400, "intraday", "5m"),
    ("60m", 90 * 86400, "history", "60m"),
    ("1d", 3 * 365 * 86400, "history", "1d"),
]

HOSTS = ["https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com"]
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json,text/plain,*/*", "Accept-Language": "en-US,en;q=0.9"}
TIMEOUT_S = 20
GAP_S = 0.4          # przerwa między zapytaniami (Yahoo tnie bursty)
MAX_ATTEMPTS = 4     # na jeden (symbol, interwał)


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


class YahooSession:
    """Jedna sesja HTTP z ciasteczkiem i crumbem Yahoo.

    Yahoo zwraca 429/401 dla „gołych” zapytań z adresów centrów danych; z ciasteczkiem
    (fc.yahoo.com) i crumbem (/v1/test/getcrumb) przepuszcza. Gdy dostępny jest
    `curl_cffi`, używa go z odciskiem TLS Chrome; inaczej urllib + cookiejar."""

    def __init__(self) -> None:
        self.crumb: str | None = None
        self._lock = __import__("threading").Lock()
        self._last = 0.0
        self.impl = "urllib"
        try:
            from curl_cffi import requests as cffi_requests  # type: ignore

            self._sess = cffi_requests.Session(impersonate="chrome")
            self.impl = "curl_cffi"
        except Exception:  # noqa: BLE001 — brak pakietu to normalna sytuacja
            import http.cookiejar

            self._jar = http.cookiejar.CookieJar()
            self._opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self._jar))

    def _pace(self) -> None:
        with self._lock:
            wait = self._last + GAP_S - time.time()
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()

    def get(self, url: str) -> tuple[int, str]:
        """GET → (status, body). Nie rzuca na kody HTTP."""
        self._pace()
        if self.impl == "curl_cffi":
            r = self._sess.get(url, headers=HEADERS, timeout=TIMEOUT_S, allow_redirects=True)
            return r.status_code, r.text
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with self._opener.open(req, timeout=TIMEOUT_S) as resp:
                return resp.status, resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace") if e.fp else ""

    def bootstrap(self) -> None:
        """Ciasteczko + crumb. Błąd tu nie jest fatalny — zapytania pójdą bez crumba."""
        try:
            self.get("https://fc.yahoo.com/")  # 404, ale ustawia ciasteczko A3
            st, body = self.get(HOSTS[0] + "/v1/test/getcrumb")
            if st == 200 and body and "<" not in body and len(body) < 64:
                self.crumb = body.strip()
                log(f"crumb OK ({self.impl})")
            else:
                log(f"crumb: HTTP {st} ({self.impl}) — bez crumba")
        except Exception as e:  # noqa: BLE001
            log(f"crumb: {e} — bez crumba")


def fetch_chart(session: YahooSession, symbol: str, interval: str, lookback_s: int, now: int) -> dict:
    """Jedno zapytanie do Yahoo v8/chart z ponowieniami: 429/5xx → odczekaj i spróbuj
    znów (na zmianę hosty, po 429 odświeżenie crumba)."""
    params = {
        "interval": interval,
        "period1": str(now - lookback_s),
        "period2": str(now + 60),
        "includePrePost": "false",
        "events": "div,splits",
    }
    last_err = "brak próby"
    for attempt in range(MAX_ATTEMPTS):
        host = HOSTS[attempt % len(HOSTS)]
        q = dict(params)
        if session.crumb:
            q["crumb"] = session.crumb
        url = f"{host}/v8/finance/chart/{urllib.parse.quote(symbol)}?{urllib.parse.urlencode(q)}"
        try:
            st, body = session.get(url)
        except Exception as e:  # noqa: BLE001
            st, body, last_err = 0, "", f"{type(e).__name__}: {e}"
        if st == 200:
            try:
                return json.loads(body)
            except ValueError as e:
                last_err = f"niepoprawny JSON: {e}"
        elif st == 404:
            raise RuntimeError(f"{symbol} {interval}: HTTP 404 (symbol nieznany)")
        elif st:
            last_err = f"HTTP {st}"
        if attempt < MAX_ATTEMPTS - 1:
            delay = 2.0 * (2 ** attempt) + random.random()
            if st in (401, 429):
                delay = max(delay, 6.0)
                session.bootstrap()
            time.sleep(delay)
    raise RuntimeError(f"{symbol} {interval}: {last_err}")


def transform(payload: dict, dec: int) -> tuple[list[list[float]], dict]:
    """Yahoo v8/chart → lista świec [t,o,h,l,c,v] posortowana, bez luk w OHLC.

    Zwraca też meta (poprzednie zamknięcie, ostatnia cena z meta)."""
    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise RuntimeError(f"Yahoo error: {chart['error']}")
    results = chart.get("result") or []
    if not results:
        raise RuntimeError("Yahoo: pusty result")
    r = results[0]
    ts = r.get("timestamp") or []
    quote = ((r.get("indicators") or {}).get("quote") or [{}])[0]
    o, h, l, c, v = (quote.get(k) or [] for k in ("open", "high", "low", "close", "volume"))
    bars: dict[int, list[float]] = {}
    for i, t in enumerate(ts):
        try:
            oo, hh, ll, cc = o[i], h[i], l[i], c[i]
        except IndexError:
            continue
        if any(x is None for x in (oo, hh, ll, cc)):
            continue
        if not all(math.isfinite(float(x)) for x in (oo, hh, ll, cc)):
            continue
        vv = v[i] if i < len(v) and v[i] is not None else 0
        hi = max(float(hh), float(oo), float(cc))
        lo = min(float(ll), float(oo), float(cc))
        bars[int(t)] = [int(t), round(float(oo), dec), round(hi, dec), round(lo, dec), round(float(cc), dec), int(vv)]
    meta_in = r.get("meta") or {}
    meta = {
        "prev_close": meta_in.get("regularMarketPreviousClose", meta_in.get("previousClose")),
        "last": meta_in.get("regularMarketPrice"),
        "currency": meta_in.get("currency"),
        "exchange_tz": meta_in.get("exchangeTimezoneName"),
    }
    return [bars[k] for k in sorted(bars)], meta


def synthetic(inst: dict, interval_s: int, lookback_s: int, now: int, seed: int) -> list[list[float]]:
    """Syntetyczny błądzenie losowe do testów (`--sample`); wyraźnie nie-rynkowe."""
    rnd = random.Random(seed)
    base = float(inst.get("sample_base") or {"SPX": 6500.0, "ES": 6520.0, "XAUUSD": 4200.0, "XAGUSD": 52.0, "USDPLN": 3.65, "EURPLN": 4.25, "DXY": 98.5}.get(inst["id"], 100.0))
    vol = base * 0.0006 * math.sqrt(interval_s / 60)
    t0 = (now - lookback_s) // interval_s * interval_s
    price = base
    out = []
    for t in range(t0, now + 1, interval_s):
        o = price
        c = o + rnd.gauss(0, vol)
        hi = max(o, c) + abs(rnd.gauss(0, vol * 0.5))
        lo = min(o, c) - abs(rnd.gauss(0, vol * 0.5))
        d = inst["dec"]
        out.append([t, round(o, d), round(hi, d), round(lo, d), round(c, d), rnd.randint(0, 5000)])
        price = c
    return out


def build_instrument(inst: dict, now: int, sample: bool, session: YahooSession | None = None) -> tuple[dict, dict, dict]:
    """Zwraca (intraday.json, history.json, wiersz index.json) dla instrumentu."""
    files = {"intraday": {"tf": {}}, "history": {"tf": {}}}
    errors: list[str] = []
    prev_close = None
    last_meta = None
    interval_s = {"1m": 60, "5m": 300, "60m": 3600, "1d": 86400}
    for interval, lookback, target, tf in REQUESTS:
        bars: list[list[float]] = []
        source = None
        if sample:
            bars = synthetic(inst, interval_s[interval], min(lookback, 120 * interval_s[interval] * 4), now, seed=hash((inst["id"], tf)) & 0xFFFF)
            source = "synthetic"
        else:
            for sym in [inst["yahoo"], inst["fallback"]]:
                if not sym:
                    continue
                try:
                    payload = fetch_chart(session, sym, interval, lookback, now)
                    bars, meta = transform(payload, inst["dec"])
                    if bars:
                        source = f"yahoo:{sym}"
                        if interval == "1d" and meta.get("prev_close") is not None:
                            prev_close = meta["prev_close"]
                        if interval == "1m" and meta.get("last") is not None:
                            last_meta = meta["last"]
                        break
                    errors.append(f"{sym} {interval}: 0 świec")
                except Exception as e:  # noqa: BLE001
                    errors.append(str(e))
        files[target]["tf"][tf] = {"interval_s": interval_s[interval], "source": source, "bars": bars}
    # prev_close z 1d, gdy meta go nie dało: przedostatnia świeca dzienna
    d1 = files["history"]["tf"].get("1d", {}).get("bars") or []
    if prev_close is None and d1:
        last_is_today = now - d1[-1][0] < 36 * 3600
        prev_close = d1[-2][4] if (last_is_today and len(d1) >= 2) else d1[-1][4]
    # ostatnia cena: ostatnia świeca 1m, potem 5m, potem meta
    last = None
    last_bar_time = None
    for tf in ("1m", "5m"):
        b = files["intraday"]["tf"].get(tf, {}).get("bars") or []
        if b:
            last, last_bar_time = b[-1][4], b[-1][0]
            break
    if last is None and last_meta is not None:
        last = last_meta
    common = {"id": inst["id"], "name": inst["name"], "short": inst.get("short") or inst["id"], "dec": inst["dec"], "kind": inst.get("kind", ""), "group": inst.get("group", "inne"), "updated": now}
    intraday = {**common, **files["intraday"]}
    history = {**common, **files["history"]}
    row = {
        **common,
        "last": last,
        "prev_close": prev_close,
        "last_bar_time": last_bar_time,
        "bars": {tf: len(files[t]["tf"][tf]["bars"]) for _, _, t, tf in REQUESTS},
        "errors": errors,
    }
    return intraday, history, row


def write_json(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def run(out_dir: str, sample: bool) -> int:
    now = int(time.time())
    session = None
    if not sample:
        session = YahooSession()
        session.bootstrap()
    results = [build_instrument(inst, now, sample, session) for inst in INSTRUMENTS]
    rows = []
    ok = 0
    for inst, (intraday, history, row) in zip(INSTRUMENTS, results):
        write_json(os.path.join(out_dir, "feed", inst["id"], "intraday.json"), intraday)
        write_json(os.path.join(out_dir, "feed", inst["id"], "history.json"), history)
        rows.append(row)
        total = sum(row["bars"].values())
        if total:
            ok += 1
        log(f"{inst['id']:7s} last={row['last']} bars={row['bars']} errors={len(row['errors'])}")
        for e in row["errors"]:
            log(f"   ! {e}")
    index = {
        "schema": 1,
        "updated": now,
        "generated_by": "fetch_feed.py" + (" --sample" if sample else ""),
        "synthetic": sample,
        "instruments": rows,
    }
    write_json(os.path.join(out_dir, "feed", "index.json"), index)
    log(f"index: {ok}/{len(INSTRUMENTS)} instrumentów z danymi, updated={now}")
    return 0 if ok > 0 else 1


def selftest() -> int:
    """Testy transformacji na ładunku o kształcie Yahoo (bez sieci)."""
    payload = {
        "chart": {
            "result": [
                {
                    "meta": {"chartPreviousClose": 1934.3, "regularMarketPreviousClose": 4190.5, "regularMarketPrice": 4201.25, "currency": "USD"},
                    "timestamp": [1760533200, 1760533260, 1760533320, 1760533380, 1760533140],
                    "indicators": {
                        "quote": [
                            {
                                "open": [4200.0, 4200.5, None, 4201.0, 4199.0],
                                "high": [4200.8, 4201.0, 4202.0, 4200.9, 4199.5],
                                "low": [4199.9, 4200.3, 4200.0, 4200.6, 4198.8],
                                "close": [4200.5, 4200.9, 4201.5, 4201.25, 4200.0],
                                "volume": [10, None, 5, 7, 3],
                            }
                        ]
                    },
                }
            ],
            "error": None,
        }
    }
    bars, meta = transform(payload, 2)
    assert len(bars) == 4, bars  # świeca z None odrzucona
    assert [b[0] for b in bars] == sorted(b[0] for b in bars), "sortowanie po czasie"
    assert bars[0][0] == 1760533140, "najstarsza świeca pierwsza"
    assert bars[2][5] == 0, "brak wolumenu → 0"
    assert bars[-1] == [1760533380, 4201.0, 4201.25, 4200.6, 4201.25, 7], bars[-1]  # high podniesione do close
    assert meta["prev_close"] == 4190.5 and meta["last"] == 4201.25, "prev_close z regularMarketPreviousClose, nie chartPreviousClose"
    err = {"chart": {"result": None, "error": {"code": "Not Found", "description": "No data"}}}
    try:
        transform(err, 2)
        raise AssertionError("błąd Yahoo powinien rzucić wyjątek")
    except RuntimeError:
        pass
    s = synthetic(INSTRUMENTS[2], 300, 3600, 1760533200, seed=1)
    assert len(s) == 13 and all(b[2] >= max(b[1], b[4]) and b[3] <= min(b[1], b[4]) for b in s)
    insts = load_instruments()
    assert len(insts) >= 7 and len({i["id"] for i in insts}) == len(insts), "instruments.json: unikalne id"
    assert all(i["short"] and i["group"] for i in insts)
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tf_:
        json.dump({"instruments": [{"id": "bad id", "name": "x", "yahoo": "X", "dec": 2}]}, tf_)
    try:
        load_instruments(tf_.name)
        raise AssertionError("złe id powinno przerwać")
    except SystemExit:
        pass
    finally:
        os.unlink(tf_.name)
    print("selftest OK")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="out", help="katalog wyjściowy (korzeń gałęzi market-feed)")
    ap.add_argument("--sample", action="store_true", help="dane syntetyczne zamiast Yahoo (test)")
    ap.add_argument("--selftest", action="store_true", help="testy transformacji bez sieci")
    ap.add_argument("--instruments", default=INSTRUMENTS_FILE, help="plik JSON z listą instrumentów")
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    global INSTRUMENTS
    INSTRUMENTS = load_instruments(args.instruments)
    log(f"instrumenty: {len(INSTRUMENTS)} ({', '.join(i['id'] for i in INSTRUMENTS)})")
    return run(args.out, args.sample)


if __name__ == "__main__":
    sys.exit(main())
