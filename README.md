# market-feed — świece dla artefaktu „Pulpit sesji NY”

Producent danych dla wykresów na telefonie (artefakt Claude). Workflow `market-feed` co 5 minut w oknie sesji NY (13:00–21:59 UTC, pn–pt) pobiera z Yahoo Finance świece instrumentów z `instruments.json` i wypycha je na gałąź `market-feed` (katalog `feed/`). Artefakt czyta tę gałąź konektorem GitHub.

- `fetch_feed.py` — pobieranie (stdlib + opcjonalnie curl_cffi), `--selftest` bez sieci, `--sample` dane syntetyczne.
- `instruments.json` — lista instrumentów. **Dodanie instrumentu = nowy wiersz** (id, name, short, yahoo, fallback, dec, group, kind, sample_base); po zapisaniu na `main` workflow rusza od razu (push) i potem według harmonogramu. Nie zmieniaj istniejących `id` (klucz rysunków w artefakcie).
- Pliki: `feed/index.json`, `feed/<ID>/intraday.json` (1m, 5m), `feed/<ID>/history.json` (60m, 1d); świeca `[t, o, h, l, c, v]`, `t` w sekundach UTC.
- Ręczne uruchomienie: Actions → market-feed → Run workflow. Koszt: ~108 przebiegów/dzień × 1 min Actions (repo prywatne, plan Free: 2000 min/mies.).

Kopia narzędzia i dokumentacji: `dominikdebski/FreeWilly`, `05_TOOLING/market_feed/`.
