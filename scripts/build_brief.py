#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


@dataclass(frozen=True)
class Item:
    partner: str
    title: str
    url: str
    source: str
    published_at: Optional[dt.datetime]
    snippet: str
    region: str
    category: str
    score: int
    dedupe_key: str


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_datetime(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # Common Atom patterns: 2026-03-30T12:34:56Z or with offset
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def _read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _http_get(url: str, timeout_s: int = 30) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "ExecutivePartnerBrief/1.0 (+rss; noncommercial; contact: local)",
            "Accept": "application/atom+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.read()


def google_news_rss_url(query: str, *, gl: str, ceid: str, hl: str) -> str:
    # Example:
    # https://news.google.com/rss/search?q=Workday%20HCM%20when%3A7d&hl=en-US&gl=US&ceid=US:en
    q = f"{query} when:7d"
    params = {"q": q, "hl": hl, "gl": gl, "ceid": ceid}
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode(params)


def _extract_domain(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""


def _canonical_url(url: str) -> str:
    try:
        p = urllib.parse.urlparse(url)
        # Drop tracking params aggressively.
        q = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
        q = [(k, v) for (k, v) in q if k.lower() not in {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "gclid", "fbclid"}]
        query = urllib.parse.urlencode(q)
        canon = p._replace(query=query, fragment="").geturl()
        return canon
    except Exception:
        return url


def _normalize_title(s: str) -> str:
    s = html.unescape((s or "").strip())
    s = re.sub(r"\s+", " ", s)
    return s


def parse_google_news_atom(feed_xml: bytes) -> List[Dict[str, Any]]:
    # Google News RSS is Atom-like; parse as Atom if possible.
    root = ET.fromstring(feed_xml)
    entries = []
    for entry in root.findall("atom:entry", ATOM_NS):
        title_el = entry.find("atom:title", ATOM_NS)
        link_el = entry.find("atom:link", ATOM_NS)
        published_el = entry.find("atom:published", ATOM_NS) or entry.find("atom:updated", ATOM_NS)
        source_el = entry.find("atom:source/atom:title", ATOM_NS)
        summary_el = entry.find("atom:summary", ATOM_NS) or entry.find("atom:content", ATOM_NS)

        title = _normalize_title(title_el.text if title_el is not None else "")
        url = link_el.attrib.get("href", "") if link_el is not None else ""
        published_at = _parse_datetime(published_el.text if published_el is not None else "")
        source = _normalize_title(source_el.text if source_el is not None else "") or _extract_domain(url)
        snippet = _normalize_title(summary_el.text if summary_el is not None else "")

        if not title or not url:
            continue

        entries.append(
            {
                "title": title,
                "url": url,
                "published_at": published_at,
                "source": source,
                "snippet": snippet,
            }
        )
    return entries


def region_tag(text: str, us_terms: Iterable[str], eu_terms: Iterable[str]) -> str:
    t = (text or "").lower()
    has_us = any(term.lower() in t for term in us_terms)
    has_eu = any(term.lower() in t for term in eu_terms)
    if has_us and has_eu:
        return "Global"
    if has_us:
        return "US"
    if has_eu:
        return "EU"
    return "Unknown"


def categorize(text: str, topic_keywords: Dict[str, List[str]]) -> Tuple[str, int]:
    t = (text or "").lower()
    best_cat = "Other"
    best_hits = 0
    for cat, kws in topic_keywords.items():
        hits = 0
        for kw in kws:
            if kw.lower() in t:
                hits += 1
        if hits > best_hits:
            best_hits = hits
            best_cat = cat
    return best_cat, best_hits


def score_item(*, title: str, snippet: str, category: str, cat_hits: int, published_at: Optional[dt.datetime]) -> int:
    base = 0
    base += min(cat_hits, 6) * 10
    if category == "LeadershipChange":
        base += 25
    elif category == "ProductLaunch":
        base += 20
    elif category == "FundingOrMA":
        base += 22
    elif category == "MajorNews":
        base += 12

    # Prefer more recent within the week window.
    if published_at is not None:
        age_h = (_utc_now() - published_at).total_seconds() / 3600
        # 0h -> +30, 168h -> ~0
        base += int(max(0, 30 - (age_h / 6)))
    # Bonus for “executive-ish” phrases in the title.
    tt = (title or "").lower()
    if any(x in tt for x in ["ceo", "chief", "president", "launch", "unveil", "acquire", "acquisition", "partnership"]):
        base += 8
    return base


def _dedupe_key(partner: str, title: str, url: str) -> str:
    canon = _canonical_url(url)
    norm_title = re.sub(r"[^a-z0-9]+", " ", (title or "").lower()).strip()
    raw = f"{partner}||{canon}||{norm_title}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def collect_items(
    partners_cfg: Dict[str, Any],
    *,
    include_regions: Tuple[str, str] = ("US", "EU"),
    per_partner_limit: int = 30,
    polite_sleep_s: float = 0.7,
) -> List[Item]:
    regions = partners_cfg.get("regions", {}) or {}
    us_terms = regions.get("us_terms", []) or []
    eu_terms = regions.get("eu_terms", []) or []
    topic_keywords = partners_cfg.get("topic_keywords", {}) or {}
    partners = partners_cfg.get("partners", []) or []

    out: List[Item] = []
    for p in partners:
        name = p.get("name", "").strip()
        queries = p.get("queries", []) or []
        if not name or not queries:
            continue

        # Two Google News “views” to approximate US/EU coverage.
        views = [
            ("US", dict(gl="US", hl="en-US", ceid="US:en")),
            ("EU", dict(gl="GB", hl="en-GB", ceid="GB:en")),
        ]

        for region_label, v in views:
            if region_label not in include_regions:
                continue

            for q in queries:
                url = google_news_rss_url(q, **v)
                try:
                    xml_bytes = _http_get(url)
                    rows = parse_google_news_atom(xml_bytes)
                except Exception:
                    rows = []

                for row in rows[:per_partner_limit]:
                    title = row["title"]
                    item_url = row["url"]
                    published_at = row.get("published_at")
                    source = row.get("source") or _extract_domain(item_url)
                    snippet = row.get("snippet") or ""

                    text_for_tags = f"{title}\n{snippet}\n{source}\n{item_url}"
                    r = region_tag(text_for_tags, us_terms=us_terms, eu_terms=eu_terms)
                    cat, cat_hits = categorize(text_for_tags, topic_keywords=topic_keywords)
                    score = score_item(
                        title=title,
                        snippet=snippet,
                        category=cat,
                        cat_hits=cat_hits,
                        published_at=published_at,
                    )
                    dk = _dedupe_key(name, title, item_url)
                    out.append(
                        Item(
                            partner=name,
                            title=title,
                            url=_canonical_url(item_url),
                            source=source,
                            published_at=published_at,
                            snippet=snippet,
                            region=r if r != "Unknown" else region_label,
                            category=cat,
                            score=score,
                            dedupe_key=dk,
                        )
                    )
                time.sleep(polite_sleep_s)

    return out


def _within_last_days(published_at: Optional[dt.datetime], days: int) -> bool:
    if published_at is None:
        return True
    return (_utc_now() - published_at) <= dt.timedelta(days=days)


def dedupe_and_filter(items: List[Item], *, days: int = 7, max_per_partner: int = 15) -> List[Item]:
    # Drop anything older than window when date is known.
    items = [it for it in items if _within_last_days(it.published_at, days)]

    # Dedupe by (partner, dedupe_key) but also collapse identical URLs across queries/views.
    seen: set[str] = set()
    best_by_key: Dict[str, Item] = {}
    for it in items:
        k = f"{it.partner}::{it.dedupe_key}"
        if k not in best_by_key or it.score > best_by_key[k].score:
            best_by_key[k] = it

    # Secondary: canonical url per partner
    best: List[Item] = []
    for it in best_by_key.values():
        k2 = f"{it.partner}::{it.url}"
        if k2 in seen:
            continue
        seen.add(k2)
        best.append(it)

    # Sort and cap per partner
    best.sort(key=lambda x: (x.partner.lower(), -(x.score), x.published_at or dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)), reverse=False)
    out: List[Item] = []
    counts: Dict[str, int] = {}
    for it in sorted(best, key=lambda x: (x.partner.lower(), -x.score)):
        c = counts.get(it.partner, 0)
        if c >= max_per_partner:
            continue
        counts[it.partner] = c + 1
        out.append(it)
    return out


def _fmt_dt(d: Optional[dt.datetime]) -> str:
    if d is None:
        return ""
    try:
        return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return ""


def render_html(items: List[Item], *, title: str, days: int = 7) -> str:
    partners = sorted({it.partner for it in items})
    by_partner: Dict[str, List[Item]] = {p: [] for p in partners}
    for it in items:
        by_partner[it.partner].append(it)
    for p in partners:
        by_partner[p].sort(key=lambda x: -x.score)

    period_end = _utc_now().date()
    period_start = (period_end - dt.timedelta(days=days)).isoformat()
    period_end_s = period_end.isoformat()

    def esc(s: str) -> str:
        return html.escape(s or "", quote=True)

    rows_html = []
    top_bullets_html = []
    for p in partners:
        top3 = by_partner[p][:3]
        if top3:
            lis = "".join(
                f'<li><a href="{esc(it.url)}" target="_blank" rel="noopener noreferrer">{esc(it.title)}</a>'
                f' <span style="color:#6b7280">({esc(it.category)}, {esc(it.region)})</span></li>'
                for it in top3
            )
            top_bullets_html.append(f"<h3>{esc(p)}</h3><ul>{lis}</ul>")

        for it in by_partner[p]:
            rows_html.append(
                "<tr>"
                f"<td>{esc(it.partner)}</td>"
                f'<td><a href="{esc(it.url)}" target="_blank" rel="noopener noreferrer">{esc(it.title)}</a></td>'
                f"<td>{esc(it.category)}</td>"
                f"<td>{esc(it.region)}</td>"
                f"<td>{esc(_fmt_dt(it.published_at))}</td>"
                f"<td>{esc(it.source)}</td>"
                "</tr>"
            )

    rows = "\n".join(rows_html) if rows_html else '<tr><td colspan="6">No items found in the last 7 days.</td></tr>'

    # Simple, email-friendly HTML (no external CSS).
    return f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{esc(title)}</title>
  </head>
  <body style="margin:0;padding:0;background:#f6f7fb;font-family:Arial,Helvetica,sans-serif;">
    <div style="max-width:980px;margin:0 auto;padding:24px;">
      <div style="background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;padding:20px;">
        <div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;">
          <h1 style="margin:0;font-size:20px;line-height:1.2;">{esc(title)}</h1>
          <div style="color:#6b7280;font-size:12px;">{esc(period_start)} → {esc(period_end_s)} (UTC)</div>
        </div>
        <p style="margin:12px 0 18px;color:#374151;font-size:13px;line-height:1.4;">
          Weekly partner intelligence (free sources). Best-effort tagging for region and topic; always confirm via the linked sources.
        </p>

        <h2 style="margin:18px 0 10px;font-size:16px;">Top highlights (per partner)</h2>
        <div style="color:#111827;font-size:13px;line-height:1.4;">
          {''.join(top_bullets_html) if top_bullets_html else '<p style="margin:0;color:#6b7280;">No highlights.</p>'}
        </div>

        <h2 style="margin:22px 0 10px;font-size:16px;">Partner Intelligence</h2>
        <div style="overflow:auto;border:1px solid #e5e7eb;border-radius:10px;">
          <table cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;min-width:900px;width:100%;font-size:12px;">
            <thead>
              <tr style="background:#f3f4f6;color:#111827;text-align:left;">
                <th style="padding:10px;border-bottom:1px solid #e5e7eb;">Partner</th>
                <th style="padding:10px;border-bottom:1px solid #e5e7eb;">Item</th>
                <th style="padding:10px;border-bottom:1px solid #e5e7eb;">Category</th>
                <th style="padding:10px;border-bottom:1px solid #e5e7eb;">Region</th>
                <th style="padding:10px;border-bottom:1px solid #e5e7eb;">Date</th>
                <th style="padding:10px;border-bottom:1px solid #e5e7eb;">Source</th>
              </tr>
            </thead>
            <tbody>
              {rows}
            </tbody>
          </table>
        </div>

        <p style="margin:16px 0 0;color:#6b7280;font-size:11px;line-height:1.4;">
          Generated at {esc(_utc_now().strftime('%Y-%m-%d %H:%M UTC'))}.
        </p>
      </div>
    </div>
  </body>
</html>
"""


def main(argv: List[str]) -> int:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    partners_path = os.path.join(root, "scripts", "partners.json")
    out_html_path = os.environ.get("BRIEF_OUT", os.path.join(root, "out", "executive_partner_brief.html"))
    title = os.environ.get("BRIEF_TITLE", "Executive Partner Brief")
    days = int(os.environ.get("BRIEF_DAYS", "7"))

    partners_cfg = _read_json(partners_path)

    items = collect_items(partners_cfg)
    items = dedupe_and_filter(items, days=days)
    html_doc = render_html(items, title=title, days=days)

    os.makedirs(os.path.dirname(out_html_path), exist_ok=True)
    with open(out_html_path, "w", encoding="utf-8") as f:
        f.write(html_doc)

    # Also emit a compact JSON for debugging / future v2.
    debug_out = os.environ.get("BRIEF_DEBUG_JSON", os.path.join(root, "out", "executive_partner_brief.items.json"))
    payload = [
        {
            "partner": it.partner,
            "title": it.title,
            "url": it.url,
            "source": it.source,
            "published_at": it.published_at.isoformat() if it.published_at else None,
            "region": it.region,
            "category": it.category,
            "score": it.score,
        }
        for it in items
    ]
    with open(debug_out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(out_html_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

