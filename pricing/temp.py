def _build_dynamic_query(title: Any) -> Optional[str]:
    """
    Build a safe dynamic query from title.
    Ensures no ints slip into join() calls by converting all tokens to strings.
    """
    # Handle dict vs string safely
    if isinstance(title, dict):
        raw = title.get("value") or title.get("title") or ""
    else:
        raw = str(title or "")

    raw = raw.strip()
    if not raw:
        return None

    norm = normalize_title_global(raw)

    # Extract structured pieces (may return ints → MUST sanitize)
    year = extract_year_from_title(norm)
    card_no = extract_card_number_from_title(norm)
    player_tokens = extract_player_tokens_from_title(norm) or []
    set_tokens = extract_set_tokens(norm) or []
    parallel_tokens = extract_parallels_from_title(norm) or []

    # SAFETY: Convert everything to strings
    if year is not None:
        year = str(year)
    if card_no is not None:
        card_no = str(card_no)

    player_tokens = [str(t) for t in player_tokens]
    set_tokens = [str(t) for t in set_tokens]
    parallel_tokens = [str(t) for t in parallel_tokens]

    parts: List[str] = []

    if year:
        parts.append(year)

    if set_tokens:
        parts.append(" ".join(set_tokens))

    if player_tokens:
        parts.append(" ".join(player_tokens))

    if card_no:
        parts.append(card_no)

    if parallel_tokens:
        parts.append(" ".join(parallel_tokens))

    # If nothing structured was found → fallback to normalized title
    if not parts:
        parts.append(norm)

    base_query = " ".join(parts)

    # Standard negative filters
    filters = "-lot -lots -factory -break -case -sealed"

    return f"{base_query} {filters}".strip()


def _build_active_fallback_queries(title: Any) -> List[str]:
    """
    Build a small list of fallback queries for active comp search.
    Accepts either a raw string or an eBay dict with 'title' / 'value'.
    """
    if isinstance(title, dict):
        raw = title.get("value") or title.get("title") or ""
    else:
        raw = str(title or "")

    raw = raw.strip()
    if not raw:
        return []

    norm = normalize_title_global(raw)
    year = extract_year_from_title(norm) or ""
    player_tokens = extract_player_tokens_from_title(norm) or []
    set_tokens = extract_set_tokens(norm) or []

    filters = " -lot -lots -factory -break -case -sealed"
    queries: List[str] = []

    # Fallback 1: year + normalized title
    if year:
        queries.append(f"{year} {norm}{filters}")

    # Fallback 2: player + set
    if player_tokens and set_tokens:
        queries.append(
            f"{' '.join(player_tokens)} {' '.join(set_tokens)}{filters}"
        )

    # Fallback 3: just normalized title
    queries.append(f"{norm}{filters}")

    # Deduplicate while preserving order
    seen_local = set()
    deduped: List[str] = []
    for q in queries:
        q_norm = q.strip().lower()
        if not q_norm or q_norm in seen_local:
            continue
        seen_local.add(q_norm)
        deduped.append(q.strip())

    return deduped

def _fetch_active_items_browse_for_query(query: str, limit: int = ACTIVE_LIMIT) -> List[Dict]:
    """
    v7 — Browse API for actives (FINAL)
    Uses strict match after normal Browse filters.
    """
    if not query:
        return []

    print(f"[BROWSE MERGED] {query}")

    filter_parts = [
        "buyingOptions:FIXED_PRICE",
        "priceType:FIXED",
    ]
    filter_str = ",".join(filter_parts)

    params = {
        "q": query,
        "limit": str(limit),
        "filter": filter_str,
        "fieldgroups": "EXTENDED",
    }

    r, hdrs = _request(
        "GET",
        EBAY_BROWSE_SEARCH,
        headers=_headers(),
        params=params,
        timeout=ACTIVE_TIMEOUT,
        label="Browse/ActiveMerged",
    )
    api_meter_browse()

    if not r or r.status_code != 200:
        return []

    items = r.json().get("itemSummaries", [])
    results: List[Dict] = []

    bad_condition_terms = [
        "poor", "fair", "filler", "filler card", "crease", "creased",
        "damage", "damaged", "bent", "writing", "pen", "marker",
        "tape", "miscut", "off-center", "oc", "kid card",
    ]
    lot_like_terms = [
        " lot", "lot of", "lots", "complete set", "factory set", "team set", "set ",
        "sealed box", "hobby box", "blaster box", "mega box", "hanger box", "value box",
        "cello box", "rack pack", "value pack", "fat pack", "jumbo box",
        "case break", "player break", "team break", "group break", "box break",
        "box", "case",
    ]

    subject_sig = _extract_card_signature_from_title(query)

    for it in items:
        title_it = normalize_title(it.get("title"))
        lower_title = title_it.lower()

        # exclude graded
        if _is_graded(title_it):
            continue

        opts = it.get("buyingOptions") or []
        if "FIXED_PRICE" not in opts:
            continue
        if "AUCTION" in opts:
            continue

        group_type = it.get("itemGroupType")
        if group_type:
            continue

        web_url = (it.get("itemWebUrl") or "").lower()
        if "variation" in web_url:
            continue
        if "auction" in web_url or "bid=" in web_url or "bids=" in web_url:
            continue

        if any(term in lower_title for term in lot_like_terms):
            continue
        if re.search(r"\b\d+\s*(card|cards)\b", lower_title):
            continue
        if re.search(r"\bx\d{1,3}\b", lower_title):
            continue
        if any(term in lower_title for term in bad_condition_terms):
            continue

        price = _extract_total_price(it)
        if not price:
            continue

        comp_sig = _extract_card_signature_from_title(title_it)
        if subject_sig and not _titles_match_strict(subject_sig, comp_sig):
            continue

        results.append({"title": title_it, "total": price})

    return results

def _fetch_active_items_finding_for_query(query: str, limit: int = ACTIVE_LIMIT) -> List[Dict]:
    """
    v7 — Finding API fallback (FINAL)
    • Uses v7 signature strict match BEFORE returning
    • Much softer filters (Finding data is already less consistent)
    """
    if not query:
        return []

    print(f"[FINDING MERGED] {query}")

    params = {
        "OPERATION-NAME": "findItemsByKeywords",
        "SERVICE-VERSION": "1.0.0",
        "SECURITY-APPNAME": os.getenv("EBAY_APP_ID"),
        "RESPONSE-DATA-FORMAT": "JSON",
        "paginationInput.entriesPerPage": str(limit),
        "keywords": query,
    }

    r, hdrs = _request(
        "GET",
        EBAY_FINDING_API,
        headers={"X-EBAY-SOA-REQUEST-DATA-FORMAT": "JSON"},
        params=params,
        timeout=ACTIVE_TIMEOUT,
        label="Finding/ActiveMerged",
    )
    # api_meter_finding()

    if not r or r.status_code != 200:
        return []

    data = r.json()
    items = (
        data.get("findItemsByKeywordsResponse", [{}])[0]
        .get("searchResult", [{}])[0]
        .get("item", [])
    )

    results: List[Dict] = []
    subject_sig = _extract_card_signature_from_title(query)

    for it in items:
        title_it = normalize_title(it.get("title", [""])[0])
        price = _extract_total_price_from_finding(it)
        if not price:
            continue

        if _is_graded(title_it):
            continue

        comp_sig = _extract_card_signature_from_title(title_it)
        if subject_sig and not _titles_match_strict(subject_sig, comp_sig):
            continue

        results.append({"title": title_it, "total": price})

    return results
