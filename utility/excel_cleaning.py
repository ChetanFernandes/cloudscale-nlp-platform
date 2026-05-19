

def is_valid_header(row):
    if not row:
        return False

    non_null = [c for c in row if c is not None and str(c).strip() != ""]

    # Rule 1: Reject single-cell rows (like "PUMP REQUIREMENT")
    if len(non_null) <= 1:
        return False
    
    # Rule 2: Must span across multiple columns (at least 3)
    if len(non_null) < 3:
        return False

    # Reject title-like rows (long phrases)
    avg_words = sum(len(str(c).split()) for c in non_null) / len(non_null)
    if avg_words > 3:
        return False

    # Mostly strings
    string_count = sum(isinstance(c, str) for c in non_null)
    if string_count < len(non_null) * 0.6:
        return False

    return True

def has_split_headers(header, next_row):
    for i in range(max(len(header), len(next_row))):
        h = header[i] if i < len(header) else None
        n = next_row[i] if i < len(next_row) else None

        h_empty = h is None or str(h).strip() == ""
        n_filled = n is not None and str(n).strip() != ""

        # ❌ Split header detected
        if h_empty and n_filled:
            return True

    return False

def is_footer(row):
    if not row:
        return False
    first = next((str(c).lower() for c in row if c), "")
    keywords = ["total", "summary", "remarks", "notes"]
    return any(k in first for k in keywords)