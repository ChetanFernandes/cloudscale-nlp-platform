import pandas as pd

# -----------------------------------
# ✅ Heuristic 1: Valid Header Check
# -----------------------------------
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


# -----------------------------------
# ✅ Heuristic 2: Detect Key-Value Format
# -----------------------------------
def is_key_value_pattern(df):
    low_value_rows = 0 # rows with very few values
    total = 0 #how many rows cheked

    for _, row in df.head(20).iterrows():
        values = [v for v in row if pd.notna(v) and str(v).strip() != ""]
        total += 1

        if len(values) <= 2:
            low_value_rows += 1

    return low_value_rows > total * 0.7
