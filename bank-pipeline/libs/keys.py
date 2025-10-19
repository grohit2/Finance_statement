import hashlib
def txn_id(profile_id: str, account_id: str, post_date: str, amount: float, clean_desc: str) -> str:
    payload = f"{profile_id}|{account_id}|{post_date}|{amount:.2f}|{clean_desc.strip().lower()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
