from config import MAX_BETS_PER_COUPON


def validate_and_select(bets: list[dict]) -> list[dict]:
    """
    Keep the highest-edge bet per match, then return top-N by edge.
    One match = one bet.
    """
    best_per_match: dict[str, dict] = {}
    for bet in bets:
        mid = bet["match_id"]
        if mid not in best_per_match or bet["edge"] > best_per_match[mid]["edge"]:
            best_per_match[mid] = bet

    ranked = sorted(best_per_match.values(), key=lambda x: x["edge"], reverse=True)
    return ranked[:MAX_BETS_PER_COUPON]
