from config import KELLY_FRACTION, MAX_BET_PCT, STOP_LOSS_PCT
from memory import get_bankroll


def kelly_stake(model_prob: float, odds: float, bankroll: float) -> float:
    """Quarter-Kelly stake, capped at MAX_BET_PCT of bankroll."""
    b = odds - 1.0
    q = 1.0 - model_prob
    kelly_full = (model_prob * b - q) / b
    kelly_full = max(kelly_full, 0.0)
    stake = min(kelly_full * KELLY_FRACTION * bankroll, bankroll * MAX_BET_PCT)
    return round(stake, 2)


def is_stop_loss_triggered() -> bool:
    """Return True if drawdown >= STOP_LOSS_PCT from initial bankroll."""
    current, initial = get_bankroll()
    if initial <= 0:
        return False
    return (initial - current) / initial >= STOP_LOSS_PCT


def add_stakes(bets: list[dict]) -> list[dict]:
    """Attach Kelly stake to each bet; drop bets where stake == 0."""
    current, _ = get_bankroll()
    result = []
    for bet in bets:
        stake = kelly_stake(bet["model_prob"], bet["odds"], current)
        if stake > 0:
            result.append({**bet, "stake": stake})
    return result
