"""Allowlisted COIN-M conditional algo routes."""

COIN_M_ALGO_ORDER_PATH = "algoOrder"
COIN_M_OPEN_ALGO_ORDERS_PATH = "openAlgoOrders"
COIN_M_ALL_ALGO_ORDERS_PATH = "allAlgoOrders"

COIN_M_ALGO_ENDPOINTS = frozenset(
    {
        COIN_M_ALGO_ORDER_PATH,
        COIN_M_OPEN_ALGO_ORDERS_PATH,
        COIN_M_ALL_ALGO_ORDERS_PATH,
    }
)
COIN_M_ALGO_METHODS = frozenset({"GET", "POST", "DELETE"})
