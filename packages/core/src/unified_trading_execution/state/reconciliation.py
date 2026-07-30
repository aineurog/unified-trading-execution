"""Reconciliation logic — mismatch detection per Section 6.3."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from unified_trading_execution.events import ReconciliationMismatch
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Output of a reconciliation pass — all detected mismatches and actions."""

    position_mismatches: list[ReconciliationMismatch]
    balance_mismatches: list[ReconciliationMismatch]
    orphan_orders_on_platform: list[OrderRecord]  # import into local
    orphan_orders_in_local: list[str]  # client_order_ids to remove
    partial_fill_discrepancies: list[ReconciliationMismatch]

    @property
    def is_clean(self) -> bool:
        return not any(
            [
                self.position_mismatches,
                self.balance_mismatches,
                self.orphan_orders_on_platform,
                self.orphan_orders_in_local,
                self.partial_fill_discrepancies,
            ]
        )

    @property
    def all_mismatches(self) -> tuple[ReconciliationMismatch, ...]:
        return tuple(
            self.position_mismatches
            + self.balance_mismatches
            + self.orphan_orders_on_platform_as_mismatches()
            + self.orphan_orders_in_local_as_mismatches()
            + self.partial_fill_discrepancies
        )

    def orphan_orders_on_platform_as_mismatches(self) -> list[ReconciliationMismatch]:
        return [
            ReconciliationMismatch(
                mismatch_type="orphan_on_platform",
                instrument=o.instrument,
                local_value="absent",
                platform_value=o.client_order_id,
            )
            for o in self.orphan_orders_on_platform
        ]

    def orphan_orders_in_local_as_mismatches(self) -> list[ReconciliationMismatch]:
        return [
            ReconciliationMismatch(
                mismatch_type="orphan_in_local",
                instrument=None,
                local_value=cid,
                platform_value="absent",
            )
            for cid in self.orphan_orders_in_local
        ]


def reconcile(
    *,
    local_positions: dict[Instrument, Position],
    platform_positions: dict[Instrument, Position],
    local_balances: dict[str, Balance],
    platform_balances: dict[str, Balance],
    local_orders: dict[str, OrderRecord],
    platform_orders: dict[str, OrderRecord],
    local_fills: dict[str, list[FillRecord]],
    platform_fills: dict[str, list[FillRecord]],
) -> ReconciliationResult:
    """Compare local mirror against platform state and detect all mismatches.

    Cases handled (Section 6.3):
      1. Position quantity mismatch
      2. Balance mismatch
      3. Orphan order on platform (unknown to local)
      4. Orphan order in local (not on platform)
      5. Partial fill discrepancy
    """

    # Case 1: Position quantity mismatch
    position_mismatches: list[ReconciliationMismatch] = []
    all_instruments = set(local_positions.keys()) | set(platform_positions.keys())
    for inst in all_instruments:
        local = local_positions.get(inst)
        platform = platform_positions.get(inst)
        if local is None or platform is None:
            continue
        if local.quantity != platform.quantity:
            position_mismatches.append(
                ReconciliationMismatch(
                    mismatch_type="position_quantity",
                    instrument=inst,
                    local_value=str(local.quantity),
                    platform_value=str(platform.quantity),
                )
            )

    # Case 2: Balance mismatch
    balance_mismatches: list[ReconciliationMismatch] = []
    all_currencies = set(local_balances.keys()) | set(platform_balances.keys())
    for cur in all_currencies:
        local_bal = local_balances.get(cur)
        platform_bal = platform_balances.get(cur)
        if local_bal is None or platform_bal is None:
            continue
        if local_bal.free != platform_bal.free or local_bal.total != platform_bal.total:
            balance_mismatches.append(
                ReconciliationMismatch(
                    mismatch_type="balance",
                    instrument=None,
                    local_value=f"free={local_bal.free}, total={local_bal.total}",
                    platform_value=f"free={platform_bal.free}, total={platform_bal.total}",
                )
            )

    # Case 3: Orphan order on platform (unknown to local mirror)
    orphan_on_platform = [
        order for cid, order in platform_orders.items() if cid not in local_orders
    ]

    # Case 4: Orphan order in local mirror (not on platform)
    orphan_in_local = [cid for cid in local_orders if cid not in platform_orders]

    # Case 5: Partial fill discrepancy
    partial_fill_discrepancies: list[ReconciliationMismatch] = []
    for cid in set(local_fills.keys()) | set(platform_fills.keys()):
        local_total = sum(
            (f.fill_quantity for f in local_fills.get(cid, [])),
            start=Decimal("0"),
        )
        platform_total = sum(
            (f.fill_quantity for f in platform_fills.get(cid, [])),
            start=Decimal("0"),
        )
        if local_total != platform_total:
            partial_fill_discrepancies.append(
                ReconciliationMismatch(
                    mismatch_type="partial_fill",
                    instrument=None,  # order-id scoped, not instrument scoped
                    local_value=str(local_total),
                    platform_value=str(platform_total),
                )
            )

    return ReconciliationResult(
        position_mismatches=position_mismatches,
        balance_mismatches=balance_mismatches,
        orphan_orders_on_platform=orphan_on_platform,
        orphan_orders_in_local=orphan_in_local,
        partial_fill_discrepancies=partial_fill_discrepancies,
    )
