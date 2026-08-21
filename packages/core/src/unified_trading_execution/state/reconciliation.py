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
    platform_positions: dict[Instrument, Position] | None,
    local_balances: dict[str, Balance],
    platform_balances: dict[str, Balance] | None,
    local_orders: dict[str, OrderRecord],
    platform_orders: dict[str, OrderRecord] | None,
    local_fills: dict[str, list[FillRecord]],
    platform_fills: dict[str, list[FillRecord]] | None,
) -> ReconciliationResult:
    """Compare local mirror against platform state and detect all mismatches.

    A platform dataset passed as ``None`` means the adapter does not support
    that fetch (``NotImplementedError``): its comparison is skipped entirely,
    so an unsupported dataset is never mistaken for an empty one.  The local
    side is always a concrete snapshot.

    Cases handled (Section 6.3):
      1. Position quantity mismatch (including presence/absence)
      2. Balance mismatch (including presence/absence)
      3. Orphan order on platform (unknown to local)
      4. Orphan order in local (not on platform)
      5. Partial fill discrepancy
    """

    # Case 1: Position quantity mismatch.  Absence on either side is normalised
    # to quantity 0 so an open on one side and a close/absence on the other is
    # detected as a quantity disagreement rather than silently skipped.
    position_mismatches: list[ReconciliationMismatch] = []
    if platform_positions is not None:
        all_instruments = set(local_positions.keys()) | set(platform_positions.keys())
        for inst in all_instruments:
            local = local_positions.get(inst)
            platform = platform_positions.get(inst)
            local_qty = local.quantity if local is not None else Decimal("0")
            platform_qty = platform.quantity if platform is not None else Decimal("0")
            if local_qty != platform_qty:
                position_mismatches.append(
                    ReconciliationMismatch(
                        mismatch_type="position_quantity",
                        instrument=inst,
                        local_value="absent" if local is None else str(local_qty),
                        platform_value="absent" if platform is None else str(platform_qty),
                    )
                )

    # Case 2: Balance mismatch.  Absence on either side is normalised to zero.
    balance_mismatches: list[ReconciliationMismatch] = []
    if platform_balances is not None:
        all_currencies = set(local_balances.keys()) | set(platform_balances.keys())
        for cur in all_currencies:
            local_bal = local_balances.get(cur)
            platform_bal = platform_balances.get(cur)
            local_free = local_bal.free if local_bal is not None else Decimal("0")
            local_total = local_bal.total if local_bal is not None else Decimal("0")
            platform_free = platform_bal.free if platform_bal is not None else Decimal("0")
            platform_total = platform_bal.total if platform_bal is not None else Decimal("0")
            if local_free != platform_free or local_total != platform_total:
                balance_mismatches.append(
                    ReconciliationMismatch(
                        mismatch_type="balance",
                        instrument=None,
                        local_value=(
                            "absent"
                            if local_bal is None
                            else f"free={local_free}, total={local_total}"
                        ),
                        platform_value=(
                            "absent"
                            if platform_bal is None
                            else f"free={platform_free}, total={platform_total}"
                        ),
                    )
                )

    # Case 3: Orphan order on platform (unknown to local open orders).
    orphan_on_platform: list[OrderRecord] = []
    orphan_in_local: list[str] = []
    if platform_orders is not None:
        orphan_on_platform = [
            order for cid, order in platform_orders.items() if cid not in local_orders
        ]
        # Case 4: Orphan order in local (not on platform).  ``local_orders`` is
        # already scoped to live orders by the caller, so terminal orders are
        # never flagged as orphans.
        orphan_in_local = [cid for cid in local_orders if cid not in platform_orders]

    # Case 5: Partial fill discrepancy.
    partial_fill_discrepancies: list[ReconciliationMismatch] = []
    if platform_fills is not None:
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
