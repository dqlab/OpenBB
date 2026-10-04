"""Thin saved-data boundary, with no collector, database or implicit network call."""

from openbb_core.provider.standard_models.alpha_research import (
    DatasetInput,
    ResearchDatasetRef,
)

from openbb_alpha.store import Store


def register(data: DatasetInput) -> ResearchDatasetRef:
    import pyarrow as pa

    data = DatasetInput.model_validate(data)
    store = Store()
    source = {(r.instrument_id, r.session): r for r in data.rows}
    aligned = []
    for instrument in sorted(data.instruments):
        for index, session in enumerate(data.calendar):
            row = source.get((instrument, session.session))
            record = {
                "instrument_id": instrument,
                "session": session.session.isoformat(),
                "slot": index,
                "decision_time": session.decision_time.isoformat(),
                "open_time": session.open_time.isoformat(),
                "available_at": row.available_at.isoformat() if row else None,
                "eligible": row.eligible if row else False,
                "present": row is not None,
                "known": row.available_at <= session.decision_time if row else False,
            }
            for field in ("price", "raw_close", "raw_share_volume", "open"):
                record[field] = getattr(row, field) if row else None
            if row and data.price_convention == "synthetic_no_actions":
                if row.price != row.raw_close:
                    raise ValueError("synthetic no-action price must equal raw_close")
            aligned.append(record)
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("session", pa.string()),
            ("slot", pa.int32()),
            ("decision_time", pa.string()),
            ("open_time", pa.string()),
            ("available_at", pa.string()),
            ("eligible", pa.bool_()),
            ("present", pa.bool_()),
            ("known", pa.bool_()),
            ("price", pa.float64()),
            ("raw_close", pa.float64()),
            ("raw_share_volume", pa.float64()),
            ("open", pa.float64()),
        ]
    )
    panel = store.put_table(pa.Table.from_pylist(aligned, schema=schema))
    metadata = data.model_dump(mode="json", exclude={"rows", "calendar"})
    metadata.update(
        panel=panel.model_dump(mode="json"),
        instruments=sorted(data.instruments),
        calendar=[s.model_dump(mode="json") for s in data.calendar],
        start_date=data.calendar[0].session.isoformat(),
        end_date=data.calendar[-1].session.isoformat(),
    )
    key = store.put_json("dataset", metadata)
    return ResearchDatasetRef(id=key, **metadata)


def load(dataset_id: str):
    store = Store()
    ref = ResearchDatasetRef(id=dataset_id, **store.get_json(dataset_id, "dataset"))
    if ref.classification != "Lab":
        raise ValueError("v1 research is Lab only")
    return ref, store.table(ref.panel)


def from_dq_history(result, *, metadata: dict, eligibility: dict, price_field: str | None):
    """Convert a bounded, already fetched dq_market.history OBBject/dict.

    Caller supplies explicit convention/calendar and historical eligibility. Raw
    fields and canonical IDs are mandatory; no vendor-based adjustment inference.
    The existing DQ command owns all querying and publication resolution.
    """
    payload = result.results if hasattr(result, "results") else result
    rows = payload.get("data", payload.get("rows"))
    meta = payload.get("metadata", payload.get("meta", {}))
    publication = meta.get("publication_id") or payload.get("publication_id")
    if not publication or meta.get("next_cursor") or payload.get("next_cursor"):
        raise ValueError("DQ history must be complete and pinned to a publication")
    if not isinstance(rows, list) or len(rows) > 100000:
        raise ValueError("expected bounded saved DQ history records")
    if price_field not in (None, "close", "total_return_index"):
        raise ValueError("explicit close, total_return_index or absent-price mapping required")
    expected = metadata["price_convention"]
    if expected == "verified_total_return_index" and price_field != "total_return_index":
        raise ValueError("real factors require verified total_return_index")
    if expected == "raw_close_only" and price_field is not None:
        raise ValueError("raw-close snapshots must not invent a consistent return index")
    from datetime import datetime
    from zoneinfo import ZoneInfo

    if meta.get("revision_mode") not in (None, "latest"):
        raise ValueError("resolve revisions using the existing DQ interface first")
    if meta.get("reconstructed_collector_history"):
        raise ValueError("use central publication availability for research snapshots")
    mapped = []
    for row in rows:
        instrument = row.get("instrument_uid", row.get("instrument_id"))
        session = row.get("session")
        if session is None:
            stamp = row["bar_start"]
            stamp = (
                datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                if isinstance(stamp, str)
                else stamp
            )
            if stamp.tzinfo is None:
                raise ValueError("DQ bar_start must be timezone aware")
            session = stamp.astimezone(ZoneInfo(metadata["timezone"])).date().isoformat()
        if row.get("kind", "bars") != "bars" or row.get("frequency", "1d") not in (
            "1d",
            "1 day",
            "1D",
        ):
            raise ValueError("v1 research consumes daily bars only")
        key = (instrument, session)
        if key not in eligibility:
            raise ValueError("missing historical eligibility")
        if (
            row.get("volume_unit") not in ("raw_shares", "shares")
            or row.get("currency") != metadata["currency"]
        ):
            raise ValueError("DQ source units must match the declared dataset")
        if row.get("adjustment") not in ("raw", "unadjusted", "none"):
            raise ValueError("DQ close and share volume must be explicitly unadjusted")
        available = row.get("first_published_at", row.get("available_time"))
        if available is None:
            raise ValueError("DQ central publication availability is missing")
        mapped.append(
            {
                "instrument_id": instrument,
                "session": session,
                "available_at": available,
                "eligible": eligibility[key],
                "price": row[price_field] if price_field else None,
                "raw_close": row["close"],
                "raw_share_volume": row["volume"],
                "open": row["open"],
            }
        )
    metadata = dict(metadata)
    metadata["quality_flags"] = tuple(metadata.get("quality_flags", ())) + (
        "DQ publication-pinned; historical universe supplied by caller; not PIT certified",
    )
    metadata["source"] = dict(
        metadata["source"], access_adapter="dq_quant_data", snapshot=publication
    )
    return register(DatasetInput(rows=tuple(mapped), **metadata))
