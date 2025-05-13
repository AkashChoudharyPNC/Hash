# Let's extend the current baggage tracking API to support the 3 milestone features.

from fastapi import Query
from sqlalchemy import func
from collections import defaultdict

# 1. Enhance GET /scan/latest/{bag_tag_id} to support ?latest=true
@app.get("/baggage/scans/bag/{bag_tag_id}", response_model=Optional[ScanOut])
def get_bag_scan(
    bag_tag_id: str,
    latest: bool = Query(default=False)
):
    db = next(get_db())
    query = db.query(BagScan).filter(BagScan.bag_tag_id == bag_tag_id)
    if latest:
        scan = query.order_by(BagScan.scan_time.desc()).first()
    else:
        scan = query.order_by(BagScan.scan_time).all()
        return scan  # list of scans if not latest
    if not scan:
        raise HTTPException(status_code=404, detail="No scan record found for this bag")
    return scan

# 2. Bags currently en route to a gate (unique bags within N minutes)
@app.get("/baggage/active/gate/{destination_gate}")
def get_active_bags(destination_gate: str, since_minutes: int = 60):
    db = next(get_db())
    time_threshold = datetime.utcnow() - timedelta(minutes=since_minutes)

    # Get latest scan per bag for the given gate
    subquery = (
        db.query(
            BagScan.bag_tag_id,
            func.max(BagScan.scan_time).label("latest_time")
        )
        .filter(BagScan.destination_gate == destination_gate)
        .group_by(BagScan.bag_tag_id)
        .subquery()
    )

    results = (
        db.query(BagScan)
        .join(subquery, (BagScan.bag_tag_id == subquery.c.bag_tag_id) & (BagScan.scan_time == subquery.c.latest_time))
        .filter(BagScan.scan_time >= time_threshold)
        .all()
    )

    return [
        {
            "bag_tag_id": r.bag_tag_id,
            "last_scan_time": r.scan_time,
            "last_location": r.location
        }
        for r in results
    ]

# 3. Count of unique bags per gate in last N minutes
@app.get("/baggage/stats/gate-counts")
def get_gate_counts(since_minutes: int = 60):
    db = next(get_db())
    time_threshold = datetime.utcnow() - timedelta(minutes=since_minutes)

    subquery = (
        db.query(
            BagScan.bag_tag_id,
            func.max(BagScan.scan_time).label("latest_time")
        )
        .filter(BagScan.scan_time >= time_threshold)
        .group_by(BagScan.bag_tag_id)
        .subquery()
    )

    recent_scans = (
        db.query(BagScan)
        .join(subquery, (BagScan.bag_tag_id == subquery.c.bag_tag_id) & (BagScan.scan_time == subquery.c.latest_time))
        .all()
    )

    gate_counts = defaultdict(set)
    for scan in recent_scans:
        gate_counts[scan.destination_gate].add(scan.bag_tag_id)

    return [
        {"destination_gate": gate, "unique_bag_count": len(tags)}
        for gate, tags in gate_counts.items()
    ]
