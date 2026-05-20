import csv
import io
import os
import logging
import threading
from flask import Flask, jsonify, render_template, request, Response
from gcp_fetcher import GCPOrgFetcher

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_cache: dict = {}
_cache_lock = threading.Lock()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/org-structure")
def get_org_structure():
    org_id = request.args.get("org_id", "").strip()
    force_refresh = request.args.get("refresh", "false").lower() == "true"
    cache_key = f"org_{org_id or 'all'}"

    with _cache_lock:
        if not force_refresh and cache_key in _cache:
            logger.info("Returning cached structure for %s", cache_key)
            return jsonify(_cache[cache_key])

    try:
        fetcher = GCPOrgFetcher()
        structure = fetcher.get_org_structure(org_id or None)
        with _cache_lock:
            _cache[cache_key] = structure
        return jsonify(structure)
    except Exception as exc:
        logger.error("Error fetching org structure: %s", exc, exc_info=True)
        return jsonify({"error": str(exc), "type": "error"}), 500


_SERVICE_CATEGORY = {
    "vm":       "Compute Engine",
    "gke":      "Kubernetes Engine",
    "cloudrun": "Cloud Run",
    "function": "Cloud Functions",
    "cloudsql": "Databases (Cloud SQL)",
    "spanner":  "Databases (Spanner)",
    "bigquery": "BigQuery",
    "storage":  "Cloud Storage",
    "pubsub":   "Pub / Sub",
}

_DETAIL_SKIP = frozenset({"icon", "accent_color", "count", "full_id", "full_name", "api_errors"})
_LOCATION_KEYS = ("zone", "location", "region")
_STATUS_KEYS   = ("status", "state")

CSV_COLUMNS = [
    "Organization ID", "Organization Name",
    "Folder Path",
    "Project ID", "Project Name",
    "Service Category", "Service Type",
    "Resource Name",
    "Location / Region / Zone",
    "Status / State",
    "Additional Details",
]


def _flatten_structure(structure: dict) -> list[dict]:
    rows: list[dict] = []

    def walk(node, org_id="", org_name="", folder_parts=None, project_id="", project_name=""):
        if folder_parts is None:
            folder_parts = []
        ntype = node.get("type", "")

        if ntype == "organization":
            org_id   = node.get("details", {}).get("org_id", node.get("id", ""))
            org_name = node.get("name", "")
        elif ntype == "folder":
            folder_parts = folder_parts + [node.get("name", "")]
        elif ntype == "project":
            project_id   = node.get("details", {}).get("project_id", node.get("id", ""))
            project_name = node.get("name", "")
        elif ntype in _SERVICE_CATEGORY:
            details  = node.get("details") or {}
            location = next((details[k] for k in _LOCATION_KEYS if details.get(k)), "")
            status   = next((details[k] for k in _STATUS_KEYS   if details.get(k)), "")
            extra    = "; ".join(
                f"{k.replace('_', ' ')}: {v}"
                for k, v in details.items()
                if k not in _DETAIL_SKIP
                and k not in _LOCATION_KEYS
                and k not in _STATUS_KEYS
                and v not in ("", None)
            )
            rows.append({
                "Organization ID":          org_id,
                "Organization Name":        org_name,
                "Folder Path":              " / ".join(folder_parts),
                "Project ID":               project_id,
                "Project Name":             project_name,
                "Service Category":         _SERVICE_CATEGORY[ntype],
                "Service Type":             ntype,
                "Resource Name":            node.get("name", ""),
                "Location / Region / Zone": location,
                "Status / State":           status,
                "Additional Details":       extra,
            })
            return  # leaf — no children to walk

        for child in node.get("children", []):
            walk(child, org_id, org_name, folder_parts, project_id, project_name)

    walk(structure)
    return rows


@app.route("/api/export-csv")
def export_csv():
    org_id    = request.args.get("org_id", "").strip()
    cache_key = f"org_{org_id or 'all'}"

    with _cache_lock:
        structure = _cache.get(cache_key)

    if not structure:
        try:
            fetcher   = GCPOrgFetcher()
            structure = fetcher.get_org_structure(org_id or None)
            with _cache_lock:
                _cache[cache_key] = structure
        except Exception as exc:
            logger.error("Error fetching org structure for CSV export: %s", exc, exc_info=True)
            return jsonify({"error": str(exc)}), 500

    rows = _flatten_structure(structure)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(rows)

    # UTF-8 BOM so Excel auto-detects encoding on double-click
    csv_bytes = ("﻿" + buf.getvalue()).encode("utf-8")

    return Response(
        csv_bytes,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="gcp_asset_inventory.csv"'},
    )


@app.route("/api/cache/clear", methods=["POST"])
def clear_cache():
    with _cache_lock:
        _cache.clear()
    return jsonify({"status": "cleared"})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
