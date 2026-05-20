import logging
import concurrent.futures
import threading
from typing import Optional
import google.auth
from google.api_core.exceptions import PermissionDenied, NotFound
from google.cloud import resourcemanager_v3
from google.cloud import compute_v1
from google.cloud import bigquery
from google.cloud import container_v1
import googleapiclient.discovery
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

MAX_RESOURCES_PER_TYPE = 100

# googleapiclient service objects are NOT thread-safe; keep one per thread.
_tls = threading.local()

# Product category definitions — order controls display order under each project.
PRODUCT_GROUPS = [
    {"name": "Compute Engine",    "types": ["vm"],                  "icon": "🖥",  "color": "#b71c1c"},
    {"name": "Kubernetes Engine", "types": ["gke"],                 "icon": "⚙",  "color": "#01579b"},
    {"name": "Cloud Run",         "types": ["cloudrun"],            "icon": "🚀",  "color": "#1a237e"},
    {"name": "Cloud Functions",   "types": ["function"],            "icon": "ƒ",   "color": "#880e4f"},
    {"name": "Databases",         "types": ["cloudsql", "spanner"], "icon": "🗄",  "color": "#4a148c"},
    {"name": "BigQuery",          "types": ["bigquery"],            "icon": "📊",  "color": "#004d40"},
    {"name": "Cloud Storage",     "types": ["storage"],             "icon": "🪣",  "color": "#1b5e20"},
    {"name": "Pub / Sub",         "types": ["pubsub"],              "icon": "📨",  "color": "#bf360c"},
]


class GCPOrgFetcher:
    def __init__(self):
        self.credentials, self.default_project = google.auth.default()
        self.org_client = resourcemanager_v3.OrganizationsClient()
        self.folder_client = resourcemanager_v3.FoldersClient()
        self.project_client = resourcemanager_v3.ProjectsClient()

    def _get_svc(self, service_name: str, version: str):
        """Return a thread-local discovery service client (safe to reuse within one thread)."""
        if not hasattr(_tls, "services"):
            _tls.services = {}
        key = f"{service_name}_{version}"
        if key not in _tls.services:
            _tls.services[key] = googleapiclient.discovery.build(
                service_name, version,
                credentials=self.credentials,
                cache_discovery=False,
            )
        return _tls.services[key]

    # ── Organization ──────────────────────────────────────────────────────────

    def get_org_structure(self, org_id: Optional[str] = None) -> dict:
        orgs = self._list_organizations(org_id)
        if not orgs:
            return {
                "name": "No Organizations Found",
                "type": "root",
                "id": "root",
                "details": {"message": "No accessible GCP organizations found. Verify IAM permissions."},
                "children": [],
            }
        if len(orgs) == 1:
            return self._build_org_node(orgs[0])
        return {
            "name": "GCP Organizations",
            "type": "root",
            "id": "root",
            "details": {},
            "children": [self._build_org_node(o) for o in orgs],
        }

    def _list_organizations(self, org_id: Optional[str]) -> list:
        orgs = []
        try:
            if org_id:
                orgs.append(self.org_client.get_organization(name=f"organizations/{org_id}"))
            else:
                orgs.extend(self.org_client.search_organizations())
        except Exception as exc:
            logger.error("Error listing organizations: %s", exc)
        return orgs

    def _build_org_node(self, org) -> dict:
        org_numeric_id = org.name.split("/")[-1]
        node = {
            "name": org.display_name or f"Organization {org_numeric_id}",
            "id": org.name,
            "type": "organization",
            "details": {
                "org_id": org_numeric_id,
                "domain": getattr(org, "domain", ""),
                "state": org.state.name if hasattr(org.state, "name") else str(org.state),
            },
            "children": [],
        }
        self._populate_children(node, org.name)
        return node

    # ── Folders / Projects ────────────────────────────────────────────────────

    def _populate_children(self, parent_node: dict, parent_resource: str):
        folders = self._list_folders(parent_resource)
        projects = self._list_projects(parent_resource)

        for folder in folders:
            folder_node = {
                "name": folder.display_name,
                "id": folder.name,
                "type": "folder",
                "details": {
                    "folder_id": folder.name.split("/")[-1],
                    "state": folder.state.name if hasattr(folder.state, "name") else str(folder.state),
                },
                "children": [],
            }
            self._populate_children(folder_node, folder.name)
            parent_node["children"].append(folder_node)

        active_projects = [p for p in projects if p.state.name == "ACTIVE"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self._build_project_node, p): p for p in active_projects}
            for fut in concurrent.futures.as_completed(futures):
                proj = futures[fut]
                try:
                    parent_node["children"].append(fut.result())
                except Exception as exc:
                    logger.warning("Project node error %s: %s", proj.project_id, exc)

    def _list_folders(self, parent: str) -> list:
        try:
            return list(self.folder_client.list_folders(parent=parent))
        except Exception as exc:
            logger.warning("list_folders(%s): %s", parent, exc)
            return []

    def _list_projects(self, parent: str) -> list:
        try:
            return list(self.project_client.list_projects(parent=parent))
        except Exception as exc:
            logger.warning("list_projects(%s): %s", parent, exc)
            return []

    # ── Project ───────────────────────────────────────────────────────────────

    def _build_project_node(self, project) -> dict:
        project_id = project.project_id
        node = {
            "name": project.display_name or project_id,
            "id": project_id,
            "type": "project",
            "details": {
                "project_id": project_id,
                "project_number": project.name.split("/")[-1],
                "state": project.state.name if hasattr(project.state, "name") else str(project.state),
            },
            "children": [],
        }

        # Each entry: resource_type_key → fetcher function
        fetchers = {
            "vm":       self._get_compute_instances,
            "gke":      self._get_gke_clusters,
            "cloudrun": self._get_cloud_run_services,
            "function": self._get_cloud_functions,
            "cloudsql": self._get_cloud_sql,
            "spanner":  self._get_spanner_instances,
            "bigquery": self._get_bigquery_datasets,
            "storage":  self._get_cloud_storage,
            "pubsub":   self._get_pubsub_topics,
        }

        type_map: dict = {}
        api_errors: dict = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=9) as executor:
            futures = {executor.submit(fn, project_id): type_key for type_key, fn in fetchers.items()}
            for fut in concurrent.futures.as_completed(futures):
                type_key = futures[fut]
                try:
                    type_map[type_key] = fut.result()
                except Exception as exc:
                    logger.warning("Fetch error [%s] %s: %s", type_key, project_id, exc)
                    type_map[type_key] = []
                    api_errors[type_key] = str(exc)[:120]

        # Group resources into product category nodes (only non-empty groups appear)
        for group in PRODUCT_GROUPS:
            resources = []
            for t in group["types"]:
                resources.extend(type_map.get(t, []))
            if resources:
                node["children"].append({
                    "name": group["name"],
                    "type": "category",
                    "id": f"{project_id}/{group['name']}",
                    "details": {
                        "icon": group["icon"],
                        "accent_color": group["color"],
                        "count": len(resources),
                    },
                    "children": resources,
                })

        if api_errors:
            node["details"]["api_errors"] = api_errors

        return node

    # ── Compute Engine ────────────────────────────────────────────────────────

    def _get_compute_instances(self, project_id: str) -> list:
        results = []
        try:
            client = compute_v1.InstancesClient(credentials=self.credentials)
            for _zone, zone_data in client.aggregated_list(project=project_id):
                for inst in (getattr(zone_data, "instances", None) or []):
                    zone = inst.zone.split("/")[-1] if "/" in inst.zone else inst.zone
                    mtype = inst.machine_type.split("/")[-1] if "/" in inst.machine_type else inst.machine_type
                    ni = inst.network_interfaces[0] if inst.network_interfaces else None
                    ip = (getattr(ni, "network_i_p", "") or getattr(ni, "network_ip", "")) if ni else ""
                    results.append({
                        "name": inst.name,
                        "id": str(inst.id),
                        "type": "vm",
                        "details": {"zone": zone, "machine_type": mtype, "status": inst.status, "internal_ip": ip},
                        "children": [],
                    })
                    if len(results) >= MAX_RESOURCES_PER_TYPE:
                        return results
        except (PermissionDenied, NotFound):
            return []
        return results

    # ── Cloud SQL ─────────────────────────────────────────────────────────────

    def _get_cloud_sql(self, project_id: str) -> list:
        results = []
        try:
            svc = self._get_svc("sqladmin", "v1")
            req = svc.instances().list(project=project_id)
            while req and len(results) < MAX_RESOURCES_PER_TYPE:
                resp = req.execute()
                for item in resp.get("items", []):
                    results.append({
                        "name": item["name"],
                        "id": item["name"],
                        "type": "cloudsql",
                        "details": {
                            "database_version": item.get("databaseVersion", ""),
                            "tier": item.get("settings", {}).get("tier", ""),
                            "state": item.get("state", ""),
                            "region": item.get("region", ""),
                        },
                        "children": [],
                    })
                req = svc.instances().list_next(previous_request=req, previous_response=resp)
        except HttpError as exc:
            if exc.resp.status not in (403, 404):
                raise
        return results

    # ── BigQuery ──────────────────────────────────────────────────────────────

    def _get_bigquery_datasets(self, project_id: str) -> list:
        results = []
        try:
            client = bigquery.Client(project=project_id, credentials=self.credentials)
            for ds in client.list_datasets():
                results.append({
                    "name": ds.dataset_id,
                    "id": f"{project_id}.{ds.dataset_id}",
                    "type": "bigquery",
                    "details": {
                        "location": getattr(ds, "location", "unknown") or "unknown",
                        "full_id": ds.full_dataset_id,
                    },
                    "children": [],
                })
                if len(results) >= MAX_RESOURCES_PER_TYPE:
                    break
        except (PermissionDenied, NotFound):
            return []
        return results

    # ── GKE ───────────────────────────────────────────────────────────────────

    def _get_gke_clusters(self, project_id: str) -> list:
        results = []
        try:
            client = container_v1.ClusterManagerClient(credentials=self.credentials)
            response = client.list_clusters(parent=f"projects/{project_id}/locations/-")
            for cluster in response.clusters:
                results.append({
                    "name": cluster.name,
                    "id": cluster.self_link or cluster.name,
                    "type": "gke",
                    "details": {
                        "location": cluster.location,
                        "node_count": cluster.current_node_count,
                        "status": cluster.status.name if hasattr(cluster.status, "name") else str(cluster.status),
                        "k8s_version": cluster.current_master_version,
                    },
                    "children": [],
                })
        except (PermissionDenied, NotFound):
            return []
        return results

    # ── Cloud Storage ─────────────────────────────────────────────────────────

    def _get_cloud_storage(self, project_id: str) -> list:
        results = []
        try:
            svc = self._get_svc("storage", "v1")
            req = svc.buckets().list(project=project_id, maxResults=MAX_RESOURCES_PER_TYPE)
            while req:
                resp = req.execute()
                for bucket in resp.get("items", []):
                    results.append({
                        "name": bucket["name"],
                        "id": bucket["id"],
                        "type": "storage",
                        "details": {
                            "location": bucket.get("location", ""),
                            "storage_class": bucket.get("storageClass", ""),
                        },
                        "children": [],
                    })
                if len(results) >= MAX_RESOURCES_PER_TYPE:
                    break
                req = svc.buckets().list_next(previous_request=req, previous_response=resp)
        except HttpError as exc:
            if exc.resp.status not in (403, 404):
                raise
        return results

    # ── Cloud Run ─────────────────────────────────────────────────────────────

    def _get_cloud_run_services(self, project_id: str) -> list:
        results = []
        try:
            svc = self._get_svc("run", "v2")
            req = svc.projects().locations().services().list(
                parent=f"projects/{project_id}/locations/-"
            )
            while req:
                resp = req.execute()
                for service in resp.get("services", []):
                    parts = service["name"].split("/")
                    name = parts[-1]
                    region = parts[3] if len(parts) > 3 else ""
                    results.append({
                        "name": name,
                        "id": service["name"],
                        "type": "cloudrun",
                        "details": {
                            "region": region,
                            "uri": service.get("uri", ""),
                            "state": service.get("terminalCondition", {}).get("state", ""),
                        },
                        "children": [],
                    })
                    if len(results) >= MAX_RESOURCES_PER_TYPE:
                        return results
                req = svc.projects().locations().services().list_next(
                    previous_request=req, previous_response=resp
                )
        except HttpError as exc:
            if exc.resp.status not in (403, 404):
                raise
        return results

    # ── Cloud Functions ───────────────────────────────────────────────────────

    def _get_cloud_functions(self, project_id: str) -> list:
        results = []
        try:
            svc = self._get_svc("cloudfunctions", "v2")
            req = svc.projects().locations().functions().list(
                parent=f"projects/{project_id}/locations/-"
            )
            while req:
                resp = req.execute()
                for func in resp.get("functions", []):
                    name = func["name"].split("/")[-1]
                    results.append({
                        "name": name,
                        "id": func["name"],
                        "type": "function",
                        "details": {
                            "runtime": func.get("buildConfig", {}).get("runtime", ""),
                            "state": func.get("state", ""),
                        },
                        "children": [],
                    })
                    if len(results) >= MAX_RESOURCES_PER_TYPE:
                        return results
                req = svc.projects().locations().functions().list_next(
                    previous_request=req, previous_response=resp
                )
        except HttpError as exc:
            if exc.resp.status not in (403, 404):
                raise
        return results

    # ── Pub/Sub ───────────────────────────────────────────────────────────────

    def _get_pubsub_topics(self, project_id: str) -> list:
        results = []
        try:
            svc = self._get_svc("pubsub", "v1")
            req = svc.projects().topics().list(project=f"projects/{project_id}")
            while req:
                resp = req.execute()
                for topic in resp.get("topics", []):
                    name = topic["name"].split("/")[-1]
                    results.append({
                        "name": name,
                        "id": topic["name"],
                        "type": "pubsub",
                        "details": {"full_name": topic["name"]},
                        "children": [],
                    })
                    if len(results) >= MAX_RESOURCES_PER_TYPE:
                        return results
                req = svc.projects().topics().list_next(
                    previous_request=req, previous_response=resp
                )
        except HttpError as exc:
            if exc.resp.status not in (403, 404):
                raise
        return results

    # ── Cloud Spanner ─────────────────────────────────────────────────────────

    def _get_spanner_instances(self, project_id: str) -> list:
        results = []
        try:
            svc = self._get_svc("spanner", "v1")
            req = svc.projects().instances().list(parent=f"projects/{project_id}")
            while req:
                resp = req.execute()
                for inst in resp.get("instances", []):
                    name = inst["name"].split("/")[-1]
                    results.append({
                        "name": inst.get("displayName", name),
                        "id": inst["name"],
                        "type": "spanner",
                        "details": {
                            "instance_id": name,
                            "node_count": inst.get("nodeCount", 0),
                            "state": inst.get("state", ""),
                        },
                        "children": [],
                    })
                    if len(results) >= MAX_RESOURCES_PER_TYPE:
                        return results
                req = svc.projects().instances().list_next(
                    previous_request=req, previous_response=resp
                )
        except HttpError as exc:
            if exc.resp.status not in (403, 404):
                raise
        return results
