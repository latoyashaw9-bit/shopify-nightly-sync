import os
import sys
import csv
import datetime
from urllib.parse import urljoin
import requests
from typing import Dict, Any, List, Optional

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
STORE = os.environ["SHOPIFY_STORE_DOMAIN"]
TOKEN = os.environ["SHOPIFY_ADMIN_API_TOKEN"]
BASE = f"https://{STORE}/admin/api/2024-10/"
HEADERS = {"X-Shopify-Access-Token": TOKEN}

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
STAGING_URL = os.environ["NOTION_STAGING_URL"]  # e.g., 

# Upsert key: SKU + Location ID
UPSERT_KEY_FIELDS = ("SKU", "Location ID")

# ------------------------------------------------------------------------------
# Shopify helpers
# ------------------------------------------------------------------------------
def shopify_get_all(path: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
	"""Get all pages for a Shopify endpoint, following Link headers."""
	items: List[Dict[str, Any]] = []
	url = urljoin(BASE, path)
	while True:
		r = requests.get(url, headers=HEADERS, params=params)
		r.raise_for_status()
		data = r.json()
		# Detect envelope key
		for k in ("variants", "inventory_levels", "products"):
			if k in data:
				items.extend(data[k])
				break
		else:
			# Unknown envelope, best effort
			if isinstance(data, dict):
				# Flatten any list values we find
				for v in data.values():
					if isinstance(v, list):
						items.extend(v)
			elif isinstance(data, list):
				items.extend(data)

		link = r.headers.get("Link", "")
		if 'rel="next"' not in link:
			break
		next_url = None
		for part in link.split(","):
			if 'rel="next"' in part:
				s = part.find("<") + 1
				e = part.find(">")
				if s > 0 and e > s:
					next_url = part[s:e]
				break
		if not next_url:
			break
		url = next_url
		params = None
	return items

# ------------------------------------------------------------------------------
# Notion helpers (Data Source API)
# ------------------------------------------------------------------------------
NOTION_BASE = "https://api.notion.com"
NOTION_VERSION = "2023-08-01"

def _notion_headers() -> Dict[str, str]:
	return {
		"Authorization": f"Bearer {NOTION_API_KEY}",
		"Notion-Version": NOTION_VERSION,
		"Content-Type": "application/json",
	}

def notion_query_by_filters(staging_url: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
	"""
	Lightweight query to find pages matching exact-text properties.
	filters = {"SKU": "ABC", "Location ID": "123"}
	Note: This uses a simple text equality filter approach.
	"""
	url = f"{NOTION_BASE}/v1/data-sources/query"
	payload = {
		"dataSourceUrl": staging_url,
		"filter": {
			"operator": "and",
			"filters": [
				{
					"property": k,
					"filter": {"operator": "text_is", "value": v}
				}
				for k, v in filters.items()
			],
		},
		"page_size": 1
	}
	r = requests.post(url, headers=_notion_headers(), json=payload, timeout=30)
	r.raise_for_status()
	data = r.json()
	return data.get("results", [])

def notion_create(staging_url: str, props: Dict[str, Any]) -> None:
	url = f"{NOTION_BASE}/v1/pages"
	payload = {
		"parentDataSourceUrl": staging_url,
		"properties": props,
	}
	r = requests.post(url, headers=_notion_headers(), json=payload, timeout=30)
	r.raise_for_status()

def notion_update(page_url: str, props: Dict[str, Any]) -> None:
	url = f"{NOTION_BASE}/v1/pages/update"
	payload = {
		"pageUrl": page_url,
		"properties": props,
	}
	r = requests.post(url, headers=_notion_headers(), json=payload, timeout=30)
	r.raise_for_status()

def build_props(row: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Maps a logical row dict to Notion Data Source properties.
	Data Source schema:
	- SKU (text)
	- Shopify Variant ID (text)
	- Shopify Product ID (text)
	- Available (number)
	- Location ID (text)
	- Location Name (text)
	- Updated At (date with time)
	- Notes (text)
	- Name (title) [optional to set]
	"""
	def to_float(x):
		try:
			return float(x)
		except Exception:
			return None

	props: Dict[str, Any] = {
		"SKU": str(row.get("SKU", "")),
		"Shopify Variant ID": str(row.get("Shopify Variant ID", "")),
		"Shopify Product ID": str(row.get("Shopify Product ID", "")),
		"Available": to_float(row.get("Available")),
		"Location ID": str(row.get("Location ID", "")),
		"Location Name": str(row.get("Location Name", "")),
		"date:Updated At:start": str(row.get("Updated At", "")),
		"date:Updated At:is_datetime": 1,
		"Notes": str(row.get("Notes", "")),
	}

	# Optional title for readability in views
	title = f"{props['SKU']} @ {props['Location ID']}".strip()
	if title:
		props["Name"] = title

	return props

def notion_upsert(staging_url: str, row: Dict[str, Any]) -> None:
	# Build lookup filters from UPSERT_KEY_FIELDS
	filters = {k: str(row.get(k, "")) for k in UPSERT_KEY_FIELDS}
	# If any key is missing, fall back to create
	if any(v == "" for v in filters.values()):
		props = build_props(row)
		notion_create(staging_url, props)
		return

	existing = notion_query_by_filters(staging_url, filters)
	props = build_props(row)

	if existing:
		page_url = existing[0].get("url")
		if page_url:
			notion_update(page_url, props)
		else:
			# No URL returned, create instead
			notion_create(staging_url, props)
	else:
		notion_create(staging_url, props)

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main() -> None:
	# Collect variant metadata so we can map inventory_item_id → variant details
	variants = shopify_get_all("variants.json")
	variant_by_inventory_item: Dict[int, Dict[str, Any]] = {}
	product_id_by_variant: Dict[int, int] = {}
	for v in variants:
		inv_item_id = v.get("inventory_item_id")
		if inv_item_id is not None:
			variant_by_inventory_item[inv_item_id] = v
		if "id" in v:
			product_id_by_variant[v["id"]] = v.get("product_id")

	levels = shopify_get_all("inventory_levels.json")
	now_iso = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

	# Also stream CSV to stdout for optional auditing
	out = csv.writer(sys.stdout)
	out.writerow([
		"SKU",
		"Shopify Variant ID",
		"Shopify Product ID",
		"Available",
		"Location ID",
		"Location Name",
		"Updated At",
	])

	for lvl in levels:
		inv_item_id = lvl.get("inventory_item_id")
		available = lvl.get("available")
		location_id = lvl.get("location_id")
		location_name = ""  # Optional: call /locations.json to fill names

		v = variant_by_inventory_item.get(inv_item_id)
		if v:
			sku = (v.get("sku") or "").strip()
			variant_id = str(v.get("id") or "")
			product_id = str(product_id_by_variant.get(v.get("id")) or "")
		else:
			sku = ""
			variant_id = ""
			product_id = ""

		row = {
			"SKU": sku,
			"Shopify Variant ID": variant_id,
			"Shopify Product ID": product_id,
			"Available": available,
			"Location ID": str(location_id or ""),
			"Location Name": location_name,
			"Updated At": now_iso,
		}

		# Print CSV for logs
		out.writerow([
			row["SKU"],
			row["Shopify Variant ID"],
			row["Shopify Product ID"],
			row["Available"],
			row["Location ID"],
			row["Location Name"],
			row["Updated At"],
		])

		# Upsert to Notion Staging
		notion_upsert(STAGING_URL, row)

if __name__ == "__main__":
	main()
