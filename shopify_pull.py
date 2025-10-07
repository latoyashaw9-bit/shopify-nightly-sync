import os
import requests
import datetime
from urllib.parse import urljoin

STORE = os.environ["SHOPIFY_STORE_DOMAIN"]
TOKEN = os.environ["SHOPIFY_ADMIN_API_TOKEN"]
BASE = f"https://{STORE}/admin/api/2024-10/"
HEADERS = {"X-Shopify-Access-Token": TOKEN}

print("SKU,Shopify Variant ID,Shopify Product ID,Available,Location ID,Location Name,Updated At")
now_iso = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def get_all(path, params=None):
    items = []
    url = urljoin(BASE, path)
    while True:
        r = requests.get(url, headers=HEADERS, params=params)
        r.raise_for_status()
        data = r.json()
        key = None
        for k in ("variants", "inventory_levels", "products"):
            if k in data:
                key = k
                break
        batch = data.get(key, [])
        items.extend(batch)
        link = r.headers.get("Link", "")
        if 'rel="next"' not in link:
            break
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<")+1:part.find(">")]
                break
        if not next_url:
            break
        url = next_url
        params = None
    return items

# 1) Variants (for SKU and IDs)
variants = get_all("variants.json")
variant_by_inventory_item = {}
product_id_by_variant = {}
for v in variants:
    inv_item_id = v.get("inventory_item_id")
    if inv_item_id is not None:
        variant_by_inventory_item[inv_item_id] = v
    product_id_by_variant[v.get("id")] = v.get("product_id")

# 2) Inventory levels per location
levels = get_all("inventory_levels.json")

for lvl in levels:
    inv_item_id = lvl.get("inventory_item_id")
    available = lvl.get("available")
    location_id = lvl.get("location_id")
    location_name = ""  # optional: resolve via /locations.json

    v = variant_by_inventory_item.get(inv_item_id)
    if v:
        sku = (v.get("sku") or "").strip()
        variant_id = str(v.get("id") or "")
        product_id = str(product_id_by_variant.get(v.get("id")) or "")
    else:
        sku = ""
        variant_id = ""
        product_id = ""

    print(f"{sku},{variant_id},{product_id},{available},{location_id},{location_name},{now_iso}")
