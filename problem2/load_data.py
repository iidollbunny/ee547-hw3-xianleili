#!/usr/bin/env python3
"""
EE547 HW3 — Problem 2: Data Loader for DynamoDB (robust version)

Usage:
  python load_data.py <papers_json_path> <table_name> [--region REGION]

What this does:
- Creates the DynamoDB table + GSIs if missing (PAY_PER_REQUEST)
- Transforms HW#1 papers.json into denormalized items
- Extracts top-10 keywords from abstracts (excl. STOPWORDS)
- Ensures NO EMPTY STRINGS in key attributes (and avoids empty strings anywhere)
- Batch writes items
- Prints load statistics

Only dependencies: boto3, Python stdlib
"""
import sys
import json
import time
import re
from collections import Counter, defaultdict
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError, ParamValidationError

STOPWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
    'can', 'this', 'that', 'these', 'those', 'we', 'our', 'use', 'using',
    'based', 'approach', 'method', 'paper', 'propose', 'proposed', 'show'
}


# --------------------------
# Utilities
# --------------------------
def die(msg: str, code: int = 1):
    print(msg, file=sys.stderr)
    sys.exit(code)


def parse_args():
    if len(sys.argv) < 3:
        die("Usage: python load_data.py <papers_json_path> <table_name> [--region REGION]")
    papers_json = sys.argv[1]
    table_name = sys.argv[2]
    region = None
    if len(sys.argv) >= 5 and sys.argv[3] == "--region":
        region = sys.argv[4]
    return papers_json, table_name, region


def get_session(region):
    if region:
        return boto3.session.Session(region_name=region)
    return boto3.session.Session()


def ymd(date_str_iso: str) -> str:
    """Return YYYY-MM-DD from ISO8601 or '0000-00-00' if missing/invalid."""
    if not date_str_iso:
        return "0000-00-00"
    s = str(date_str_iso)
    if 'T' in s:
        s = s.split('T', 1)[0]
    # rudimentary sanity
    if len(s) >= 10:
        return s[:10]
    return "0000-00-00"


def tokenize_words(text: str) -> List[str]:
    tokens = re.findall(r"[a-zA-Z]+", (text or "").lower())
    return [t for t in tokens if t not in STOPWORDS and len(t) > 2]


def top_keywords_from_abstract(abstract: str, k: int = 10) -> List[str]:
    tokens = tokenize_words(abstract)
    if not tokens:
        return []
    counts = Counter(tokens)
    return [w for w, _ in counts.most_common(k)]


def compact(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove attributes that DynamoDB dislikes:
    - empty strings ""
    - None
    - empty lists/dicts
    """
    out = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, str) and v == "":
            continue
        if isinstance(v, (list, dict)) and len(v) == 0:
            continue
        out[k] = v
    return out


# --------------------------
# DynamoDB table management
# --------------------------
def ensure_table(dynamodb, table_name: str):
    client = dynamodb.meta.client
    try:
        existing = client.describe_table(TableName=table_name)
        status = existing['Table']['TableStatus']
        if status not in ("ACTIVE", "UPDATING"):
            print(f"Table {table_name} exists with status {status}; waiting...")
            waiter = client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
        print(f"Using existing table: {table_name}")
        return dynamodb.Table(table_name)
    except client.exceptions.ResourceNotFoundException:
        pass

    print(f"Creating DynamoDB table: {table_name}")
    table = dynamodb.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
            {"AttributeName": "GSI2PK", "AttributeType": "S"},
            {"AttributeName": "GSI2SK", "AttributeType": "S"},
            {"AttributeName": "GSI3PK", "AttributeType": "S"},
            {"AttributeName": "GSI3SK", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        BillingMode='PAY_PER_REQUEST',
        GlobalSecondaryIndexes=[
            {
                "IndexName": "AuthorIndex",
                "KeySchema": [
                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "PaperIdIndex",
                "KeySchema": [
                    {"AttributeName": "GSI2PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI2SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "KeywordIndex",
                "KeySchema": [
                    {"AttributeName": "GSI3PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI3SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    )
    table.wait_until_exists()
    print("Table created and ACTIVE")
    return table


# --------------------------
# Item builders (NO empty key attributes)
# --------------------------
def make_master_item(paper: Dict[str, Any], pub_date: str) -> Dict[str, Any]:
    # Keys must be non-empty strings
    arxiv_id = paper['arxiv_id']
    pk = f"PAPER#{arxiv_id}"
    sk = "MASTER"
    gsi2pk = f"PAPER#{arxiv_id}"
    gsi2sk = pub_date or "0000-00-00"

    item = {
        "PK": pk,
        "SK": sk,
        "GSI2PK": gsi2pk,
        "GSI2SK": gsi2sk,
        "item_type": "MASTER",
        **paper,
    }
    return compact(item)


def make_category_item(paper: Dict[str, Any], category: str, pub_date: str) -> Dict[str, Any]:
    pk = f"CATEGORY#{category}"
    sk = f"{pub_date or '0000-00-00'}#{paper['arxiv_id']}"

    item = {
        "PK": pk,
        "SK": sk,
        "item_type": "CATEGORY",
        **paper,
    }
    return compact(item)


def make_author_item(paper: Dict[str, Any], author: str, pub_date: str) -> Dict[str, Any]:
    if not author:
        # Do not emit author items with empty author names
        return None
    # Use a spread PK only to avoid hot partitions on main table (not queried by PK)
    pk = f"AUTHORITEM#{author}#{paper['arxiv_id']}"
    sk = f"{pub_date or '0000-00-00'}#{paper['arxiv_id']}"
    gsi1pk = f"AUTHOR#{author}"
    gsi1sk = sk

    item = {
        "PK": pk,
        "SK": sk,
        "GSI1PK": gsi1pk,
        "GSI1SK": gsi1sk,
        "item_type": "AUTHOR",
        **paper,
    }
    return compact(item)


def make_keyword_item(paper: Dict[str, Any], keyword: str, pub_date: str) -> Dict[str, Any]:
    if not keyword:
        return None
    kw = keyword.lower()
    pk = f"KEYWORDITEM#{kw}#{paper['arxiv_id']}"
    sk = f"{pub_date or '0000-00-00'}#{paper['arxiv_id']}"
    gsi3pk = f"KEYWORD#{kw}"
    gsi3sk = sk

    item = {
        "PK": pk,
        "SK": sk,
        "GSI3PK": gsi3pk,
        "GSI3SK": gsi3sk,
        "item_type": "KEYWORD",
        **paper,
    }
    return compact(item)


def batch_write(table, items: List[Dict[str, Any]]):
    # Write in chunks of 25; batch_writer retries unprocessed automatically
    with table.batch_writer(overwrite_by_pkeys=['PK', 'SK']) as writer:
        for it in items:
            if it is None:
                continue
            # Extra guard: ensure no empty key attributes
            if not it.get("PK") or not it.get("SK"):
                continue
            writer.put_item(Item=it)


# --------------------------
# Load & transform
# --------------------------
def load_json(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def transform_and_write(table, papers):
    total_papers = 0
    total_items = 0
    counts = defaultdict(int)

    for p in papers:
        # Normalize core fields
        arxiv_id = (p.get('arxiv_id') or p.get('id') or "").strip()
        if not arxiv_id:
            # Skip malformed paper (no ID → cannot build keys safely)
            continue

        title = (p.get('title') or "").strip()
        authors = [a for a in (p.get('authors') or []) if isinstance(a, str) and a.strip()]
        abstract = (p.get('abstract') or "").strip()
        categories = [c for c in (p.get('categories') or []) if isinstance(c, str) and c.strip()]

        published_raw = p.get('published') or p.get('date') or ""
        pub_date = ymd(published_raw)  # guaranteed non-empty fallback

        if not categories:
            # We need at least one category to emit category items; skip entirely if none
            continue

        paper_doc = {
            'arxiv_id': arxiv_id,
            'title': title,
            'authors': authors,
            'abstract': abstract,
            'categories': categories,
            'keywords': top_keywords_from_abstract(abstract),
            # Keep original timestamp if present, else synthesize a midnight UTC ISO
            'published': p.get('published') or (f"{pub_date}T00:00:00Z"),
        }
        # Remove any empty strings/lists to satisfy DynamoDB constraints
        paper = compact(paper_doc)

        items = []

        # 1) MASTER item (for PaperIdIndex)
        items.append(make_master_item(paper, pub_date))
        counts['master'] += 1

        # 2) CATEGORY items (for recent & date range)
        for c in categories:
            it = make_category_item(paper, c, pub_date)
            if it:
                items.append(it)
                counts['category'] += 1

        # 3) AUTHOR items (for AuthorIndex)
        for a in authors:
            it = make_author_item(paper, a, pub_date)
            if it:
                items.append(it)
                counts['author'] += 1

        # 4) KEYWORD items (for KeywordIndex)
        for kw in paper.get('keywords', []):
            it = make_keyword_item(paper, kw, pub_date)
            if it:
                items.append(it)
                counts['keyword'] += 1

        # Batch write
        batch_write(table, items)
        total_papers += 1
        total_items += len(items)

    return total_papers, total_items, counts


# --------------------------
# Main
# --------------------------
def main():
    papers_json, table_name, region = parse_args()
    sess = get_session(region)
    dynamodb = sess.resource('dynamodb')

    table = ensure_table(dynamodb, table_name)

    print(f"Loading papers from {papers_json}...")
    papers = load_json(papers_json)
    if isinstance(papers, dict) and 'papers' in papers:
        papers = papers['papers']

    t0 = time.time()
    total_papers, total_items, counts = transform_and_write(table, papers)
    dt = time.time() - t0

    denorm = (total_items / total_papers) if total_papers else 0.0

    print(f"Loaded {total_papers} papers")
    print(f"Created {total_items} DynamoDB items (denormalized)")
    print(f"Denormalization factor: {denorm:.1f}x")

    def avg(x):
        return (counts[x] / total_papers) if total_papers else 0

    print("\nStorage breakdown:")
    print(f"  - Category items: {counts['category']} ({avg('category'):.1f} per paper avg)")
    print(f"  - Author items:   {counts['author']} ({avg('author'):.1f} per paper avg)")
    print(f"  - Keyword items:  {counts['keyword']} ({avg('keyword'):.1f} per paper avg)")
    print(f"  - Paper ID items: {counts['master']} ({avg('master'):.1f} per paper)")

    print(f"\nCompleted in {dt*1000:.0f} ms")


if __name__ == "__main__":
    main()
