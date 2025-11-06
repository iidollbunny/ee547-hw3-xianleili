#!/usr/bin/env python3
"""
EE547 HW3 — Problem 2: Query implementations

Commands:
  python query_papers.py recent <category> [--limit 20] [--table TABLE]
  python query_papers.py author <author_name> [--table TABLE]
  python query_papers.py get <arxiv_id> [--table TABLE]
  python query_papers.py daterange <category> <start_date> <end_date> [--table TABLE]
  python query_papers.py keyword <keyword> [--limit 20] [--table TABLE]

Outputs JSON to stdout.
"""
import sys
import json
import time
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key

DEFAULT_TABLE = 'arxiv-papers'


def _dynamodb():
    return boto3.resource('dynamodb')


def _table(name):
    return _dynamodb().Table(name)


def query_recent_in_category(table_name, category, limit=20):
    t = _table(table_name)
    resp = t.query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,
        Limit=int(limit)
    )
    return resp.get('Items', [])



def query_papers_by_author(table_name, author_name):
    t = _table(table_name)
    resp = t.query(
        IndexName='AuthorIndex',
        KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
    )
    return resp.get('Items', [])


def get_paper_by_id(table_name, arxiv_id):
    t = _table(table_name)
    resp = t.query(
        IndexName='PaperIdIndex',
        KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}')
    )
    items = resp.get('Items', [])
    return items[0] if items else None


def query_papers_in_date_range(table_name, category, start_date, end_date):
    t = _table(table_name)
    resp = t.query(
        KeyConditionExpression=(
            Key('PK').eq(f'CATEGORY#{category}') &
            Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
        )
    )
    return resp.get('Items', [])


def query_papers_by_keyword(table_name, keyword, limit=20):
    t = _table(table_name)
    resp = t.query(
        IndexName='KeywordIndex',
        KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=int(limit)
    )
    return resp.get('Items', [])


def printer(payload):
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main():
    if len(sys.argv) < 2:
        print("Usage: python query_papers.py <command> ...", file=sys.stderr)
        sys.exit(2)

    cmd = sys.argv[1]
    table = DEFAULT_TABLE

    # Simple arg parsing
    args = sys.argv[2:]
    def get_flag(name, default=None):
        if name in args:
            i = args.index(name)
            if i + 1 < len(args):
                return args[i+1]
        return default

    t0 = time.time()
    try:
        if cmd == 'recent':
            if len(args) < 1:
                raise ValueError('recent <category> [--limit N] [--table T]')
            category = args[0]
            limit = int(get_flag('--limit', 20))
            table = get_flag('--table', table) or table
            items = query_recent_in_category(table, category, limit)
            payload = {
                "query_type": "recent_in_category",
                "parameters": {"category": category, "limit": limit},
                "results": [
                    {
                        "arxiv_id": it.get("arxiv_id"),
                        "title": it.get("title"),
                        "authors": it.get("authors", []),
                        "published": it.get("published"),
                        "categories": it.get("categories", [])
                    } for it in items
                ],
                "count": len(items),
            }
        elif cmd == 'author':
            if len(args) < 1:
                raise ValueError('author <author_name> [--table T]')
            author = args[0]
            table = get_flag('--table', table) or table
            items = query_papers_by_author(table, author)
            payload = {
                "query_type": "papers_by_author",
                "parameters": {"author": author},
                "results": items,
                "count": len(items),
            }
        elif cmd == 'get':
            if len(args) < 1:
                raise ValueError('get <arxiv_id> [--table T]')
            arxiv_id = args[0]
            table = get_flag('--table', table) or table
            item = get_paper_by_id(table, arxiv_id)
            payload = {
                "query_type": "get_by_id",
                "parameters": {"arxiv_id": arxiv_id},
                "result": item,
                "found": bool(item),
            }
        elif cmd == 'daterange':
            if len(args) < 3:
                raise ValueError('daterange <category> <start_date> <end_date> [--table T]')
            category, start_date, end_date = args[0], args[1], args[2]
            table = get_flag('--table', table) or table
            items = query_papers_in_date_range(table, category, start_date, end_date)
            payload = {
                "query_type": "date_range_in_category",
                "parameters": {"category": category, "start_date": start_date, "end_date": end_date},
                "results": items,
                "count": len(items),
            }
        elif cmd == 'keyword':
            if len(args) < 1:
                raise ValueError('keyword <keyword> [--limit N] [--table T]')
            kw = args[0]
            limit = int(get_flag('--limit', 20))
            table = get_flag('--table', table) or table
            items = query_papers_by_keyword(table, kw, limit)
            payload = {
                "query_type": "papers_by_keyword",
                "parameters": {"keyword": kw, "limit": limit},
                "results": items,
                "count": len(items),
            }
        else:
            raise ValueError(f"Unknown command: {cmd}")
    except Exception as e:
        payload = {"error": str(e)}
    finally:
        payload["execution_time_ms"] = int((time.time() - t0) * 1000)
        printer(payload)


if __name__ == '__main__':
    main()
