#!/usr/bin/env python3
"""
EE547 HW3 — Problem 2: Minimal HTTP API using http.server (no Flask/FastAPI)

Endpoints:
  GET /papers/recent?category=cs.LG&limit=5[&table=arxiv-papers]
  GET /papers/author/{author_name}[?table=arxiv-papers]
  GET /papers/{arxiv_id}[?table=arxiv-papers]
  GET /papers/search?category=cs.LG&start=YYYY-MM-DD&end=YYYY-MM-DD[&table=arxiv-papers]
  GET /papers/keyword/{keyword}?limit=20[&table=arxiv-papers]

Run:
  python api_server.py [port]
Notes:
  - Default port: 8080
  - Default table: arxiv-papers
  - Proper JSON responses with 400/404/500 status codes
  - Logs basic requests to stdout
"""

import sys
import json
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler

import boto3
from boto3.dynamodb.conditions import Key

DEFAULT_TABLE = "arxiv-papers"
DEFAULT_PORT = 8080


def _table(name: str):
    return boto3.resource("dynamodb").Table(name)


def json_response(handler: BaseHTTPRequestHandler, payload: dict, code: int = 200):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    server_version = "EE547HW3/1.1"

    # Log to stdout in a compact format
    def log_message(self, fmt, *args):
        print(
            "%s - - [%s] %s"
            % (self.client_address[0], self.log_date_time_string(), fmt % args)
        )

    def do_GET(self):
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            q = urllib.parse.parse_qs(parsed.query)

            # allow overriding table via querystring, default to DEFAULT_TABLE
            table = q.get("table", [DEFAULT_TABLE])[0]

            # Health check
            if path == "/health":
                return json_response(self, {"status": "ok"}, 200)

            # 1) Recent papers in category
            if path == "/papers/recent":
                category = q.get("category", [None])[0]
                if not category:
                    return json_response(self, {"error": "category is required"}, 400)
                try:
                    limit = int(q.get("limit", [20])[0])
                except ValueError:
                    return json_response(self, {"error": "limit must be integer"}, 400)

                # IMPORTANT: do NOT use begins_with('') on SK — it causes ValidationException
                # We just query the partition and sort descending, limiting N items
                items = (
                    _table(table)
                    .query(
                        KeyConditionExpression=Key("PK").eq(f"CATEGORY#{category}"),
                        ScanIndexForward=False,
                        Limit=limit,
                    )
                    .get("Items", [])
                )
                return json_response(
                    self,
                    {"category": category, "papers": items, "count": len(items)},
                    200,
                )

            # 2) Papers by author
            if path.startswith("/papers/author/"):
                author = urllib.parse.unquote(path.split("/papers/author/", 1)[1])
                if not author:
                    return json_response(self, {"error": "author is required"}, 400)
                items = (
                    _table(table)
                    .query(
                        IndexName="AuthorIndex",
                        KeyConditionExpression=Key("GSI1PK").eq(f"AUTHOR#{author}"),
                    )
                    .get("Items", [])
                )
                return json_response(
                    self, {"author": author, "papers": items, "count": len(items)}, 200
                )

            # 3) Papers by keyword
            if path.startswith("/papers/keyword/"):
                kw = urllib.parse.unquote(path.split("/papers/keyword/", 1)[1])
                if not kw:
                    return json_response(self, {"error": "keyword is required"}, 400)
                try:
                    limit = int(q.get("limit", [20])[0])
                except ValueError:
                    return json_response(self, {"error": "limit must be integer"}, 400)
                items = (
                    _table(table)
                    .query(
                        IndexName="KeywordIndex",
                        KeyConditionExpression=Key("GSI3PK").eq(
                            f"KEYWORD#{kw.lower()}"
                        ),
                        ScanIndexForward=False,
                        Limit=limit,
                    )
                    .get("Items", [])
                )
                return json_response(
                    self, {"keyword": kw, "papers": items, "count": len(items)}, 200
                )

            # 4) Papers in date range within a category
            if path == "/papers/search":
                category = q.get("category", [None])[0]
                start = q.get("start", [None])[0]
                end = q.get("end", [None])[0]
                if not (category and start and end):
                    return json_response(
                        self,
                        {"error": "category, start, end are required"},
                        400,
                    )
                # SK is "YYYY-MM-DD#<arxiv_id>", so we range on YYYY-MM-DD prefix
                items = (
                    _table(table)
                    .query(
                        KeyConditionExpression=Key("PK").eq(
                            f"CATEGORY#{category}"
                        ) & Key("SK").between(f"{start}#", f"{end}#zzzzzzz")
                    )
                    .get("Items", [])
                )
                return json_response(
                    self,
                    {
                        "category": category,
                        "start": start,
                        "end": end,
                        "papers": items,
                        "count": len(items),
                    },
                    200,
                )

            # 5) Get paper by arxiv_id
            #    Path shape: /papers/{arxiv_id}
            if path.startswith("/papers/") and path.count("/") == 2:
                arxiv_id = urllib.parse.unquote(path.split("/papers/", 1)[1])
                if not arxiv_id:
                    return json_response(self, {"error": "arxiv_id is required"}, 400)
                items = (
                    _table(table)
                    .query(
                        IndexName="PaperIdIndex",
                        KeyConditionExpression=Key("GSI2PK").eq(f"PAPER#{arxiv_id}"),
                    )
                    .get("Items", [])
                )
                if not items:
                    return json_response(self, {"error": "not found"}, 404)
                return json_response(self, items[0], 200)

            # No route matched
            return json_response(self, {"error": "Not Found"}, 404)

        except Exception as e:
            # Generic server error with message
            return json_response(self, {"error": str(e)}, 500)


def main():
    port = DEFAULT_PORT
    if len(sys.argv) >= 2:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass

    httpd = HTTPServer(("0.0.0.0", port), Handler)
    print(f"Listening on 0.0.0.0:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")


if __name__ == "__main__":
    main()
