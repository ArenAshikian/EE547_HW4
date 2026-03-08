import argparse
import json
import sys
import time

import boto3
from boto3.dynamodb.conditions import Key


TABLE_NAME = "arxiv-papers"


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    recent = subparsers.add_parser("recent")
    recent.add_argument("category")
    recent.add_argument("--limit", type=int, default=20)
    recent.add_argument("--table", default=TABLE_NAME)

    author = subparsers.add_parser("author")
    author.add_argument("author_name")
    author.add_argument("--table", default=TABLE_NAME)

    get_cmd = subparsers.add_parser("get")
    get_cmd.add_argument("arxiv_id")
    get_cmd.add_argument("--table", default=TABLE_NAME)

    dr = subparsers.add_parser("daterange")
    dr.add_argument("category")
    dr.add_argument("start_date")
    dr.add_argument("end_date")
    dr.add_argument("--table", default=TABLE_NAME)

    kw = subparsers.add_parser("keyword")
    kw.add_argument("keyword")
    kw.add_argument("--limit", type=int, default=20)
    kw.add_argument("--table", default=TABLE_NAME)

    return parser.parse_args()


def clean_item(item):
    return {
        "arxiv_id": item.get("arxiv_id"),
        "title": item.get("title"),
        "authors": item.get("authors", []),
        "published": item.get("published"),
        "categories": item.get("categories", []),
        "abstract": item.get("abstract"),
        "keywords": item.get("keywords", []),
    }


def main():
    args = parse_args()
    db = boto3.resource("dynamodb")
    table = db.Table(args.table)

    start_time = time.perf_counter()
    results = []
    query_info = {}

    try:
        if args.command == "recent":
            query_info = {
                "query_type": "recent_in_category",
                "parameters": {
                    "category": args.category,
                    "limit": args.limit
                }
            }
            resp = table.query(
                KeyConditionExpression=Key("PK").eq(f"CATEGORY#{args.category}"),
                ScanIndexForward=False,
                Limit=args.limit
            )
            results = resp.get("Items", [])

        elif args.command == "author":
            query_info = {
                "query_type": "papers_by_author",
                "parameters": {
                    "author_name": args.author_name
                }
            }
            resp = table.query(
                IndexName="AuthorIndex",
                KeyConditionExpression=Key("GSI1PK").eq(f"AUTHOR#{args.author_name}"),
                ScanIndexForward=False
            )
            results = resp.get("Items", [])

        elif args.command == "get":
            query_info = {
                "query_type": "get_paper_by_id",
                "parameters": {
                    "arxiv_id": args.arxiv_id
                }
            }
            resp = table.query(
                IndexName="PaperIdIndex",
                KeyConditionExpression=(
                    Key("GSI2PK").eq(f"PAPER#{args.arxiv_id}") &
                    Key("GSI2SK").eq("META")
                )
            )
            results = resp.get("Items", [])

        elif args.command == "daterange":
            query_info = {
                "query_type": "papers_in_date_range",
                "parameters": {
                    "category": args.category,
                    "start_date": args.start_date,
                    "end_date": args.end_date
                }
            }
            s_key = f"{args.start_date}T00:00:00Z"
            e_key = f"{args.end_date}T23:59:59Z#zzzzzz"
            resp = table.query(
                KeyConditionExpression=(
                    Key("PK").eq(f"CATEGORY#{args.category}") &
                    Key("SK").between(s_key, e_key)
                )
            )
            results = resp.get("Items", [])

        elif args.command == "keyword":
            kw_lower = args.keyword.lower()
            query_info = {
                "query_type": "papers_by_keyword",
                "parameters": {
                    "keyword": kw_lower,
                    "limit": args.limit
                }
            }
            resp = table.query(
                IndexName="KeywordIndex",
                KeyConditionExpression=Key("GSI3PK").eq(f"KEYWORD#{kw_lower}"),
                ScanIndexForward=False,
                Limit=args.limit
            )
            results = resp.get("Items", [])

        elapsed = int((time.perf_counter() - start_time) * 1000)
        final_output = {
            **query_info,
            "results": [clean_item(i) for i in results],
            "count": len(results),
            "execution_time_ms": elapsed
        }
        print(json.dumps(final_output, indent=2))

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()