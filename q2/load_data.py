import argparse
import json
import re
import sys
from collections import Counter

import boto3
from botocore.exceptions import ClientError
from stopwords import STOPWORDS


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("papers_json_path")
    parser.add_argument("table_name")
    parser.add_argument("--region", default="us-west-2")
    return parser.parse_args()


def load_data(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if "papers" in data:
            return data["papers"]
        if "results" in data:
            return data["results"]
    return []


def clean_paper(raw):
    pid = raw.get("arxiv_id") or raw.get("id") or raw.get("paper_id")
    if not pid:
        return None

    auths = raw.get("authors", [])
    if isinstance(auths, str):
        auths = [a.strip() for a in auths.split(",") if a.strip()]

    cats = raw.get("categories", [])
    if isinstance(cats, str):
        cats = [c.strip() for c in cats.split() if c.strip()]

    return {
        "arxiv_id": str(pid).strip(),
        "title": str(raw.get("title") or "").strip(),
        "abstract": str(raw.get("abstract") or raw.get("summary") or "").strip(),
        "authors": auths,
        "categories": cats,
        "published": str(raw.get("published") or "").strip(),
    }


def get_keywords(text, limit=10):
    words = re.findall(r"[a-zA-Z]{3,}", text.lower())
    filtered = [w for w in words if w not in STOPWORDS]
    return [word for word, _ in Counter(filtered).most_common(limit)]


def make_table(db, name):
    client = db.meta.client

    try:
        client.describe_table(TableName=name)
        return db.Table(name)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code != "ResourceNotFoundException":
            raise

    print(f"Creating DynamoDB table: {name}")
    table = db.create_table(
        TableName=name,
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
            {"AttributeName": "SK", "KeyType": "RANGE"}
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "AuthorIndex",
                "KeySchema": [
                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
            },
            {
                "IndexName": "PaperIdIndex",
                "KeySchema": [
                    {"AttributeName": "GSI2PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI2SK", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
            },
            {
                "IndexName": "KeywordIndex",
                "KeySchema": [
                    {"AttributeName": "GSI3PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI3SK", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
            },
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    )
    table.wait_until_exists()
    print("Creating GSIs: AuthorIndex, PaperIdIndex, KeywordIndex")
    return table


def main():
    args = parse_args()
    db = boto3.resource("dynamodb", region_name=args.region)
    table = make_table(db, args.table_name)

    raw_list = load_data(args.papers_json_path)
    final_items = []

    p_total = 0
    c_total = 0
    a_total = 0
    k_total = 0
    id_total = 0

    for raw in raw_list:
        p = clean_paper(raw)
        if not p or not p["published"]:
            continue

        p["keywords"] = get_keywords(p["abstract"])

        for c in p["categories"]:
            final_items.append({
                "PK": f"CATEGORY#{c}",
                "SK": f"{p['published']}#{p['arxiv_id']}",
                **p
            })
            c_total += 1

        for a in p["authors"]:
            final_items.append({
                "PK": f"PAPER#{p['arxiv_id']}",
                "SK": f"AUTHOR#{a}",
                "GSI1PK": f"AUTHOR#{a}",
                "GSI1SK": f"{p['published']}#{p['arxiv_id']}",
                **p
            })
            a_total += 1

        for k in p["keywords"]:
            final_items.append({
                "PK": f"PAPER#{p['arxiv_id']}",
                "SK": f"KEYWORD#{k}",
                "GSI3PK": f"KEYWORD#{k}",
                "GSI3SK": f"{p['published']}#{p['arxiv_id']}",
                **p
            })
            k_total += 1

        final_items.append({
            "PK": f"PAPER#{p['arxiv_id']}",
            "SK": "META",
            "GSI2PK": f"PAPER#{p['arxiv_id']}",
            "GSI2SK": "META",
            **p
        })
        id_total += 1
        p_total += 1

    with table.batch_writer() as batch:
        for item in final_items:
            batch.put_item(Item=item)

    print(f"Loaded {p_total} papers")
    print(f"Created {len(final_items)} DynamoDB items (denormalized)")
    print(f"Denormalization factor: {len(final_items) / p_total:.1f}x")
    print()
    print("Storage breakdown:")
    print(f"  - Category items: {c_total} ({c_total / p_total:.1f} per paper avg)")
    print(f"  - Author items: {a_total} ({a_total / p_total:.1f} per paper avg)")
    print(f"  - Keyword items: {k_total} ({k_total / p_total:.1f} per paper avg)")
    print(f"  - Paper ID items: {id_total} ({id_total / p_total:.1f} per paper)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)