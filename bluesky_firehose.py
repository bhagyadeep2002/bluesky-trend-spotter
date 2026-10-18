#!/usr/bin/env python3
"""Print live Bluesky post events from the public Jetstream WebSocket.

Install the one dependency with: python -m pip install websockets
Run with:                         python bluesky_firehose.py
Show other record types too:      python bluesky_firehose.py --all

Jetstream is a beginner-friendly JSON view of Bluesky's lower-level AT Protocol
firehose. This example listens live; it does not download historical posts.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import textwrap
from datetime import datetime, timezone
from urllib.parse import urlencode

import websockets


JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"
POST_COLLECTION = "app.bsky.feed.post"


def make_url(posts_only: bool) -> str:
    """Build the WebSocket URL, optionally asking Jetstream for posts only."""
    query = ""
    if posts_only:
        query = "?" + urlencode({"wantedCollections": POST_COLLECTION})
    return JETSTREAM_URL + query


def event_time(event: dict) -> str:
    """Format Jetstream's Unix-microseconds timestamp for display."""
    time_us = event.get("time_us")
    if time_us is None:
        return "time unknown"
    timestamp = datetime.fromtimestamp(time_us / 1_000_000, tz=timezone.utc)
    return timestamp.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def print_event(event: dict) -> None:
    """Show the useful parts of an event without hiding its record type."""
    kind = event.get("kind", "unknown")
    did = event.get("did", "unknown DID")
    commit = event.get("commit", {})

    if kind == "commit":
        operation = commit.get("operation", "change")
        collection = commit.get("collection", "unknown collection")
        print(f"\n[{event_time(event)}] {operation} {collection}")
        print(f"  repo: {did}")

        record = commit.get("record", {})
        if collection == POST_COLLECTION and isinstance(record, dict):
            post_text = record.get("text", "")
            if post_text:
                wrapped = textwrap.fill(
                    post_text,
                    width=88,
                    initial_indent="  text: ",
                    subsequent_indent="        ",
                )
                print(wrapped)
            else:
                print("  (post has no text, or this is a delete event)")
        elif operation == "delete":
            print(f"  record key: {commit.get('rkey', 'unknown')}")
        else:
            print(f"  record: {json.dumps(record, ensure_ascii=False)}")
    else:
        # Account and identity notices can appear even with a post collection
        # filter, so print their kind and payload rather than silently dropping them.
        payload = event.get(kind, {})
        print(f"\n[{event_time(event)}] {kind} event from {did}")
        print(f"  {json.dumps(payload, ensure_ascii=False)}")


async def consume(posts_only: bool, limit: int) -> None:
    url = make_url(posts_only)
    print(f"Connecting to {JETSTREAM_URL}")
    print("Watching posts. Press Ctrl+C to stop." if posts_only else
          "Watching all collections. Press Ctrl+C to stop.")
    print("The WebSocket stays open and sends one JSON event at a time.")

    event_count = 0
    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as socket:
            async for message in socket:
                try:
                    event = json.loads(message)
                except json.JSONDecodeError:
                    print(f"Received a message that was not JSON: {message!r}")
                    continue

                print_event(event)
                event_count += 1
                if limit and event_count >= limit:
                    print(f"\nReached the {limit}-event limit.")
                    break
    except websockets.ConnectionClosed as error:
        print(f"The stream closed: {error}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--all",
        action="store_true",
        help="receive all record collections instead of just posts",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="stop after this many events; use 0 to keep listening (default: 20)",
    )
    args = parser.parse_args()

    if args.limit < 0:
        parser.error("--limit must be zero or greater")

    try:
        asyncio.run(consume(posts_only=not args.all, limit=args.limit))
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
