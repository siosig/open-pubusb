# open-pubusb Python Sample

> Japanese: [README.ja.md](README.ja.md)

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Usage](#usage)
- [What the Sample Does](#what-the-sample-does)

## Overview

A minimal sample that uses the official Google Cloud client (`google-cloud-pubsub`), pointed at a local
`open-pubusb` solely via `PUBSUB_EMULATOR_HOST`, to create and delete a Topic / Subscription and to
publish (push) and receive-and-ack (pull & ack) messages.

## Setup

```bash
pip install -r sample/python/requirements.txt
```

Start `open-pubusb` beforehand (in another terminal, from the repository root):

```bash
cargo run -p open-pubusb -- serve --ephemeral --listen 127.0.0.1:8085 --admin-listen 127.0.0.1:8086
```

## Usage

```bash
PUBSUB_EMULATOR_HOST=127.0.0.1:8085 python3 sample/python/pubsub_demo.py
```

## What the Sample Does

`pubsub_demo.py` performs the following steps in order.

1. Create a topic (`create_topic`)
2. Create a subscription (`create_subscription`)
3. Publish a message (`push_message`)
4. Receive and ack the message (`pull_and_ack`) — then pull again to confirm the acked message is not redelivered
5. Delete the subscription (`delete_subscription`)
6. Delete the topic (`delete_topic`)
