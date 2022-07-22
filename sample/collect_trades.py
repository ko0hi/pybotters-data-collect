import asyncio
import time
from typing import Any, Dict

import loguru
import os
import motor.motor_asyncio
import pybotters

from datetime import datetime
from pybotters_data_collect import MongoHandler


def bybit(msg, ws):
    if "topic" in msg:
        topic = msg["topic"]
        if topic.startswith("trade"):
            return msg["data"]


def binance(msg, ws):
    if "e" in msg and msg["e"] == "aggTrade":
        return [msg]


def ftx(msg, ws):
    if "channel" in msg and msg["channel"] == "trades" and "data" in msg:
        return msg["data"]


def gmo(msg, ws):
    if "channel" in msg and msg["channel"] == "trades":
        return [msg]


def phemex(msg, ws):
    if "trades" in msg and msg["type"] == "incremental":
        data = []
        for d in msg["trades"]:
            item = {"timestamp": d[0], "side": d[1], "price": d[2], "volume": d[3]}
            data.append(item)
        return data


def bitget(msg, ws):
    if "action" in msg and msg["action"] == "update" and "data" in msg:
        data = []
        for d in msg["data"]:
            data.append(
                {
                    "timestamp": d[0],
                    "price": float(d[1]),
                    "size": float(d[2]),
                    "side": d[3],
                }
            )
        return data


def okx(msg, ws):
    if "arg" in msg and msg["arg"]["channel"] == "trades" and "data" in msg:
        return msg["data"]


def bitflyer(msg, ws):
    if "params" in msg:
        params = msg["params"]
        if params["channel"] == "lightning_executions_FX_BTC_JPY":
            return params["message"]


def coincheck(msg, ws):
    if isinstance(msg, list):
        return [
            {
                "id": msg[0],
                "symbol": msg[1],
                "price": float(msg[2]),
                "size": float(msg[3]),
                "side": msg[4],
                "timestamp": datetime.utcnow(),
            }
        ]


async def main(exchange):
    os.makedirs("log", exist_ok=True)

    logger = loguru.logger
    logger.add(f"log/{exchange}.log", retention=3, rotation="10MB")

    async with pybotters.Client() as client:
        if exchange == "bybit":
            wstask = await client.ws_connect(
                "wss://stream.bybit.com/realtime",
                send_json={"op": "subscribe", "args": ["trade.BTCUSD"]},
                hdlr_json=MongoHandler(
                    bybit, db_name="bybit", collection_name="trades", logger=logger
                ),
            )
        elif exchange == "binance":
            wstask = await client.ws_connect(
                "wss://fstream.binance.com/ws",
                send_json={
                    "method": "SUBSCRIBE",
                    "params": ["btcusdt@aggTrade"],
                    "id": 1,
                },
                hdlr_json=MongoHandler(
                    binance, db_name="binance", collection_name="trades", logger=logger
                ),
            )
        elif exchange == "ftx":
            wstask = await client.ws_connect(
                "wss://ftx.com/ws",
                send_json={
                    "op": "subscribe",
                    "channel": "trades",
                    "market": "BTC-PERP",
                },
                hdlr_json=MongoHandler(
                    ftx, db_name="ftx", collection_name="trades", logger=logger
                ),
            )
        elif exchange == "gmo":
            wstask = await client.ws_connect(
                "wss://api.coin.z.com/ws/public/v1",
                send_json={
                    "command": "subscribe",
                    "channel": "trades",
                    "symbol": "BTC_JPY",
                    "option": "TAKER_ONLY",
                },
                hdlr_json=MongoHandler(
                    gmo, db_name="gmo", collection_name="trades", logger=logger
                ),
            )
        elif exchange == "phemex":
            wstask = await client.ws_connect(
                "wss://phemex.com/ws",
                send_json={"id": 1, "method": "trade.subscribe", "params": ["BTCUSD"]},
                hdlr_json=MongoHandler(
                    phemex, db_name="phemex", collection_name="trades", logger=logger
                ),
            )
        elif exchange == "bitget":
            wstask = await client.ws_connect(
                "wss://ws.bitget.com/mix/v1/stream",
                send_json={
                    "op": "subscribe",
                    "args": [
                        {"instType": "mc", "channel": "trade", "instId": "BTCUSD"}
                    ],
                },
                hdlr_json=MongoHandler(
                    bitget, db_name="bitget", collection_name="trades", logger=logger
                ),
            )
        elif exchange == "okx":
            wstask = await client.ws_connect(
                "wss://ws.okx.com:8443/ws/v5/public",
                send_json={
                    "op": "subscribe",
                    "args": [{"channel": "trades", "instId": "BTC-USD-SWAP"}],
                },
                hdlr_json=MongoHandler(
                    okx, db_name="okx", collection_name="trades", logger=logger
                ),
            )
        elif exchange == "bitflyer":
            wstask = await client.ws_connect(
                "wss://ws.lightstream.bitflyer.com/json-rpc",
                send_json=[
                    {
                        "method": "subscribe",
                        "params": {"channel": "lightning_executions_FX_BTC_JPY"},
                        "id": 1,
                    },
                ],
                hdlr_json=MongoHandler(
                    bitflyer,
                    db_name="bitflyer",
                    collection_name="trades",
                    logger=logger,
                ),
            )
        elif exchange == "bitflyer.board":
            # 超適当な実装
            class A:
                LAST_INSERT = time.monotonic()
                STORE = pybotters.bitFlyerDataStore()

                @classmethod
                def onmessage(cls, msg, ws):
                    cls.STORE.onmessage(msg, ws)

                    if time.monotonic() - cls.LAST_INSERT > 0.5:
                        board = cls.STORE.board.sorted()
                        if len(board["BUY"]) == 0:
                            return None

                        for k, v in board.items():
                            board[k] = [
                                {k2: v2 for (k2, v2) in item.items() if k2 in ("price", "size")}
                                for item in v
                            ]

                        board["timestamp"] = str(datetime.utcnow())
                        board["mid"] = cls.STORE.board.mid_price
                        cls.LAST_INSERT = time.monotonic()
                        return [board]
                    else:
                        return None

            mongo_hdlr = MongoHandler(
                A.onmessage, db_name="bitflyer", collection_name="board", logger=logger
            )

            wstask = await client.ws_connect(
                "wss://ws.lightstream.bitflyer.com/json-rpc",
                send_json=[
                    {
                        "method": "subscribe",
                        "params": {"channel": "lightning_board_snapshot_FX_BTC_JPY"},
                        "id": 1,
                    },
                    {
                        "method": "subscribe",
                        "params": {"channel": "lightning_board_FX_BTC_JPY"},
                        "id": 2,
                    },
                ],
                hdlr_json=mongo_hdlr,
            )
        elif exchange == "coincheck":
            wstask = await client.ws_connect(
                "wss://ws-api.coincheck.com/",
                send_json=[{"type": "subscribe", "channel": "btc_jpy-trades"}],
                hdlr_json=MongoHandler(
                    coincheck,
                    db_name="coincheck",
                    collection_name="trades",
                    logger=logger,
                ),
            )

        else:
            raise RuntimeError(f"Unsupported: {exchange}")

        while True:
            await asyncio.sleep(1)


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "--exchange",
        choices=[
            "bybit",
            "binance",
            "ftx",
            "phemex",
            "gmo",
            "bitget",
            "okx",
            "bitflyer",
            "bitflyer.board",
            "coincheck",
        ],
        required=True,
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.exchange))
    except KeyboardInterrupt:
        pass
