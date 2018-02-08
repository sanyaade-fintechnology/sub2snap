import zmq
import zmq.asyncio
import asyncio
from asyncio import ensure_future as create_task
import sys
import copy
import logging
import json
import argparse
from copy import deepcopy
from zmapi.codes import error
from zmapi.zmq import SockRecvPublisher
from zmapi.zmq.utils import ident_to_str, split_message
from zmapi.logging import setup_root_logger
from zmapi.exceptions import *
from zmapi.utils import update_dict
from zmapi import SubscriptionDefinition
import uuid
from time import time, gmtime
from datetime import datetime
from pprint import pprint, pformat
from collections import defaultdict

################################## CONSTANTS ##################################

MODULE_NAME = "sub2snap"
COLLECTOR_TIMEOUT = 10 * 1000

################################### GLOBALS ###################################

L = logging.root

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.num_snapshots_emulated = 0
g.running_collectors = set()
g.subscriptions = defaultdict(SubscriptionDefinition)

############################### COLLECTOR LOGIC ###############################

def update_book(res_book, msg_book):
    if not msg_book:
        return
    for lvl in msg_book:
        # float key is ok without any arithmetic operations?
        if g.cap_pub_order_book_positions:
            key = lvl["position"]
        else:
            key = lvl["price"]
        res_book[key] = lvl

def collect_depth(res, msg, ob_levels):
    ob = res["order_book"]
    update_book(ob["bids"], msg.get("bids"))
    update_book(ob["asks"], msg.get("asks"))
    update_book(ob["implied_bids"], msg.get("implied_bids"))
    update_book(ob["implied_asks"], msg.get("implied_asks"))

def collect_trade(res, msg):
    res["last_price"] = msg["price"]
    res["last_size"] = msg["size"]

def collect_quote(res, msg):
    update_dict(res, msg)

def check_req_fields(res, req_fields):
    for k, sub_fields in req_fields.items():
        if k not in res:
            # L.debug("{} still missing".format(k))
            return False
        if sub_fields:
            if not check_req_fields(res[k], sub_fields):
                return False
    return True

def check_collection_finished(res, ob_levels, req_quotes):
    if ob_levels > 0:
        ob = res["order_book"]
        if not g.cap_pub_order_book_positions:
            # Best estimate of the required order book is fetched by just
            # waiting long enough time.
            return False
        if len(ob["bids"]) < ob_levels:
            return False
        if len(ob["asks"]) < ob_levels:
            return False
        req_pos = set(range(ob_levels))
        if req_pos.difference(set(x["position"] for x in ob["bids"].values())):
            return False
        if req_pos.difference(set(x["position"] for x in ob["asks"].values())):
            return False
        # TODO: implied books need to be handled in some specific way?
    if "last_price" not in res:
        return False
    if "last_size" not in res:
        return False
    return check_req_fields(res, req_quotes)

async def collect_data(sock, ticker_id, ob_levels, daily, timeout):
    tic = time()
    poller = zmq.asyncio.Poller()
    poller.register(sock, zmq.POLLIN)
    res = {}
    res["order_book"] = ob = {}
    ob["bids"] = {}
    ob["asks"] = {}
    ob["implied_bids"] = {}
    ob["implied_asks"] = {}
    req_quotes = copy.deepcopy(g.available_quotes)
    if not daily:
        req_quotes.pop("daily", None)
    timed_out = False
    while True:
        if timeout is None:
            tout = None
        else:
            tout = timeout - (time() - tic) * 1000
            # L.debug("ticker_id: {}, tout: {}".format(ticker_id, tout))
        p = await poller.poll(tout)
        if not p:
            timed_out = True
            break
        topic, msg = await sock.recv_multipart()
        msg = json.loads(msg.decode())
        # L.debug("{}: {}".format(topic, msg))
        msg_type = topic[-1]
        if msg_type == 1:
            collect_depth(res, msg, ob_levels)
        elif msg_type == 2:
            collect_trade(res, msg)
        elif msg_type == 3:
            collect_quote(res, msg)
        # L.debug(pformat(res))
        if check_collection_finished(res, ob_levels, req_quotes):
            break
    if not daily and "daily" in res:
        del res["daily"]
    # sort books
    if g.cap_pub_order_book_positions:
        sort_key = "position"
    else:
        sort_key = "price"
    ob["bids"] = sorted(ob["bids"].values(), key=lambda x: x[sort_key])
    ob["asks"] = sorted(ob["asks"].values(), key=lambda x: x[sort_key])
    ob["implied_bids"] = sorted(ob["implied_bids"].values(),
                                key=lambda x: x[sort_key])
    ob["implied_asks"] = sorted(ob["implied_asks"].values(), 
                                key=lambda x: x[sort_key])
    if not g.cap_pub_order_book_positions:
        ob["bids"] = ob["bids"][::-1]
        ob["implied_bids"] = ob["implied_bids"][::-1]
    # fill bid/ask from books if not filled yet
    if "bid_price" not in res and ob["bids"]:
        top_lvl = ob["bids"][0]
        if not g.cap_pub_order_book_positions or \
                (g.cap_pub_order_book_positions and top_lvl["position"] == 0):
            res["bid_price"] = top_lvl["price"]
            res["bid_size"] = top_lvl["size"]
    if "ask_price" not in res and ob["asks"]:
        top_lvl = ob["asks"][0]
        if not g.cap_pub_order_book_positions or \
                (g.cap_pub_order_book_positions and top_lvl["position"] == 0):
            res["ask_price"] = top_lvl["price"]
            res["ask_size"] = top_lvl["size"]
    # TODO: max/min bid/ask with implied
    if ob_levels == 0:
        del res["order_book"]
    else:
        # truncate order books
        ob["bids"] = ob["bids"][:ob_levels]
        ob["asks"] = ob["asks"][:ob_levels]
        ob["implied_bids"] = ob["implied_bids"][:ob_levels]
        ob["implied_asks"] = ob["implied_asks"][:ob_levels]
    # remove empty order books
    items = list(ob.items())
    for k, v in items:
        if v is None or (type(v) is list and not v):
            del ob[k]
    return res, timed_out

###############################################################################

async def fwd_message_no_change(msg_id, msg_parts):
    await g.sock_deal.send_multipart(msg_parts)
    res = await g.sock_deal_pub.poll_for_msg_id(msg_id)
    await g.sock_ctl.send_multipart(res)
    return res

async def send_error(ident, msg_id, ecode, what=None):
    if isinstance(what, Exception):
        what = "{}: {}".format(type(what).__name__, what)
    msg = error.gen_error(ecode, what)
    msg["msg_id"] = msg_id
    msg = " " + json.dumps(msg)
    msg = msg.encode()
    await g.sock_ctl.send_multipart(ident + [b"", msg])

async def handle_msg_2(ident, msg):
    cmd = msg.get("command")
    if not cmd:
        raise InvalidArguments("command missing")
    sanitize_msg(msg)
    debug_str = "ident={}, command={}, msg_id={}"
    debug_str = debug_str.format(ident_to_str(ident), cmd, msg["msg_id"])
    L.debug("> " + debug_str)
    msg_bytes = " " + json.dumps(msg)
    msg_bytes = msg_bytes.encode()
    await g.sock_deal.send_multipart(ident + [b"", msg_bytes])
    msg_parts = await g.sock_deal_pub.poll_for_msg_id(msg["msg_id"])
    if cmd == "get_status":
        msg = json.loads(msg_parts[-1].decode())
        status = {
            "name": MODULE_NAME,
            "uptime": (datetime.utcnow() - g.startup_time).total_seconds(),
            "num_messages_handled": g.num_messages_handled,
            "num_msg_ids_added": g.num_msg_ids_added,
            "num_invalid_messages": g.num_invalid_messages,
        }
        msg["content"].insert(0, status)
        msg_bytes = (" " + json.dumps(msg)).encode()
        msg_parts[-1] = msg_bytes
    L.debug("< " + debug_str)
    await g.sock_ctl.send_multipart(msg_parts)

async def handle_get_status(ident, msg, msg_raw):
    await g.sock_deal.send_multipart(ident + [b"", msg_raw])
    msg_parts = await g.sock_deal_pub.poll_for_msg_id(msg["msg_id"])
    msg = json.loads(msg_parts[-1].decode())
    content = msg["content"]
    status = {
        "name": MODULE_NAME,
        "uptime": (datetime.utcnow() - g.startup_time).total_seconds(),
        "num_snapshots_emulated": num_snapshots_emulated,
    }
    content = [status] + content
    msg["content"] = content
    msg_bytes = (" " + json.dumps(msg)).encode()
    await g.sock_ctl.send_multipart(ident + [b"", msg_bytes])

async def send_command(command, content=None, block=True):
    if content is None:
        content = {}
    msg = {"command": command, "content": content}
    msg["msg_id"] = msg_id = str(uuid.uuid4())
    msg_bytes = (" " + json.dumps(msg)).encode()
    await g.sock_deal.send_multipart([b"", msg_bytes])
    if block:
        return await g.sock_deal_pub.poll_for_msg_id(msg_id)
    return msg_id

async def modify_subscription_noblock(ticker_id, sub_def):
    content = deepcopy(sub_def.__dict__)
    content["ticker_id"] = ticker_id
    return await send_command("modify_subscription", content, False)

async def handle_get_snapshot_2(ident, msg, msg_raw):
    content = msg["content"]
    ticker_id = content["ticker_id"]
    sub_def = deepcopy(g.subscriptions[ticker_id])
    ob_levels = content.get("order_book_levels", 0)
    daily = content.get("daily_data", False)
    if ob_levels > 0:
        sub_def["order_book_levels"] = max(ob_levels, 
                                           sub_def["order_book_levels"])
        sub_def["order_book_speed"] = max(5, sub_def["order_book_speed"])
    sub_def["trades_speed"] = max(5, sub_def["trades_speed"])
    sub_def["emit_quotes"] = True
    # Connect and subscribe the socket before sending modify_subscription to
    # to avoid racing condition.
    sub_sock = g.ctx.socket(zmq.SUB)
    sub_sock.connect(g.pub_addr_up)
    sub_sock.subscribe(ticker_id.encode())
    # first unsubscribe to get all updates for snapshot ASAP
    await modify_subscription_noblock(ticker_id, SubscriptionDefinition())
    # set required subscription level for snapshot
    await modify_subscription_noblock(ticker_id, sub_def)
    res, timed_out = await collect_data(
            sub_sock, ticker_id, ob_levels, daily, COLLECTOR_TIMEOUT)
    # reset subscription back to it's original value
    await modify_subscription_noblock(ticker_id, g.subscriptions[ticker_id])
    sub_sock.close()
    msg_out = {"content": res, "msg_id": msg["msg_id"], "result": "ok"}
    msg_bytes = (" " + json.dumps(msg_out)).encode()
    await g.sock_ctl.send_multipart(ident + [b"", msg_bytes])

async def handle_get_snapshot(ident, msg, msg_raw):
    with await g.sub_locks[msg["content"]["ticker_id"]]:
        await handle_get_snapshot_2(ident, msg, msg_raw)
    g.num_snapshots_emulated += 1

async def handle_modify_subscription_2(ident, msg, msg_raw):
    content = msg["content"]
    content_mod = dict(content)
    ticker_id = content_mod.pop("ticker_id")
    old_sub_def = g.subscriptions[ticker_id]
    new_sub_def = deepcopy(old_sub_def)
    new_sub_def.update(content_mod)
    if new_sub_def.empty():
        g.subscriptions.pop(ticker_id, "")
    else:
        g.subscriptions[ticker_id] = new_sub_def
    await g.sock_deal.send_multipart(ident + [b"", msg_raw])
    msg_parts = await g.sock_deal_pub.poll_for_msg_id(msg["msg_id"])
    msg_bytes = msg_parts[-1]
    msg = json.loads(msg_bytes.decode())
    if msg["result"] != "ok":
        # revert changes
        g.subscriptions[ticker_id] = old_sub_def
    await g.sock_ctl.send_multipart(ident + [b"", msg_bytes])

async def handle_modify_subscription(ident, msg, msg_raw):
    """Calls modify_subscription and tracks subscriptions."""
    with await g.sub_locks[msg["content"]["ticker_id"]]:
        await handle_modify_subscription_2(ident, msg, msg_raw)

async def handle_list_capabilities(ident, msg, msg_raw):
    await g.sock_deal.send_multipart(ident + [b"", msg_raw])
    msg_parts = await g.sock_deal_pub.poll_for_msg_id(msg["msg_id"])
    msg = json.loads(msg_parts[-1].decode())
    if msg["result"] != "ok":
        await g.sock_ctl.send_multipart(ident + [b"", msg_parts[-1]])
        return
    caps = set(msg["content"])
    caps.add("GET_SNAPSHOT")
    msg["content"] = sorted(caps)
    msg_bytes = (" " + json.dumps(msg)).encode()
    await g.sock_ctl.send_multipart(ident + [b"", msg_bytes])
    
async def handle_msg_1(ident, msg_raw):
    msg = json.loads(msg_raw.decode())
    msg_id = msg["msg_id"]
    cmd = msg["command"]
    debug_str = "ident={}, command={}, msg_id={}"
    debug_str = debug_str.format(ident_to_str(ident), cmd, msg["msg_id"])
    L.debug("> " + debug_str)
    try:
        if cmd == "get_status":
            await handle_get_status(ident, msg, msg_raw)
        elif cmd == "get_snapshot":
            await handle_get_snapshot(ident, msg, msg_raw)
        elif cmd == "modify_subscription":
            await handle_modify_subscription(ident, msg, msg_raw)
        elif cmd == "list_capabilities":
            await handle_list_capabilities(ident, msg, msg_raw)
        else:
            await fwd_message_no_change(msg_id, ident + [b"", msg_raw])
    except Exception as e:
        L.exception("{} on msg_id: {}".format(type(e).__name__, msg_id))
        await send_error(ident, msg_id, error.GENERIC, e)
    L.debug("< " + debug_str)

async def run_ctl_interceptor():
    L.info("run_ctl_interceptor running ...")
    while True:
        msg_parts = await g.sock_ctl.recv_multipart()
        ident, msg = split_message(msg_parts)
        if len(msg) == 0:
            # handle ping message
            await g.sock_deal.send_multipart(msg_parts)
            res = await g.sock_deal_pub.poll_for_pong()
            await g.sock_ctl.send_multipart(res)
            continue
        create_task(handle_msg_1(ident, msg))

###############################################################################

async def send_recv_init_cmd(command, content=None):
    msg = {"command": command}
    msg["msg_id"] = msg_id = str(uuid.uuid4())
    if content:
        msg["content"] = content
    msg_bytes = (" " + json.dumps(msg)).encode()
    await g.sock_deal.send_multipart([b"", msg_bytes])
    msg_parts = await g.sock_deal.recv_multipart()
    msg = msg_parts[-1]
    msg = json.loads(msg.decode())
    error.check_message(msg)
    return msg

def parse_args():
    desc = "middleware module that emulates get_snapshot using subscribe"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("ctl_addr_up",
                        help="address of the upstream ctl socket")
    parser.add_argument("ctl_addr_down",
                        help="ctl socket binding address")
    parser.add_argument("pub_addr_up",
                        help="address of the upstream pub socket")
    parser.add_argument("-t",
                        "--collector-timeout",
                        type=float,
                        default=COLLECTOR_TIMEOUT / 1000,
                        help="max seconds to wait for data collector")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args

def setup_logging(args):
    setup_root_logger(args.log_level)

def init_zmq_sockets(args):
    g.sock_deal = g.ctx.socket(zmq.DEALER)
    g.sock_deal.setsockopt_string(zmq.IDENTITY, MODULE_NAME)
    g.sock_deal.connect(args.ctl_addr_up)
    g.sock_deal_pub = SockRecvPublisher(g.ctx, g.sock_deal)
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr_down)
    g.pub_addr_up = args.pub_addr_up

async def init_check_capabilities(args):
    msg = await send_recv_init_cmd("list_capabilities")
    content = msg["content"]
    if "GET_SNAPSHOT" in content:
        L.critical("nothing to do, please remove this module from this chain")
        sys.exit(1)
    g.cap_pub_order_book_positions = "PUB_ORDER_BOOK_POSITIONS" in content

async def init_get_subscriptions():
    msg = await send_recv_init_cmd("get_subscriptions")
    subs = msg["content"]
    g.subscriptions.update(subs)

async def init_list_available_quotes():
    try:
        msg = await send_recv_init_cmd("list_available_quotes")
    except RemoteException:
        g.available_quotes = None
    else:
        g.available_quotes = msg["content"]

def main():
    global L
    global COLLECTOR_TIMEOUT
    args = parse_args()
    COLLECTOR_TIMEOUT = args.collector_timeout * 1000
    setup_logging(args)
    init_zmq_sockets(args)
    # A ticker_id based lock is needed to synchronize
    # modify_subscriptions commands for each ticker.
    g.sub_locks = defaultdict(lambda: asyncio.Lock())
    L.debug("checking capabilities ...")
    g.loop.run_until_complete(init_check_capabilities(args))
    L.debug("capabilities checked")
    L.debug("fetching old subscriptions ...")
    g.loop.run_until_complete(init_get_subscriptions())
    L.info("{} old subscription(s)".format(len(g.subscriptions)))
    L.debug("fetching available quotes ...")
    g.loop.run_until_complete(init_list_available_quotes())
    L.debug("available quotes fetched")
    tasks = [
        g.sock_deal_pub.run(),
        run_ctl_interceptor(),
    ]
    tasks = [create_task(coro_obj) for coro_obj in tasks]
    g.loop.run_until_complete(asyncio.gather(*tasks))

if __name__ == "__main__":
    main()
