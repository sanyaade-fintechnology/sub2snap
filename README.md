## Subscription to snapshot emulating middleware module

sub2snap emulates `get_snapshot` command using `modify_subscription` and `list_available_quotes` commands.

### Dependencies
* python 3.x with asyncio support
* pyzmq >= 17.0.0 (for asyncio support, get directly from github repo)
