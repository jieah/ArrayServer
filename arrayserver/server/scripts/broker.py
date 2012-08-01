
import gevent
import gevent.monkey
gevent.monkey.patch_all()
import gevent_zeromq
gevent_zeromq.monkey_patch()

import argparse
import logging
import shelve

from arrayserver.server.arrayserverbroker import ArrayServerBroker

def main():

    parser = argparse.ArgumentParser(description='Start a ArrayServer Array Node process.')

    parser.add_argument(
        '-v', '--verbose', help='increase output verbosity', action='store_true'
    )
    parser.add_argument(
        '-a', '--address', help='specify broker address', default="127.0.0.1"
    )
    parser.add_argument(
        '-f', '--frontend', help='specify the frontend port', type=int, default=5001
    )
    parser.add_argument(
        '-b', '--backend', help='specify the backend port', type=int, default=5002
    )

    args = parser.parse_args()

    log_level = logging.INFO if not args.verbose else logging.DEBUG
    logging.basicConfig(level=log_level)
    log = logging.getLogger(__name__)

    broker = ArrayServerBroker(
        "tcp://%s:%d" % (args.address, args.frontend),
        "tcp://%s:%d" % (args.address, args.backend)
    )
    broker.run()

