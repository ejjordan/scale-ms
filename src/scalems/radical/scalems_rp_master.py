"""Provide the entry point for SCALE-MS execution management under RADICAL Pilot."""

import logging
import sys
import typing

import radical.pilot as rp
import radical.utils as ru
from radical.pilot.raptor.request import Request

from scalems.radical.raptor import RequestInputList

logger = logging.getLogger('scalems_rp_master')


class ScaleMSMaster(rp.raptor.Master):

    def __init__(self, *args, **kwargs):
        rp.raptor.Master.__init__(self, *args, **kwargs)

        self._log = ru.Logger(self.uid, ns='radical.pilot')

    def result_cb(self, requests: typing.Sequence[Request]):
        for r in requests:
            logger.info('result_cb %s: %s [%s]' % (r.uid, r.state, r.result))

    def request_cb(self, requests: RequestInputList) -> RequestInputList:
        """Allows all incoming requests to be processed by the Master.

        Request is a dictionary deserialized from Task.description.arguments[0],
        plus three additional keys.

        Note that Tasks may not be processed in the same order in which they are submitted
        by the client.

        If overridden, request_cb() must return a list (presumably, the same as the list
        of the requests that should be processed normally after the callback). This allows
        subclasses of rp.raptor.Master to add or remove requests before they become Tasks.
        The returned list is submitted with self.request() by the base class after the
        callback returns.

        A Master may call self.request() to self-submit items (e.g. instead of or in
        addition to manipulating the returned list in request_cb()).

        It is the developer's responsibility to choose unique task IDs (uid) when crafting
        items for Master.request().
        """
        # for req_input in requests:
        #     request = Request(req=req_input)
        return requests


def main():
    # TODO: Test both with and without a provided config file.
    kwargs = {}
    if len(sys.argv) > 1:
        cfg = ru.Config(cfg=ru.read_json(sys.argv[1]))
        kwargs['cfg'] = cfg
        descr = cfg.worker_descr,
        count = cfg.n_workers,
        cores = cfg.cpn,
        gpus = cfg.gpn
    else:
        descr = rp.TaskDescription({
            'uid': 'raptor.worker',
            'executable': 'scalems_rp_worker',
            'arguments': []
        })
        count = 1
        cores = 1
        gpus = 0
    master = ScaleMSMaster(**kwargs)

    master.submit(
        descr=descr,
        count=count,
        cores=cores,
        gpus=gpus)

    master.start()
    master.join()
    master.stop()


if __name__ == '__main__':
    # Note: This block is only the entry point for, e.g. `python -m scalems.radical.scalems_rp_master`
    # When invoked using the installed console script entry point, main() is called directly
    # by installed wrapper script generated by setuptools.

    # For additional console logging, create and attach a stream handler.
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)
    # Should we be interacting with the RP logger?

    sys.exit(main())
