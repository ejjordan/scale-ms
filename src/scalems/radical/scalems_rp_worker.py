"""Provide the entry point for SCALE-MS tasks dispatched by scalems_rp_master."""

import logging
import sys
import time

import radical.pilot as rp
import radical.pilot.raptor as rpt

logger = logging.getLogger('scalems_rp_worker')


class ScaleMSWorker(rpt.Worker):
    def __init__(self, cfg: str):
        # *cfg* is a JSON file generated by the Master to provide a
        # TaskDescription-like record to the raptor.Worker initializer.
        rp.raptor.Worker.__init__(self, cfg)

        self.register_mode('gmx', self._gmx)

    def _gmx(self, data):
        out = 'gmx  : %s %s' % (time.time(), data['blob'])
        err = None
        ret = 0

        return out, err, ret

    def hello(self, world):
        return 'call : %s %s' % (time.time(), world)


def main():
    """Manage the life of a Worker instance.

    Launched as a task submitted with the worker_descr provided to the corresponding Master script.
    The framework generates a local file, passed as the first argument to the script,
    which is processed by the raptor.Worker base class for initialization.

    The Worker instance created here will receive Requests as dictionaries
    on a queue processed with Worker._request_cb, which then passes requests
    to Worker._dispatch functions in forked interpreters.

    Worker._dispatch handles requests according to the *mode* field of the request.
    New *mode* names may be registered with Worker.register_mode, but note that
    the important place for the mode to be registered is at the forked interpreter,
    not the interpreter running this main() function.
    """
    # Master generates a file to be appended to the argument list.
    #
    cfg = None
    if len(sys.argv) > 1:
        cfg = sys.argv[1]

    worker = ScaleMSWorker(cfg=cfg)
    worker.start()
    time.sleep(5)
    worker.join()


if __name__ == '__main__':
    # Note: This block is only the entry point for, e.g. `python -m scalems.radical.scalems_rp_worker`
    # When invoked using the installed console script entry point, main() is called directly
    # by installed wrapper script generated by setuptools.

    # For additional console logging, create and attach a stream handler.
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)
    # Should we be interacting with the RP logger?

    sys.exit(main())