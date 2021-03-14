"""Provide the entry point for SCALE-MS execution management under RADICAL Pilot."""

import logging
import sys

import radical.pilot as rp
import radical.utils as ru

logger = logging.getLogger('scalems_rp_agent')


class ScaleMSMaster(rp.raptor.Master):

    def __init__(self, cfg):
        rp.raptor.Master.__init__(self, cfg=cfg)

        self._log = ru.Logger(self.uid, ns='radical.pilot')

    def result_cb(self, requests):
        for r in requests:
            r['task']['stdout'] = r['out']

            logger.info('result_cb %s: %s [%s]' % (r.uid, r.state, r.result))


def main():
    cfg = ru.Config(cfg=ru.read_json(sys.argv[1]))
    master = ScaleMSMaster(cfg)

    master.submit(descr=cfg.worker_descr, count=cfg.n_workers,
                  cores=cfg.cpn, gpus=cfg.gpn)

    master.start()
    master.join()
    master.stop()


if __name__ == '__main__':
    # For additional console logging, create and attach a stream handler.
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)
    # Should we be interacting with the RP logger?

    sys.exit(main())
