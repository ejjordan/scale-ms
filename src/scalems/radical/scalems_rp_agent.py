"""Provide the entry point for SCALE-MS execution management under RADICAL Pilot."""

import logging
import sys
import typing
from typing import NamedTuple
from typing import Sequence

import radical.pilot as rp
import radical.utils as ru
from radical.pilot.raptor.request import Request

logger = logging.getLogger('scalems_rp_agent')


class PilotDescription(NamedTuple):
    """What is the role of the this in the raptor.Master config?"""
    runtime: int = 30  # ?
    resource: str = 'local.localhost'  # Target resource


class WorkerDescription(NamedTuple):
    """Describe the Raptor worker task."""
    executable: str = 'scalems_rp_worker'  # Executable to launch.
    arguments: Sequence[str] = ()  # Arguments passed to executable.
    pre_exec: Sequence[str] = ()  # Shell commands to run in the wrapper script before executable.


class SchedulerConfig(NamedTuple, object):
    cpn: int = 4  # CPUs per node
    gpn: int = 0  # GPUs per node
    n_masters: int = 1  # Number of Raptor Master tasks
    n_workers: int = 1  # Number of Raptor Worker tasks
    pilot_descr: PilotDescription = PilotDescription()  # ?
    worker_descr: WorkerDescription = WorkerDescription()  # Specify the Worker(s) to launch.


def encode_as_dict(config: NamedTuple):
    data = config._asdict()
    for key, value in data.items():
        if hasattr(value, '_asdict'):
            data[key] = encode_as_dict(value)
    return data


_RaptorReturnType = typing.Tuple[typing.Text, typing.Text, typing.SupportsInt]
"""Raptor worker task return values are interpreted as a tuple (out, err, ret).

The first two elements are cast to output and error strings, respectively.

The third is cast to an integer return code.
"""

_RaptorWorkData = typing.TypeVar('_RaptorWorkData')
"""Argument type for a Raptor task implementation.

Constraints on the data type are not yet well-defined.
Presumably, the object must be "Munch"-able.
"""


class _RequestInput(typing.Mapping):
    """Input argument for a raptor.Request instantiation.

    Not yet fully specified, but not to be confused with
    raptor.Request instances.

    A dict-like object with at least a *uid* key.
    """


class RaptorWorkCallable(typing.Protocol[_RaptorWorkData]):
    def __call__(self, data: _RaptorWorkData) -> _RaptorReturnType:
        ...


class RaptorWorkDescription(typing.Protocol[_RaptorWorkData]):
    """Represent the content of an *arguments* element in a RaptorTaskDescription.

    A dictionary resembling this structure is converted to radical.pilot.raptor.Request
    by the Master in radical.pilot.raptor.Master.request().

    Attributes:
        mode (str): Dispatching key for raptor.Worker._dispatch()

    Note that some keys may be added or overwritten during Master._receive_tasks
    (e.g. *is_task*, *uid*, *task*).
    """
    cores: int
    timeout: typing.SupportsFloat
    mode: str  # Must map to a mode (RaptorWorkCallable) in the receiving Worker._modes
    data: _RaptorWorkData  # Munch-able object to be passed to Worker._modes[*mode*](*data*)


class _RaptorTaskDescription(typing.Protocol):
    """Note the distinctions of a TaskDescription to processed by a raptor.Master.


    The single element of *arguments* is a JSON-encoded object that will be
    deserialized (RaptorWorkDescription) as the prototype for the dictionary used to instantiate the Request.
    """
    uid: str  # Unique identifier for the Task across the Session.
    executable: typing.ClassVar[str] = 'scalems'  # Unused by Raptor tasks.
    scheduler: str  # The UID of the raptor.Master scheduler task.
    arguments: typing.Sequence[str]  # Processed by raptor.Master._receive_tasks


class ScaleMSMaster(rp.raptor.Master):

    def __init__(self, cfg):
        rp.raptor.Master.__init__(self, cfg=cfg)

        self._log = ru.Logger(self.uid, ns='radical.pilot')

    def result_cb(self, requests: typing.Sequence[Request]):
        for r in requests:
            r['task']['stdout'] = r['out']

            logger.info('result_cb %s: %s [%s]' % (r.uid, r.state, r.result))

    # What is the relationship between the create_work_items() hook and request()?
    # def create_work_items(self):
    #     super().create_work_items()

    def request(self, reqs: typing.Sequence[_RequestInput]):
        # 'arguments' (element 0) gets wrapped in a Request at the Master by _receive_tasks,
        # then the list of requests is passed to Master.request(), which is presumably
        # an extension point for derived Master implementations. The base class method
        # converts requests to dictionaries and adds them to a request queue, from which they are
        # picked up by the Worker in _request_cb. Then picked up in forked interpreter
        # by Worker._dispatch, which checks the *mode* of the Request and dispatches
        # according to native or registered mode implementations. (e.g. 'call' (native) or 'scalems')

        # TODO: This seems like the place to insert special processing for, say, non-Worker tasks or control signals.
        return super().request(reqs)

    # def _receive_tasks(self, tasks: typing.Sequence[dict]):
    #     # The 'arguments' key of each element in *tasks* is a JSON-encoded object
    #     # with the "work" schema: mode, cores, timeout, and data keys.
    #     super()._receive_tasks(tasks)


def main():
    # The command-line argument is a JSON file encoding a SchedulerConfig
    cfg = ru.Config(cfg=ru.read_json(sys.argv[1]))
    master = ScaleMSMaster(cfg)

    master.submit(descr=cfg.worker_descr, count=cfg.n_workers,
                  cores=cfg.cpn, gpus=cfg.gpn)

    master.start()
    master.join()
    master.stop()


if __name__ == '__main__':
    # Note: This block is only the entry point for, e.g. `python -m scalems.radical.scalems_rp_agent`
    # When invoked using the installed console script entry point, main() is called directly
    # by installed wrapper script generated by setuptools.

    # For additional console logging, create and attach a stream handler.
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)
    # Should we be interacting with the RP logger?

    sys.exit(main())
