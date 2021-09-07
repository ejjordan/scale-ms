"""Manage non-volatile data for SCALE-MS in a filesystem context.

The data store must be active before attempting any manipulation of the managed workflow.
The data store must be closed by the time control returns to the interpreter after
leaving a managed workflow scope (and releasing a WorkflowManager instance).

We prefer to do this with the Python context manager protocol, since we already rely on
`scalems.workflow.scope()`.

We can also use a contextvars.ContextVar to hold a weakref to the FileStore, and register
a finalizer to perform a check, but the check could come late in the interpreter shutdown
and we should not rely on it. Also, note the sequence with which module variables and
class definitions are released during shutdown.

TODO: Make FileStore look more like a File interface, with open and close and context
    manager support, and provide boolean status properties. Let initialize_datastore() return the
    currently in-scope FileStore.
"""

__all__ = [
    'ContextError',
    'StaleFileStore',
    'initialize_datastore',
]

import contextvars
import dataclasses
import json
import logging
import os
import pathlib
import tempfile
import threading
import typing
import warnings
import weakref
from contextvars import ContextVar
from typing import Optional
from weakref import ReferenceType

import scalems.exceptions
from ._lock import LockException
from ._lock import scoped_directory_lock as _scoped_directory_lock

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

_data_subdirectory = '.scalems_0_0'
"""Subdirectory name to use for managed filestore."""

_metadata_filename = 'scalems_context_metadata.json'
"""Name to use for Context metadata files (module constant)."""


_filestore: ContextVar[Optional[ReferenceType]] = contextvars.ContextVar('_filestore')


class StaleFileStore(Exception):
    """The backing file store is not consistent with the known (local) history."""


class ContextError(Exception):
    """A Context operation could not be performed."""


@dataclasses.dataclass
class Metadata:
    instance: int


class FileStore:
    """Handle to the SCALE-MS nonvolatile data store for a workflow context.

    Not thread safe. User is responsible for serializing access, as necessary.

    Fields:
        instance (int): Owner's PID.
        log (list): Access log for the data store.
        filepath (pathlib.Path): filesystem path to metadata JSON file.
        directory (pathlib.Path): workflow directory.

    """
    _fields: typing.ClassVar = set([field.name for field in dataclasses.fields(Metadata)])
    _instances: typing.ClassVar = weakref.WeakValueDictionary()

    _token: typing.Optional[contextvars.Token]
    _data: Metadata
    _directory: pathlib.Path
    _update_lock: threading.Lock
    _dirty: threading.Event
    _log: typing.Sequence[str]

    @property
    def log(self):
        # Interface TBD...
        for line in self._log:
            yield line

    # TODO: Consider a caching proxy to the directory structure to reduce filesystem
    #  calls.

    @property
    def directory(self) -> pathlib.Path:
        """The work directory under management."""
        return self._directory

    @property
    def datastore(self) -> pathlib.Path:
        """Path to the data store for the workflow managed at *directory*"""
        return self.directory / _data_subdirectory

    @property
    def filepath(self) -> pathlib.Path:
        """Path to the metadata backing store."""
        return self.datastore / _metadata_filename

    @property
    def instance(self):
        return self._data.instance

    def __repr__(self):
        return f'<{self.__class__.__qualname__}, "{self.instance}:{self.directory}">'

    def __init__(self, *,
                 directory: pathlib.Path):
        """Assemble the data structure.

        Users should not create FileStore objects directly, but with
        initialize_datastore() or through the WorkflowManager instance.

        Once initialized, caller is responsible for calling the close() method either
        directly or by using the instance as a Python context manager (in a `with`
        expression).

        No directory in the filesystem should be managed by more than one FileStore.
        The FileStore class maintains a registry of instances to prevent instantiation of
        a new FileStore for a directory that is already managed.

        Raises:
            ContextError if attempting to instantiate for a directory that is already
            managed.
        """
        self._directory = pathlib.Path(directory).resolve()

        # TODO: Use a log file!
        self._log = []

        self._update_lock = threading.Lock()
        self._dirty = threading.Event()

        try:
            with _scoped_directory_lock(directory):
                existing_filestore = self._instances.get(directory, None)
                if existing_filestore:
                    raise ContextError(
                        f'{directory} is already managed by {repr(existing_filestore)}'
                    )
                # The current interpreter process is not aware of an instance for
                # *directory*

                instance_id = os.getpid()

                try:
                    self.datastore.mkdir()
                except FileExistsError:
                    # Check if the existing filesystem state is due to a previous clean
                    # shutdown, a previous dirty shutdown, or inappropriate concurrent access.
                    if not self.filepath.exists():
                        raise ContextError(
                            f'{self.directory} contains invalid datastore '
                            f'{self.datastore}.'
                        )
                    logger.debug('Restoring metadata from previous session.')
                    with self.filepath.open() as fp:
                        metadata_dict = json.load(fp)
                    if metadata_dict['instance'] is not None:
                        # The metadata file has an owner.
                        if metadata_dict['instance'] != instance_id:
                            # This would be a good place to check a heart beat or
                            # otherwise
                            # try to confirm whether the previous owner is still active.
                            raise ContextError('Context is already in use.')
                        else:
                            assert metadata_dict['instance'] == instance_id
                            raise scalems.exceptions.InternalError(
                                'This process appears already to be managing '
                                f'{self.filepath}, but is not '
                                'registered in FileStore._instances.'
                            )
                    else:
                        # The metadata file has no owner. Presume clean shutdown
                        logger.debug(
                            f'Taking ownership of metadata file for PID {instance_id}')
                        metadata_dict['instance'] = instance_id
                else:
                    logger.debug(f'Created new data store {self.datastore}.')
                    metadata_dict = {'instance': instance_id}

                metadata = Metadata(**metadata_dict)
                with open(self.filepath, 'w') as fp:
                    json.dump(dataclasses.asdict(metadata), fp)
                FileStore._instances[self.directory] = self
                self.__dict__['_data'] = metadata

        except LockException as e:
            raise ContextError(
                'Could not acquire ownership of working directory {}'.format(directory)) from e

    def __enter__(self):
        # Suggest doing the real work in open, and just check for valid state here.
        # Add a reentrance check; only one code entity should be managing the FileStore
        # lifetime.
        self._token = _filestore.set(weakref.ref(self))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.close()
        except (StaleFileStore, scalems.exceptions.ScopeError) as e:
            warnings.warn(f'{repr(self)} could not be exited cleanly.')
            logger.exception('FileStore.close() raised exception.', exc_info=e)
        else:
            _filestore.reset(self._token)
            del self._token
        # Indicate that we have not handled any exceptions.
        if exc_value:
            return False

    def flush(self):
        """Write the current metadata to the backing store, if there are pending
        updates.

        For atomic updates and better forensics, we write to a new file and *move* the
        file to replace the previous metadata file. If an error occurs, the temporary
        file will linger.

        Changes to metadata must be
        made with the update lock held and must end by setting the "dirty" condition
        (notifying the watchers of "dirty"). flush() should acquire the update lock and
        clear the "dirty" condition when successful. We can start with "dirty" as an
        Event. If we need more elaborate logic, we can use a Condition.
        It should be sufficient to use asyncio primitives, but we may need to use
        primitives from the `threading` module to allow metadata updates directly
        from RP task callbacks.

        """
        with self._update_lock:
            if self._dirty.is_set():
                with tempfile.NamedTemporaryFile(dir=self.datastore,
                                                 delete=False,
                                                 mode='w',
                                                 prefix='flush_',
                                                 suffix='.json') as fp:
                    json.dump(dataclasses.asdict(self._data), fp)
                pathlib.Path(fp.name).rename(self.filepath)
                self._dirty.clear()

    def close(self):
        """Flush, shut down, and disconnect the FileStore from the managed directory.

        Raises:
            StaleFileStore if called on an invalid or outdated handle.
            ScopeError if called from a disallowed context, such as from a forked process.

        """
        current_instance = getattr(self, 'instance', None)
        if current_instance is None or self.closed:
            raise StaleFileStore(
                'Called close() on an inactive FileStore.'
            )
        if current_instance != os.getpid():
            raise scalems.exceptions.ScopeError(
                'Calling close() on a FileStore from another process is not allowed.')

        with _scoped_directory_lock(self.directory):
            try:
                with open(self.filepath, 'r') as fp:
                    context = json.load(fp)
                    stored_instance = context.get('instance', None)
                    if stored_instance != current_instance:
                        # Note that the StaleFileStore check will be more intricate as
                        # metadata becomes more sophisticated.
                        raise StaleFileStore(
                            'Expected ownership by {}, but found {}'.format(
                                current_instance,
                                stored_instance))
            except OSError as e:
                raise StaleFileStore('Could not open metadata file.') from e

            # del self._data.instance
            self._data.instance = None
            self._dirty.set()
            self.flush()
            del FileStore._instances[self.directory]

    @property
    def closed(self) -> bool:
        return FileStore._instances.get(self.directory, None) is not self


def get_context() -> typing.Union[FileStore, None]:
    """Get currently active workflow context, if any."""
    ref = _filestore.get(None)
    if ref is not None:
        filestore = ref()
        if filestore is None:
            # Prune dead weakrefs.
            _filestore.set(None)
        return filestore
    return None


def set_context(datastore: FileStore):
    """Set the active workflow context.

    We do not yet provide for holding multiple metadata stores open at the same time.

    Raises:
        ContextError if a FileStore is already active.

    """
    current_context = get_context()
    if current_context is not None:
        raise ContextError(f'The context is already active: {current_context}')
    else:
        ref = weakref.ref(datastore)
        _filestore.set(ref)


def initialize_datastore(directory=None) -> FileStore:
    """Get a reference to a workflow metadata store.

    If initialize_datastore() succeeds, the caller is responsible for calling the
    `close()` method of the returned instance before the interpreter exits,
    either directly or by using the instance as a Python context manager.
    """
    if directory is None:
        directory = os.getcwd()
    path = pathlib.Path(directory)
    filestore = FileStore(directory=path)
    return filestore