import dataclasses

import os
import pathlib
import sys
import tempfile
import typing

import scalems.store as _store
from scalems.call import serialize_call

@dataclasses.dataclass(frozen=True)
class AirflowSubprocess:
    uid: str
    input_filenames: tuple
    output_filenames: tuple
    executable: str
    arguments: tuple[str, ...]
    keyword_arguments: dict = dataclasses.field(default_factory=dict)
    requirements: dict = dataclasses.field(default_factory=dict)


def airflow_function_call_to_subprocess(
    func: typing.Union[typing.Callable, str],
    label: str,
    args: tuple = (),
    kwargs: dict = None,
    requirements: dict = None,
    venv: str = None,
) -> AirflowSubprocess:
    if kwargs is None:
        kwargs = dict()
    if requirements is None:
        requirements = dict()
    with tempfile.NamedTemporaryFile(mode="w", suffix="-input.json") as tmp_file:
        tmp_file.write(serialize_call(func=func, args=args, kwargs=kwargs, requirements=requirements))
        tmp_file.flush()
        # We can't release the temporary file until the file reference is obtained.
        # file_ref = _store.get_file_reference(pathlib.Path(tmp_file.name), filestore=datastore)

    uid = str(label)
    input_filename = uid + "-input.json"
    # TODO: Collaborate with scalems.call to agree on output filename.
    output_filename = uid + "-output.json"
    if venv:
        executable = os.path.join(venv, "bin", "python")
    else:
        executable = "python3"
    arguments = []
    for key, value in getattr(sys, "_xoptions", {}).items():
        if value is True:
            arguments.append(f"-X{key}")
        else:
            assert isinstance(value, str)
            arguments.append(f"-X{key}={value}")

    # TODO: Validate with argparse, once scalems.call.__main__ has a real parser.
    if "ranks" in requirements:
        arguments.extend(("-m", "mpi4py"))
    # If this wrapper doesn't go away soon, we should abstract this so path arguments
    # can be generated at the execution site.
    arguments.extend(("-m", "scalems.call", input_filename, output_filename))

    return AirflowSubprocess(
        uid=uid,
        input_filenames=(input_filename,),
        output_filenames=(output_filename,),
        executable=executable,
        arguments=tuple(arguments),
        keyword_arguments=kwargs,
        requirements=requirements.copy(),
    )