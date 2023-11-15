from scalems.simple.airflow.operators.radical_operator import RadicalPythonOperator, radical_task

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils import timezone


@radical_task
def run_grompp(input_gro: str, verbose: bool = False):
    import os
    import gmxapi as gmx
    input_dir='/home/joe/experiments/radical/alanine-dipeptide'
    input_top = os.path.join(input_dir, "topol.top")
    input_mdp = os.path.join(input_dir, "grompp.mdp")
    input_gro = os.path.join(input_dir, input_gro)
    input_files={'-f': input_mdp, '-p': input_top, '-c': input_gro,},
    tpr = "run.tpr"
    output_files={'-o': tpr}
    grompp = gmx.commandline_operation(gmx.commandline.cli_executable(), 'grompp', input_files, output_files)
    grompp.run()
    if verbose:
        print(grompp.output.stderr.result())
    assert os.path.exists(grompp.output.file['-o'].result())
    return grompp.output.file['-o'].result()

"""
grompp_task = RadicalPythonOperator(
            task_id="run_grompp",
            python_callable=run_grompp,
            #params={"input_gro": "equil3.gro"},
        )
"""

@task
def run_mdrun(tpr_path, verbose: bool = False):
    import os
    import gmxapi as gmx
    if not os.path.exists(tpr_path):
        raise FileNotFoundError("You must supply a tpr file")

    input_files={'-s': tpr_path}
    output_files={'-x':'alanine-dipeptide.xtc', '-c': 'result.gro'}
    md = gmx.commandline_operation(gmx.commandline.cli_executable(), 'mdrun', input_files, output_files)
    md.run()
    if verbose:
        print(md.output.stderr.result())
    assert os.path.exists(md.output.file['-c'].result())
    return md.output.file['-c'].result()

with DAG('run_gmxapi', start_date=timezone.utcnow(), catchup=False) as dag:
    grompp=run_grompp("equil3.gro")
    mdrun=run_mdrun(grompp)

"""
@dag(
    schedule=None,
    start_date=timezone.utcnow(),
    catchup=False,
    tags=["gmxapi_task"],
)
def run_gmxapi():
    grompp=run_grompp("equil3.gro")
    mdrun=run_mdrun(grompp)

#output=run_gmxapi()
"""