import boto3
import yaml


def define_jobs():
    runnable = list()
    dependent = list()

    stream = open("gfw_pixetl/fixures/sources.yaml", "r")
    layers = yaml.load(stream)
    for layer in layers.keys():
        for attribute in layers[layer].keys():
            version = layers[layer][attribute]["version"]
            for grid in layers[layer][attribute]["grids"].keys():
                if grid != "1/4000":
                    name = f"{layer}/{attribute}/{grid}"
                    job_name = f"{layer}__{attribute}_{grid.replace('/', '-')}"
                    command = [
                        layer,
                        "--version",
                        version,
                        "--source_type",
                        "raster",
                        "--field",
                        attribute,
                        "--grid_name",
                        grid,
                        "--overwrite",
                    ]

                    if "uri" in layers[layer][attribute]["grids"][grid].keys():
                        runnable.append(
                            {
                                "layer": name,
                                "job_name": job_name,
                                "command": command,
                                "grid": grid,
                            }
                        )

                    elif "depends_on" in layers[layer][attribute]["grids"][grid].keys():
                        dependent.append(
                            {
                                "layer": name,
                                "job_name": job_name,
                                "command": command,
                                "depends_on": layers[layer][attribute]["grids"][grid][
                                    "depends_on"
                                ],
                                "grid": grid,
                            }
                        )

    return runnable, dependent


def jobs():
    runnable, dependent = define_jobs()
    running = dict()

    for job in runnable:
        running[job["layer"]] = submit_job(job)

    while dependent:
        jobs = dependent
        dependent = list()
        for job in jobs:
            try:
                depends_on = [
                    {"jobId": running[job["depends_on"]], "type": "SEQUENTIAL"}
                ]
                running[job["layer"]] = submit_job(job, depends_on)
            except KeyError:
                dependent.append(job)


def submit_job(job, depends_on=None):
    client = boto3.client("batch")

    job_name = job["job_name"]
    job_queue = "pixetl-job-queue"
    job_definition = "pixetl"
    command = job["command"]
    grid = job["grid"]

    vcpus = 48 if grid == "10/40000" else 8
    memory = 380000 if grid == "10/40000" else 63000

    attempts = 2
    attempt_duration_seconds = 7200

    if depends_on is None:
        depends_on = list()

    response = client.submit_job(
        jobName=job_name,
        jobQueue=job_queue,
        dependsOn=depends_on,
        jobDefinition=job_definition,
        containerOverrides={"command": command, "vcpus": vcpus, "memory": memory},
        retryStrategy={"attempts": attempts},
        timeout={"attemptDurationSeconds": attempt_duration_seconds},
    )

    print(response)
    return response["jobId"]


if __name__ == "__main__":
    jobs()
