import subprocess
import time
import pytest
import pika


def _run_docker_command(*args):
    cmd = ["docker"] + list(args)
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


@pytest.fixture(scope="session")
def rabbitmq_service():
    container_name = "test_rabbitmq"

    _run_docker_command("rm", "-f", container_name)

    run_cmd = [
        "run", "-d",
        "--name", container_name,
        "-p", "5672:5672",
        "-p", "15672:15672",
        "-e", "RABBITMQ_DEFAULT_USER=admin",
        "-e", "RABBITMQ_DEFAULT_PASS=admin",
        "rabbitmq:3-management"
    ]

    result = _run_docker_command(*run_cmd)
    if result.returncode != 0:
        pytest.skip(f"No se pudo iniciar RabbitMQ: {result.stdout}")

    start_time = time.time()
    timeout = 30
    connected = False

    while time.time() - start_time < timeout:
        try:
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host="localhost",
                    port=5672,
                    credentials=pika.PlainCredentials("admin", "admin"),
                )
            )
            conn.close()
            connected = True
            break
        except Exception:
            time.sleep(1)

    if not connected:
        pytest.skip("No se pudo conectar a RabbitMQ")

    yield

    _run_docker_command("rm", "-f", container_name)


@pytest.fixture(scope="session")
def rabbitmq_host():
    return "localhost"