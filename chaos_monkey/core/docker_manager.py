import json
import subprocess

import pydantic


class Container(pydantic.BaseModel):
    """
    Lightweight representation of a Docker container as returned by ``docker ps``.

    Attributes:
        ID: Unique identifier of the container.
        Names: Human–readable name assigned to the container.
        State: Current state of the container (e.g. ``"running"``, ``"exited"``).
    """
    ID: str
    Names: str
    State: str


class DockerManager:
    """
    Helper class for interacting with Docker using subprocess commands.

    This class abstracts common operations such as listing running containers
    and stopping a specific container. All methods are class methods so the
    class can be used without instantiation.
    """
    @classmethod
    def get_containers(cls) -> list[Container]:
        """
        Retrieve the list of running Docker containers.

        Returns:
            A list of :class:`Container` instances describing each running container.

        Raises:
            subprocess.CalledProcessError: If the ``docker ps`` command fails.
        """
        result = subprocess.run(['docker', 'ps', '--format', 'json'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result.check_returncode()
        output = result.stdout.decode("utf-8")
        return [Container(**json.loads(container_str)) for container_str in output.strip().splitlines()]

    @classmethod
    def kill_container(cls, container: Container):
        """
        Gracefully stop a Docker container by name.

        This method uses ``docker stop`` to stop the container, which allows
        the process to shut down cleanly before being killed.

        Args:
            container: The container to stop.

        Raises:
            subprocess.CalledProcessError: If the ``docker stop`` command fails.
        """
        result = subprocess.run(['docker', 'kill', container.Names], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result.check_returncode()
