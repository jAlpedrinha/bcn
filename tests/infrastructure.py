"""
Infrastructure management utilities for tests
"""

import time
from typing import Dict, List

import docker


class InfrastructureManager:
    """Manages Docker infrastructure for testing"""

    def __init__(self):
        """Initialize infrastructure manager"""
        try:
            self.client = docker.from_env()
        except Exception as e:
            print(f"Warning: Could not connect to Docker: {e}")
            self.client = None

    def is_container_running(self, container_name: str) -> bool:
        """
        Check if a container is running

        Args:
            container_name: Name of the container

        Returns:
            True if running, False otherwise
        """
        if not self.client:
            return False

        try:
            container = self.client.containers.get(container_name)
            return container.status == "running"
        except docker.errors.NotFound:
            return False
        except Exception as e:
            print(f"Error checking container {container_name}: {e}")
            return False

    def is_container_healthy(self, container_name: str) -> bool:
        """
        Check if a container is healthy

        Args:
            container_name: Name of the container

        Returns:
            True if healthy, False otherwise
        """
        if not self.client:
            return False

        try:
            container = self.client.containers.get(container_name)

            # Check if container is running
            if container.status != "running":
                return False

            # Check health status if available
            health = container.attrs.get("State", {}).get("Health", {})
            if health:
                return health.get("Status") == "healthy"

            # If no health check, consider running as healthy
            return True

        except docker.errors.NotFound:
            return False
        except Exception as e:
            print(f"Error checking health of {container_name}: {e}")
            return False

    def wait_for_healthy(self, container_name: str, timeout: int = 300) -> bool:
        """
        Wait for a container to become healthy

        Args:
            container_name: Name of the container
            timeout: Maximum time to wait in seconds

        Returns:
            True if container became healthy, False if timeout
        """
        print(f"Waiting for {container_name} to be healthy...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.is_container_healthy(container_name):
                print(f"✓ {container_name} is healthy")
                return True
            time.sleep(5)

        print(f"✗ {container_name} did not become healthy within {timeout} seconds")
        return False

    def check_required_services(self, services: List[str]) -> Dict[str, bool]:
        """
        Check status of required services

        Args:
            services: List of service/container names

        Returns:
            Dictionary mapping service name to health status
        """
        status = {}
        for service in services:
            status[service] = self.is_container_healthy(service)
        return status

    def ensure_infrastructure_ready(self, required_services: List[str] = None) -> bool:
        """
        Ensure all required infrastructure is running and healthy

        Args:
            required_services: List of required service names

        Returns:
            True if all services are ready, False otherwise
        """
        if required_services is None:
            required_services = ["minio", "postgres-hive", "hive-metastore", "spark-iceberg"]

        print("Checking infrastructure status...")

        status = self.check_required_services(required_services)

        all_healthy = all(status.values())

        if all_healthy:
            print("✓ All required services are healthy")
            return True
        else:
            print("✗ Some services are not healthy:")
            for service, healthy in status.items():
                symbol = "✓" if healthy else "✗"
                print(f"  {symbol} {service}")
            return False

    def start_infrastructure(self, compose_file: str = "docker-compose.yml") -> bool:
        """
        Start infrastructure using docker-compose

        Args:
            compose_file: Path to docker-compose file

        Returns:
            True if started successfully
        """
        import subprocess

        print("Starting infrastructure with docker-compose...")

        try:
            subprocess.run(
                ["docker-compose", "up", "-d"], capture_output=True, text=True, check=True
            )

            print("Infrastructure started, waiting for services to be healthy...")

            # Wait for critical services
            services_to_wait = ["hive-metastore", "minio"]
            return all(self.wait_for_healthy(service, timeout=300) for service in services_to_wait)

        except subprocess.CalledProcessError as e:
            print(f"Failed to start infrastructure: {e}")
            print(f"STDOUT: {e.stdout}")
            print(f"STDERR: {e.stderr}")
            return False
