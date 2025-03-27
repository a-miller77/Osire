import tempfile
import sqlite3
import httpx
import asyncio
import time
import subprocess
import os
import platform
import paramiko
import uvicorn
import logging
from datetime import datetime, timedelta
from typing import Optional, AsyncGenerator, Dict
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import StreamingResponse

from config import config

app = FastAPI()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ensure scripts exist (for local development/fallback)
if not os.path.exists(config.start_script_path):
    logger.error(f"Start services script not found at {config.start_script_path}")
    raise FileNotFoundError(f"Start services script not found at {config.start_script_path}")

if not os.path.exists(config.stop_script_path):
    logger.error(f"Stop services script not found at {config.stop_script_path}")
    raise FileNotFoundError(f"Stop services script not found at {config.stop_script_path}")

# Create database directory if it doesn't exist
os.makedirs(os.path.dirname(config.database_path), exist_ok=True)

# Global client for reuse
client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
# Cache for health check results
health_cache: Dict[str, tuple[bool, float]] = {}

# Hop-by-hop headers that should not be forwarded
HOP_BY_HOP_HEADERS = frozenset([
    'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
    'te', 'trailers', 'transfer-encoding', 'upgrade'
])

class SSHManager:
    _instance = None
    
    def __init__(self):
        self.ssh_client = None
        
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = SSHManager()
        return cls._instance
    
    def connect(self):
        if self.ssh_client is not None:
            return
            
        # Try pinging the host first to check connectivity
        param = '-n' if platform.system().lower()=='windows' else '-c'
        command = ['ping', param, '1', config.ssh_host]
        try:
            subprocess.check_call(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            logger.error(f"Could not reach SSH host {config.ssh_host}")
            raise ConnectionError(f"Could not reach SSH host {config.ssh_host}")
            
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            # Use key-based authentication if configured
            if config.use_ssh_key and os.path.exists(config.ssh_key_path):
                key = paramiko.RSAKey.from_private_key_file(config.ssh_key_path)
                self.ssh_client.connect(
                    hostname=config.ssh_host,
                    username=config.ssh_username,
                    pkey=key
                )
            else:
                # Use password authentication
                self.ssh_client.connect(
                    hostname=config.ssh_host,
                    username=config.ssh_username,
                    password=config.ssh_password
                )
            logger.info(f"Successfully connected to {config.ssh_host}")
        except Exception as e:
            logger.error(f"SSH connection failed: {str(e)}")
            raise ConnectionError(f"SSH connection failed: {str(e)}")
        
    def cleanup(self):
        if self.ssh_client:
            self.ssh_client.close()
            self.ssh_client = None
            logger.info("SSH connection closed")
            
    def execute_command(self, command):
        """Execute a command over SSH and return stdout and stderr."""
        if not self.ssh_client:
            self.connect()
            
        stdin, stdout, stderr = self.ssh_client.exec_command(command)
        return stdout.read().decode(), stderr.read().decode()

@app.on_event("shutdown")
async def shutdown_event():
    await client.aclose()
    # Close SSH connection if exists
    ssh_manager = SSHManager.get_instance()
    ssh_manager.cleanup()

async def check_service_health(service_name: str, url: str) -> bool:
    """Check if a service is healthy by making a GET request to its health endpoint."""
    current_time = time.time()
    
    # Check cache first
    if service_name in health_cache:
        last_status, last_check_time = health_cache[service_name]
        if current_time - last_check_time < config.health_check_cache_time:
            return last_status

    try:
        response = await client.get(f"{url}/health", timeout=5.0)
        is_healthy = response.status_code == 200
        health_cache[service_name] = (is_healthy, current_time)
        return is_healthy
    except Exception as e:
        logger.error(f"Health check failed for service {service_name}: {str(e)}")
        health_cache[service_name] = (False, current_time)
        return False

async def start_service(service_name: str):
    """Start a service using SSH to execute the remote script."""
    try:
        if config.ssh_enabled:
            # Use SSH to start the service
            ssh_manager = SSHManager.get_instance()
            
            # Execute the start script command remotely
            remote_command = f"sbatch {config.ssh_remote_scripts}/run.sh"
            stdout, stderr = ssh_manager.execute_command(remote_command)
            
            if stderr:
                logger.warning(f"Warning while starting service {service_name}: {stderr}")
            
            logger.info(f"Starting service {service_name} via SSH: {stdout}")
        else:
            # Fallback to local execution if SSH is disabled
            subprocess.Popen(
                [config.start_script_path, service_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            logger.info(f"Starting service {service_name} using local batch script")
    except Exception as e:
        logger.error(f"Error starting service {service_name}: {str(e)}")

async def wait_for_service(service_name: str, url: str, timeout: int = 60) -> bool:
    """Wait for a service to become healthy with periodic health checks."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if await check_service_health(service_name, url):
            logger.info(f"Service {service_name} is now healthy")
            return True
        await asyncio.sleep(config.health_check_interval)
    logger.warning(f"Timeout waiting for service {service_name} to become healthy")
    return False

def get_service_info(service_name: str) -> Optional[tuple]:
    """Get service information from the database.
    
    First checks the local service registry, and if the service is not found,
    attempts to check the remote service registry over SSH by copying the file
    locally and then querying it.
    """
    # Check local service registry first
    conn = sqlite3.connect(config.database_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT service_url, last_heartbeat, version FROM service_registry WHERE service_name = ?",
        (service_name,)
    )
    result = cursor.fetchone()
    conn.close()
    
    # If service not found locally and SSH is enabled, check remote registry
    if not result and config.ssh_enabled:
        try:
            logger.info(f"Service '{service_name}' not found in local registry, checking remote registry")
            ssh_manager = SSHManager.get_instance()
            
            # Remote file path
            remote_db_path = f"{config.ssh_remote_scripts}/service_registry.sqlite"
            
            # Create a temporary local file to store the remote database
            temp_dir = tempfile.gettempdir()
            temp_db_path = os.path.join(temp_dir, f"temp_remote_registry_{int(time.time())}.sqlite")
            
            # Use SFTP to copy the remote file to the local machine
            if not ssh_manager.ssh_client:
                ssh_manager.connect()
                
            sftp = ssh_manager.ssh_client.open_sftp()
            try:
                # Try to copy the remote file
                sftp.get(remote_db_path, temp_db_path)
                
                # Now query the local copy
                temp_conn = sqlite3.connect(temp_db_path)
                temp_cursor = temp_conn.cursor()
                temp_cursor.execute(
                    "SELECT service_url, last_heartbeat, version FROM service_registry WHERE service_name = ?",
                    (service_name,)
                )
                remote_result = temp_cursor.fetchone()
                temp_conn.close()
                
                # Clean up the temporary file
                os.remove(temp_db_path)
                
                if remote_result:
                    logger.info(f"Found service '{service_name}' in remote registry")
                    return remote_result
                
            except Exception as sftp_e:
                logger.error(f"SFTP error with remote registry: {str(sftp_e)}")
            finally:
                sftp.close()
                
        except Exception as e:
            logger.error(f"Error accessing remote service registry: {str(e)}")
    
    return result if result else None

async def stop_service(service_name: str):
    """Stop a service using SSH to execute the remote script."""
    try:
        if config.ssh_enabled:
            # Use SSH to stop the service
            ssh_manager = SSHManager.get_instance()
            
            # Execute the stop script command remotely
            remote_command = f"{config.ssh_remote_scripts}/stop_services.sh {service_name}"
            stdout, stderr = ssh_manager.execute_command(remote_command)
            
            if stderr:
                logger.warning(f"Warning while stopping service {service_name}: {stderr}")
                
            logger.info(f"Stopped service {service_name} via SSH: {stdout}")
        else:
            # Fallback to local execution if SSH is disabled
            subprocess.run(
                [config.stop_script_path, service_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True
            )
            logger.info(f"Stopped service {service_name} using local batch script")
    except Exception as e:
        logger.error(f"Error stopping service {service_name}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop service {service_name}: {str(e)}"
        )

@app.post("/_services/{service_name}/stop")
async def stop_service_endpoint(service_name: str):
    """Endpoint to stop a service."""
    service_info = get_service_info(service_name)
    if not service_info:
        raise HTTPException(status_code=404, detail=f"Service '{service_name}' not found")
    
    await stop_service(service_name)
    return {"message": f"Service {service_name} stopped successfully"}

async def stream_response(response: httpx.Response) -> AsyncGenerator[bytes, None]:
    """Stream response chunks from the upstream service."""
    async for chunk in response.aiter_bytes():
        yield chunk

async def forward_request(method: str, url: str, request: Request, headers: dict) -> httpx.Response:
    """Forward the request to the target service."""
    return await client.request(
        method=method,
        url=url,
        headers=headers,
        content=request.stream(),
        params=request.query_params
    )

@app.api_route("/{service_name}/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def gateway(service_name: str, path: str, request: Request):
    """Main gateway endpoint that handles all incoming requests."""
    # Skip internal endpoints
    if service_name == "_services":
        raise HTTPException(status_code=404, detail="Not found")
        
    logger.info(f"Received {request.method} request for service {service_name}, path: {path}")
    
    service_info = get_service_info(service_name)
    
    if not service_info:
        logger.warning(f"Service '{service_name}' not found in registry")
        raise HTTPException(status_code=404, detail=f"Service '{service_name}' not found")
    
    url, last_heartbeat, version = service_info
    current_time = datetime.now()
    last_heartbeat_time = datetime.fromisoformat(last_heartbeat) if last_heartbeat else None
    
    # Check if service heartbeat is recent enough
    is_recent = (last_heartbeat_time and 
                (current_time - last_heartbeat_time) < timedelta(seconds=config.heartbeat_threshold))
    
    if not is_recent and not await check_service_health(service_name, url):
        logger.warning(f"Service {service_name} unhealthy, attempting to start")
        await start_service(service_name)
        if not await wait_for_service(service_name, url):
            logger.error(f"Service {service_name} failed to start and is unavailable")
            raise HTTPException(
                status_code=503,
                detail=f"Service '{service_name}' is unavailable"
            )
    
    # Forward the request
    try:
        # Filter out hop-by-hop headers
        headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in HOP_BY_HOP_HEADERS
        }
        
        response = await forward_request(
            request.method,
            f"{url}/{path}",
            request,
            headers
        )
        
        logger.info(f"Successfully forwarded request to {service_name}, status: {response.status_code}")
        
        # Create response headers
        response_headers = {
            k: v for k, v in response.headers.items()
            if k.lower() not in HOP_BY_HOP_HEADERS
        }

        # Check if response should be streamed
        content_type = response.headers.get('content-type', '')
        is_streaming = any(
            content_type.startswith(t) for t in (
                'text/event-stream',
                'multipart/form-data',
                'application/octet-stream'
            )
        )

        if is_streaming:
            return StreamingResponse(
                stream_response(response),
                status_code=response.status_code,
                headers=response_headers,
                media_type=content_type
            )
        else:
            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=response_headers,
                media_type=content_type
            )

    except httpx.ConnectError as e:
        logger.error(f"Connection error to service {service_name}: {str(e)}")
        # Only attempt restart if health check fails
        if not await check_service_health(service_name, url):
            logger.warning(f"Service {service_name} unhealthy, attempting restart")
            await start_service(service_name)
            if await wait_for_service(service_name, url):
                # Retry the request once
                try:
                    response = await forward_request(
                        request.method,
                        f"{url}/{path}",
                        request,
                        headers
                    )
                    
                    response_headers = {
                        k: v for k, v in response.headers.items()
                        if k.lower() not in HOP_BY_HOP_HEADERS
                    }
                    
                    content_type = response.headers.get('content-type', '')
                    is_streaming = any(
                        content_type.startswith(t) for t in (
                            'text/event-stream',
                            'multipart/form-data',
                            'application/octet-stream'
                        )
                    )

                    if is_streaming:
                        return StreamingResponse(
                            stream_response(response),
                            status_code=response.status_code,
                            headers=response_headers,
                            media_type=content_type
                        )
                    else:
                        return Response(
                            content=response.content,
                            status_code=response.status_code,
                            headers=response_headers,
                            media_type=content_type
                        )
                except Exception as retry_e:
                    logger.error(f"Retry failed for service {service_name}: {str(retry_e)}")
                    
        raise HTTPException(
            status_code=502,
            detail=f"Error forwarding request to service '{service_name}': {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error forwarding request to service {service_name}: {str(e)}")
        raise HTTPException(
            status_code=502,
            detail=f"Error forwarding request to service '{service_name}': {str(e)}"
        )

if __name__ == "__main__":
    logger.info("Starting gateway server on localhost:8080")
    uvicorn.run("gateway:app", host="localhost", port=8080, reload=True)
