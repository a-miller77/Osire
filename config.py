import os
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# Base directory settings
GATEWAY_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(GATEWAY_DIR)

class GatewayConfig(BaseSettings):
    """Gateway configuration settings"""
    
    # Base directory settings
    gateway_dir: str = Field(default=GATEWAY_DIR, description="Gateway directory path")
    project_root: str = Field(default=PROJECT_ROOT, description="Project root directory path")
    
    # Database settings
    database_path: str = Field(
        default=os.path.join(PROJECT_ROOT, "privileged", "service_registry.sqlite"),
        description="Path to the service registry database"
    )
    
    # Local script paths (for fallback or local development)
    start_script_path: str = Field(
        default=os.path.join(PROJECT_ROOT, "services", "common", "start_services.bat"),
        description="Path to the script for starting services"
    )
    stop_script_path: str = Field(
        default=os.path.join(PROJECT_ROOT, "services", "common", "stop_services.bat"),
        description="Path to the script for stopping services"
    )
    
    # Gateway settings
    heartbeat_threshold: int = Field(default=90, description="Heartbeat threshold in seconds")
    health_check_interval: int = Field(default=5, description="Health check interval in seconds")
    health_check_cache_time: int = Field(default=5, description="Health check cache time in seconds")
    
    # SSH settings
    ssh_enabled: bool = Field(default=True, description="Toggle to enable/disable SSH functionality")
    ssh_host: str = Field(default="remote-server.example.com", description="Remote SSH server hostname")
    ssh_username: str = Field(default="username", description="SSH username")
    ssh_password: str = Field(default="password", description="SSH password")
    ssh_remote_scripts: str = Field(default="/data/...", description="Remote directory with service scripts")

    # SSH Key-based authentication (alternative to password auth)
    ssh_key_path: str = Field(
        default=os.path.join(PROJECT_ROOT, "privileged", "ssh_key.pem"),
        description="Path to SSH private key file"
    )
    use_ssh_key: bool = Field(default=False, description="Set to True to use key-based authentication instead of password")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Create and export config instance
config = GatewayConfig() 