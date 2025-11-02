"""
Persistance Configration Management 

Handles all configuration related to data persistance including AOF and RDB settings.
"""
import os
import time
from typing import list, Tuple,Dict,Any

class PersistenceConfig:
    """Configuation class for Redis persistance settings"""

    def __init__(self,config_dict:Dict[str,Any]=None):
        """
        Initialize persistance configuration

        Args:
            config dict: Dictionary containing configuration options
        """

        self._config=self._get_defatult_config()

        if config_dict:
            self._config.update(config_dict)

        self._validate_config()

    def _get_default_config(self)->Dict[str,Any]:
        """Get default persistance configuration"""
        return{
            'aof_enabled':True,
            'aof_filename':'appendonly.aof',
            'aof_sync_policy':'everysec',
            'aof_rewrite_percentage':100,
            'aof_rewrite_min_size':1024*1024,
            # Directory Configuaration
            'data_dir':'./data',
            'temp_dir':'./data/temp',

            #General Settings
            'persistance_enabled':True,
            'recovery_on_startup':True,
            'max_memory_usage':100*1024*1024 ## 100 MB max memeory
        }
    def _validate_config(self)->None:
        """validate configuration values"""
        valid_sync_policies=['always','everysec','no']
        if self.config['aof_sync_policy'] not in valid_sync_policies:
            raise ValueError(f"Invalid AOF sync policy. Must be one of {valid_sync_policies}")
        
        if not self._config['aof_filename']:
            raise ValueError("AOF filename empty connot be empty")
    
    def get(self,key:str,default=None):
        """Get configuration value"""
        return self._config.get(key,default)
    
    def set(self,key:str,value:Any)->None:
        """Set configuration value"""
        self._config[key]=value
        self._validate_config()

    def update(self,config_dict:Dict[str,Any]) -> None:
        """Update multiple configuration values """
        self._config.update(config_dict)
        self._validate_config()

    def get_all(self)->Dict[str,Any]:
        """Get all configuration values"""
        return self._config.copy()
    # Convenience properties for frequently accessed settings.

    @property
    def aof_enabled(self)->bool:
        return self._config['aof_enabled']
    @property
    def aof_filename(self)->str:
        return os.path.join(self._config['data_dir'],self._config['aof_filename'])
    @property
    def aof_sync_policy(self)->str:
        return self._config['aof_sync_policy']
    @property
    def data_dir(self)->str:
        return self._config['data_dir']
    @property
    def temp_dir(self)->str:
        return self._config['temp_dir']
    
    def ensure_directories(self)->None:
        """Ensure data and temp directories exits"""
        os.makedirs(self.data_dir,exist_ok=True)
        os.makedirs(self.temp_dir,exist_ok=True)

    def get_aof_temp_filename(self)->str:
        """Get temporary AOF filename for rewrite operations"""
        return os.path.join(self.temp_dir,f"temp-rewrite-aof-{int(time.time())}.aof")
    def __repr__(self)->str:
        """String representation of configuration"""
        return f"PersistanceConfig({self._config})"
    
