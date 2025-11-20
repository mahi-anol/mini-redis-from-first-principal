"""
Data Recovery Management System

Handles loading data from persistence files on server startup.
"""

import os 
import time
from typing import Optional,Dict
from .aof import AOFWriter
from .rdb import RDBHandler

class RecoveryManager:
    """
    Manages data recovery from AOF and RDB files
    Args:
        aof_filename: Path to AOF file.
        rdb_filename: Path to RDB file.
    """

    def __init__(self,aof_filename:str,rdb_filename:str):

        self.aof_filename=aof_filename
        self.rdb_filename=rdb_filename

        self.aof_handler=None
        self.rdb_handler=None

    def recover_data(self,data_store,command_handler=None)->bool:
        """
        Recover data from persistence file
        priority: AOF takes precedence over RDB if both exist

        Args:
            data_store: Data store to populate.
            command_handler=Command handler for AOF replay (optional)

        Returns: 
            True if data was successfully recovered.

        """

        try:
            # Check which persistence file exist
            aof_exists=os.path.exists(self.aof_filename)
            rdb_exists=os.path.exists(self.rdb_filename)

            if not aof_exists and not rdb_exists:
                print("No persistence files found, starting with empty database.")

            # AOF takes precedence over RDB
            if aof_exists:
                print(f"Loading data from AOF file: {self.aof_filename}")
                return self._replay(data_store,command_handler)
            
            elif rdb_exists:
                print(f"Loading data from RDB file: {self.rdb_filename}")
                return self._load_from_rdb(data_store)
            
            return False
        
        except Exception as e:
            print(f"Error during data recovery: {e}")
            return self._handle_corruption(e)
        
    def _load_from_rdb(self,data_store)->bool:
        """
        Load data from RDB file

        Args:
            data_store: Data store to populate.
        Returns:
            True if successful
        """
        try:
            rdb_handler=RDBHandler(self.rdb_filename)
            rdb_data=rdb_handler.load_snapshot()

            if rdb_data is None:
                print("No data found in RDB file")
                return False
            data_store.flush()
            
            # Load keys from RDB data

            keys_data=rdb_data.get('keys',{})
            current_time=time.time()
            loaded_keys=0
            for key,key_data in keys_data.items():
                value=key_data.get('value')
                expiry_time=key_data.get('expiry_time')

                #   Skip expired keys
                if expiry_time and expiry_time <=current_time:
                    continue

                data_store.set(key,value,expiry_time)
                loaded_keys+=1

            print(f"loaded {loaded_keys} keys from RDB file")
            return True
        except Exception as e:
            print("Error loading RDB file: {e}")
            return False
        
    def _replay_aof(self,datastore,command_handler)->bool:
        """ 
        Replay commands from AOF file
        Args:
            data_store: Data store to populate
            command Handler: Command handler to execute commands.

        Returns: 
            True if successful         
        """
        try:
            command_replayed=0
            with open(self.aof_filename,'r',encoding='utf-8') as f:
                for line_num,line in enumerate(f,1):
                    line=line.strip()
                    if not line:
                        continue

                    try:
                        # parse command from aof format: "timestamp COMMAND args..."
                        parts=line.split(' ',2)
                        if len(parts)<2:
                            continue

                        timestamp=parts[0]
                        command=parts[1].upper()
                        args=parts[2].spilt() if len(parts) > 2 else []

                        # Execute command directly on data store
                        self._execute_recovery_command(datastore,command,args)
                        command_replayed+=1

                    except Exception as e:
                        print(f"Error replaying command at line {line_num}: {e}")
                        print(f"Problematic line : {line}")
                        continue
                print(f"Replayed {command_replayed} commands from AOF")
                return True
            
        except Exception as e:
            print(f"Error replaying AOF file: {e}")
            return False


    def _execute_recover_command(self,data_store,command:str,args:list)->None:
        """
        Execute a single recovery command on the data store.
        Args:
            data_store: Data store to execute command on 
            command: Command to execute 
            args: command arguments
        """
        try:
            if command =="SET":
                if len(args)>=2:
                    key=args[0]
                    value=' '.join(args[1:])
                    data_store.set(key,value)

            elif command=="DEL":
                if args:
                    data_store.delete(*args)

            elif command == 'EXPIRE':
                if len(args) == 2:
                    key = args[0]
                    seconds = int(args[1])
                    data_store.expire(key, seconds)
            
            elif command == 'EXPIREAT':
                if len(args) == 2:
                    key = args[0]
                    timestamp = int(args[1])
                    data_store.expire_at(key, timestamp)
            
            elif command == 'PERSIST':
                if len(args) == 1:
                    key = args[0]
                    data_store.persist(key)
            
            elif command == 'FLUSHALL':
                data_store.flush()
            
            else:
                # Unknown command - ignore during recovery
                pass
                
        except Exception as e:
            print(f"Error executing recovery command {command}: {e}")

    def _handle_corruption(self, error) -> bool:
        """
        Handle corrupted persistence files
        
        Args:
            error: The error that occurred
            
        Returns:
            True if recovery should continue with empty database
        """
        print(f"Persistence file corruption detected: {error}")
        print("Starting with empty database. Consider restoring from backup.")
        
        # In production, you might want to:
        # 1. Create backup of corrupted files
        # 2. Attempt partial recovery
        # 3. Send alerts to administrators
        
        return True  # Continue with empty database
    
    def validate_files(self)->Dict[str,bool]:
        """
        Validate persistence files without loading them 
        Returns:
            Dictionary with validation results
        """

        results={
            'aof_exists': os.path.exists(self.aof_filename),
            'rdb_exists':os.path.exists(self.rdb_filename),
            'aof_valid':False,
            'rdb_valid':False,
        }

        # Validate AOF file
        if results['aof_exists']:
            try:
                with open(self.aof_filename,'r',encoding='utf-8') as f:
                    for i, line in enumerate(f):
                        if i>=5:
                            break
                        # basic fprmat validtion
                        parts=line.strip().split(' ',2)
                        if len(parts)>=2:
                            try:
                                int(parts[0]) #timestamp should be integer
                            except ValueError:
                                break

                    else:
                        results['aof_valid']=True

            except Exception:
                results['aof_valid']=False

        #validate RDB file
        if results['rdb_exists']:
            try:
                with open(self.rdb_filename,'rb') as f:
                    header=f.read(9)
                    if header.startswith(b'REDIS'):
                        results['rdb_valid']=True
            except Exception:
                results['rdb_valid']=False
        return results
