"""
Implementing Append-only File (AOF) Implementation.
Handles logging of write commands to disk for data persistance and recovery.
"""
import os
import time
import threading
import shutil

class AOFWriter:
    """Handles AOF(Append only file) operations for command logging"""
    def __init__(self,filename:str,sync_policy:str='everysec'):
        """
        Initialize aof writer 

        Args: 
            filename: path to AOF file
            sync_policy: Sync policy ('always','everysec','no')
        """

        self.filename=filename
        self.sync_policy=sync_policy
        self.file_handle=None
        self.last_sync_time=time.time()
        self.pending_writes=0
        self._lock=threading.Lock()

        # Write commands that needs to be logged.
        self.write_commands={
            'SET','DEL','EXPIRE','EXPIREAT','PERSIST','FLUSHALL'
        }
        os.makedirs(os.path.dirname(filename),exist_ok=True) 

    def open(self) ->None:
        """Open AOF file for writing"""
        try:
            self.file_handle=open(self.filename,'a',encoding='utf-8')
        except IOError as e:
            raise RuntimeError(f"Failed to open AOF file {self.filename}: {e} ")
        
    def close(self)->None:
        """Close AOF file"""
        if self.file_handle:
            self.syn_to_disk() ### FInal sync before closing.
            self.file_handle.close()
            self.file_handle=None

    def log_command(self,command:str,*args)->None:
        """
        LOG a command to the AOF file

        Args:
            comand: command name (eg., 'SET','DEL')
            *args: Command arguments
        """
        if not self.file_handle or command.upper() not in self.write_commands:
            return 
        
        with self._lock:
            try:
                formatted_command=self._format_command(command,*args)
                self.file_handle.write(formatted_command) #python buffer
                self.pending_writes+=1

                #sync based on policy

                if self.sync_policy=='always':
                    self.file_handle.flush() #os kernel buffer
                    os.fsync(self.file_handle.fileno()) #physical disk
                    self.last_sync_time=time.time()
                    self.pending_writes=0

            except IOError as e:
                print(f"Error writing to AOF FILE: {e}")


    def _format_command(self,command:str,*args)->str:
        """Format command in Redis protocol format for AOF"""
        # Simple text format for AOF (easier to read and debug)
        timestamp=int(time.time())
        formatted_args=' '.join(str(arg) for arg in args)
        return f"{timestamp} {command.upper()} {formatted_args}\n"