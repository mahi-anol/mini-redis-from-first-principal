import time
import fnmatch
import random
class DataStore:
    def __init__(self):
        ## storage format {key:(value,type,expiry_time)}
        self._data={}
        self._memory_usage=0 ### tracks total memory usage of the redis server. In bytes.

    def set(self,key,value,expiry_time=None):
        ### previously we didnt had memory cleanup overhead. Now that we have it;
        ### we can directly set. First we have to clean memory and then add, then calculate new memory.
        if key in self._data:
            old_value,_,_=self._data[key]
            self._memory_usage-=self._calculate_memory_usage(key,old_value)
        data_type=self._get_data_type(value)
        self._data[key]=(value,data_type,expiry_time)
        self._memory_usage+=self._calculate_memory_usage(key,value)
    
    def get(self,key):
        if not self._is_key_valid(key):
            return None
        value,_,_=self._data[key]
        return value
    
    def delete(self,*keys):
        count=0
        for key in keys:
            if key in self._data:
                value,_,_=self._data[key]
                self._memory_usage-=self._calculate_memory_usage(key,value)
                del self._data[key]
                count+=1
        return count
    def exists(self,*keys):
        return sum(1 for key in keys if self._is_key_valid(key))
    
    """
    Pattern matching for keys: fnmatch.fnmatch for Unix shell-style wildcard matching
    * matches any characters
    ? matches a single character
    [abc] matches any character in the brackets
    """
    
    def keys(self,pattern="*"):
        valid_keys=[key for key in self._data.keys if self._is_key_valid(key)]
        if pattern =="*":
            return valid_keys
        return [key for key in valid_keys if fnmatch.fnmatch(key,pattern)]
    
    def flush(self):
        self._data.clear()
        self._memory_usage=0
    
    def expire(self,key,seconds):
        """Sets expiration time in seconds"""
        if not self._is_key_valid(key):
            return False
        value,data_type,_=self._data[key]
        expiry_time=time.time()+seconds
        self._data[key]=(value,data_type,expiry_time)
        return True
    
    def expire_at(self,key,timestamp):
        """Sets expiration time in seconds"""
        if not self._is_key_valid(key):
            return False
        value,data_type,_=self._data[key]
        self._data[key]=(value,data_type,timestamp)
        return True
    
    def ttl(self,key):
        """Kotokkhon time baki ase sehta check kore second e return kore..."""
        if key not in self.data:
            return -2
        
        value,_,expiry_time=self._data[key]

        if expiry_time is None:
            return -1 #No expiration set.
        
        remaining=expiry_time-time.time()

        if remaining<=0:
            ### key expired, so need to remove it.
            self._memory_usage-=self._calculate_memory_usage(key,value)
            del self._data[key]
            return -2
        return int(remaining)
    
    def pttl(self,key):
        """Kotokkhon time baki ase sehta check kore mili second e return kore."""
        if key not in self.data:
            return -2
        
        value,_,expiry_time=self._data[key]

        if expiry_time is None:
            return -1 #No expiration set.
        
        remaining=expiry_time-time.time()

        if remaining<=0:
            ### key expired, so need to remove it.
            self._memory_usage-=self._calculate_memory_usage(key,value)
            del self._data[key]
            return -2
        return int(remaining*1000)
    

    def persist(self,key):
        """Remove expiration from key"""
        if not self._is_key_valid(key):
            return False
        
        value,data_type,_=self._data[key]
        self._data[key]=(value,data_type,None)
        return True
    
    def get_type(self,key):
        """Get data type of key"""
        if not self._is_key_valid(key):
            return "none"
        
        _,data_type,_=self._data[key]
        return data_type
    
    def get_memory_usage(self):
        return self._memory_usage

    def cleanup_expired_keys(self):
        if not self._data:
            return 0
        
        current_time=time.time()
        expired_keys=[]
        sample_size=min(20,len(self._data))
        sample_keys=random.sample(list(self.data.keys()),sample_size)

        for key in sample_keys:
            value,_,expiry_time=self._data[key]
            if expiry_time is not None and expiry_time<=current_time:
                expired_keys.append(key)

        for key in expired_keys:
            value,_,_=self._data[key]
            self._memory_usage-=self._calculate_memory_usage(key,value)
            del self._data[key]

        return len(expired_keys)

    def _is_key_valid(self,key):
        """Check if key exists and hasn't expired."""
        if key not in self._data:
            return False
        value,_,expiry_time=self._data[key]
        if expiry_time is not None and expiry_time<=time.time():
            self._memory_usage-=self._calculate_memory_usage(key,value)
            del self._data[key]
            return False
        return True

    def _get_data_type(self, value):
        """Determine Redis data type"""
        if isinstance(value, str):
            return "string"
        elif isinstance(value, int):
            return "string"  # Redis stores numbers as strings
        elif isinstance(value, list):
            return "list"
        elif isinstance(value, set):
            return "set"
        elif isinstance(value, dict):
            return "hash"
        else:
            return "string"
    

    def _calculate_memory_usage(self, key,value):
        key_size=len(str(key).encode('utf-8'))
        value_size=len(str(value).encode('utf-8'))
        return key_size+value_size+64 ###overhead maybe for dayatype and expirary time.