def ok():
    return b"+OK\r\n"
def pong():
    return b"+PONG\r\n" ### Accoridng to resp protocol of redis \r\n represents line end.


def null_bulk_string():
    ### if a key doesn't exist we will need it, cause we return NULL but in binary string,
    return b"$-1\r\n"

def simple_string(value):
    return f"+{value}\r\n".encode()

def bulk_string(value):
    if value is None:
        return null_bulk_string()
    return f"${len(value)}\r\n{value}\r\n".encode()
    
def error(message):
    return f"-ERR {message}\r\n".encode()

def integer(value):
    return f":{value}\r\n".encode()

def array(items):
    """
        Args: items that are in byte.
        Return: byte array.
    """
    
    if not items:
        return b"*0\r\n"
    result=[f"*{len(items)}\r\n".encode()]
    result.extend(items)
    return b"".join(result)