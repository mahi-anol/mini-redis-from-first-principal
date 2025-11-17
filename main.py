from redis_server import RedisServer

def main():
    server=RedisServer()
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down the server due to keyboard exception.")
        server.stop()
    
if __name__ == "__main__":
    main()