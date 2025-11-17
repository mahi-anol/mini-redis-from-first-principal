from redis_server import RedisServer
def main():
    server=RedisServer()
    try:
        server.start()
    except KeyboardInterrupt:
        print("Shutting Down the server by keyboard interrupt.")
        server.stop()

if __name__=="__main__":
    main()