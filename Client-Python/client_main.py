import sys
from typing import List
from client_service import ClientService
from command_processor import CommandProcessor

class ClientMain:

    @staticmethod
    def debug(DEBUG_FLAG: bool, debug_message: str):
        if DEBUG_FLAG:
            print(debug_message, file=sys.stderr)


    @staticmethod
    def main(args: List[str]):
        # set flag to true to print debug messages. 
        # the flag can be set adding "-debug" to server args 
        DEBUG_FLAG = False

        # check arguments
        if len(args) < 2 or (len(args) == 2 and ("-debug" in args)):
            print("Argument(s) missing!", file=sys.stderr)
            print("Usage: python script.py <host:port> <client_id>", file=sys.stderr)
            return

        if len(args) == 3 and args[2] == "-debug":
            DEBUG_FLAG = True

        print("ClientMain")

        # receive and print arguments
        ClientMain.debug(DEBUG_FLAG, f"Received {len(args)} arguments")
        for i, arg in enumerate(args):
            ClientMain.debug(DEBUG_FLAG, f"arg[{i}] = {arg}")

        # get the host and port of the server or front-end
        host_port = args[0]
        client_id = int(args[1])  

        parser = CommandProcessor(ClientService(DEBUG_FLAG, host_port, client_id))
        parser.parse_input()


if __name__ == "__main__":
    ClientMain.main(sys.argv[1:])