import sys
sys.path.insert(1, '../Contract/target/generated-sources/protobuf/python')

import grpc
import TupleSpaces_pb2 as pb2
import TupleSpaces_pb2_grpc as pb2_grpc

class ClientService:
    def debug(self, debug_message: str):
        if self.DEBUG_FLAG:
            print(debug_message, file=sys.stderr)

    def __init__(self, DEBUG_FLAG: bool, host_port: str, client_id: int):
        # Create channel/stub for each server
        self.DEBUG_FLAG = DEBUG_FLAG
        self.debug(f"Connecting to server at {host_port}")
        self.channel = grpc.insecure_channel(host_port)
        self.stub = pb2_grpc.TupleSpacesStub(self.channel)

    # Implement individual methods for each remote operation of the TupleSpaces service
    def put(self, tuple_value: str) -> str:
        self.debug(f"Calling put() with tuple: {tuple_value}")
        request = pb2.PutRequest(newTuple=tuple_value)
        try:
            self.stub.put(request)
            self.debug("Put operation successful")
            return "OK"
        except grpc.RpcError as e:
            self.handle_grpc_error(e)
            return None
        

    def read(self, pattern: str) ->str:
        self.debug(f"Calling read() with pattern: {pattern}")
        request = pb2.ReadRequest(searchPattern = pattern)
        
        try:
            response = self.stub.read(request)
            self.debug(f"Read operation successful, server returned: {str(response.result)}")
            return str(response.result) 
        except grpc.RpcError as e:
            self.handle_grpc_error(e)
            return None


    def take(self, tuple:str) -> str:
        self.debug(f"Calling take() with tuple: {tuple}")
        request = pb2.TakeRequest(searchPattern = tuple)

        try: 
            response = self.stub.take(request)
            self.debug(f"Take operation successful, server returned: {str(response.result)}")
            return str(response.result) 
        except grpc.RpcError as e:
            self.handle_grpc_error(e)
            return None


    def get_tuple_spaces_state(self) -> str:
        self.debug("Calling get_tuple_spaces_state()")
        try:
            request = pb2.getTupleSpacesStateRequest()
            response = self.stub.getTupleSpacesState(request)
            self.debug(f"get_tuple_spaces_state operation successful, server returned: {str(response.tuple)}")
            return "" if not response.tuple else response.tuple
        except grpc.RpcError as e:
            self.handle_grpc_error(e)
            return None
    
    def shutdown(self):
        self.channel.close()


    def handle_grpc_error(self, e: grpc.RpcError):
         
        # handles gRPC errors and provides error messages.

        code = e.code()
        description = e.details()

        
        if code == grpc.StatusCode.UNAVAILABLE:
            if description == "[ERROR] Server is unavailable":
                print("[ERROR] The server is unavailable.", file=sys.stderr)
            else:
                print("[ERROR] The front-end is unavailable.", file=sys.stderr)
        elif code == grpc.StatusCode.CANCELLED:
            print("[ERROR] Request was interrupted", file=sys.stderr)
        else:
            print(f"[ERROR] An unexpected error occurred: {description}", file=sys.stderr)