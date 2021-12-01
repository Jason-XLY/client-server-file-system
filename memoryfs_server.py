import hashlib
import pickle, logging
import argparse

# For locks: RSM_UNLOCKED=0 , RSM_LOCKED=1 
RSM_UNLOCKED = bytearray(b'\x00') * 1
RSM_LOCKED = bytearray(b'\x01') * 1

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler


# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


class DiskBlocks():
    def __init__(self, total_num_blocks, block_size):
        # This class stores the raw block array
        self.block = []
        self.checksum = []
        # Initialize raw blocks
        for i in range(0, total_num_blocks):
            putdata = bytearray(block_size)
            self.block.insert(i, putdata)
            self.checksum.insert(i, hashlib.md5(bytes(str(putdata), 'utf-8')).digest())


if __name__ == "__main__":

    # Construct the argument parser
    ap = argparse.ArgumentParser()

    ap.add_argument('-nb', '--total_num_blocks', type=int, help='an integer value')
    ap.add_argument('-bs', '--block_size', type=int, help='an integer value')
    ap.add_argument('-port', '--port', type=int, help='an integer value')
    ap.add_argument('-sid', '--sid', type=int, help='an integer value')
    ap.add_argument('-cblk', '--corrupted_block', type=int, help='an integer value')
    ap.add_argument('-ns', '--num_servers', type=int, help='an integer value')

    args = ap.parse_args()

    if args.total_num_blocks:
        TOTAL_NUM_BLOCKS = args.total_num_blocks
    else:
        print('Must specify total number of blocks')
        quit()

    if args.block_size:
        BLOCK_SIZE = args.block_size
    else:
        print('Must specify block size')
        quit()

    if args.port:
        PORT = args.port
    else:
        print('Must specify port number')
        quit()

    if args.sid >= 0:
        SERVER_ID = args.sid
    else:
        print('Must specify server ID')
        quit()

    if args.num_servers:
        NUM_SERVERS = args.num_servers
    else:
        print('Must specify number of servers')
        quit()

    Corrupt = False

    if args.corrupted_block:
        BLOCK_ID = args.corrupted_block
        Corrupt = True
    else:
        pass

    # args.corrupted_block

    # initialize blocks
    RawBlocks = DiskBlocks(TOTAL_NUM_BLOCKS, BLOCK_SIZE)

    # Create server
    server = SimpleXMLRPCServer(("127.0.0.1", PORT), requestHandler=RequestHandler)


    def Get(block_number):
        bad_block = False
        if hashlib.md5(bytearray(str(RawBlocks.block[0]), 'utf-8')).digest() != RawBlocks.checksum[block_number]:
            bad_block = True
        if Corrupt and block_number == BLOCK_ID:
            return bytearray(BLOCK_SIZE), Corrupt
        result = RawBlocks.block[block_number]
        return result, bad_block

    server.register_function(Get)

    def Put(block_number, data):
        RawBlocks.block[block_number] = data
        RawBlocks.checksum[block_number] = hashlib.md5(data.data).digest()
        return 0


    server.register_function(Put)


    def RSM(block_number):
        result = RawBlocks.block[block_number]
        # RawBlocks.block[block_number] = RSM_LOCKED
        RawBlocks.block[block_number] = bytearray(RSM_LOCKED.ljust(BLOCK_SIZE, b'\x01'))
        return result


    server.register_function(RSM)

    # Run the server's main loop
    print("Running block server with nb=" + str(TOTAL_NUM_BLOCKS) + ", bs=" + str(BLOCK_SIZE) + " on port " + str(PORT))
    server.serve_forever()
