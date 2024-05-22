import threading
import socket
import json
from collections import OrderedDict
from datetime import datetime, timedelta
from queue import Queue
import time

"""
The DistributedLRUCache class is a wrapper around the OrderedDict class
from the collections module. The OrderedDict class is used to maintain a
cache of key-value pairs in the order in which they were inserted. The
DistributedLRUCache class also provides methods to clean up expired entries
from the cache and send messages to peers.

- This class can be initialized at multiple locations let's say USA and Asia. 
- The data persists locally on both locations.
- When a change is made to the cache, it is sent to the peers as a message via a socket.
- Peers are always listening to messages and consume the message to update the cache.
- The value which is last set is evicted first.
- The get operation does not change the priority on who gets evicted. (This will increase latency)
- The values also have an expiration time. When the expiration time is reached, the entry is evicted.

1 - Simplicity. Integration needs to be dead simple.
> It is as simple as initializing this cache class and set the peers.
> I can make a global service delivery class which will handle the peers existence.

2 - Resilient to network failures or crashes.
> We have multiple retires to connect to the peers.

3 - Near real time replication of data across Geolocation. Writes need to be in real time.
> Writes are real time. every time a value is set, it sends a message to peers.

4 - Data consistency across regions
> The data will be consistent across regions as we use the version filed in message to confirm consistency.

5 - Locality of reference, data should almost always be available from the closest region
> The data will be replicated to all the peers.

6 - Flexible Schema
> The data is very flexible. The value field in the message can be any data type.

7 - Cache can expire
> I store a expiry time in the cache. When the expiry time is reached, the entry is evicted.
"""

class Message: 
    def __init__(self, key, value, version, region): 
        self.key = key 
        self.value = value 
        self.version = version 
        self.region = region 

class DistributedLRUCache:
    def __init__(self, max_size, port, peer_ports, region, ttl=None):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.port = port
        self.peer_ports = peer_ports
        self.ttl = ttl
        self.expiration_times = {}
        self.lock = threading.Lock()
        self.message_queue = Queue()
        self.region = region
        self.expected_versions = {}

        self.server_thread = threading.Thread(target=self._run_server)
        self.server_thread.daemon = True
        self.server_thread.start()

        self.consumer_thread = threading.Thread(target=self._consume_updates)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()

    def get(self, key):
        """
        Retrieves the value associated with the given key from the cache.

        Parameters:
            key (Any): The key to retrieve the value for.

        Returns:
            Any: The value associated with the key, or None if the key is not found in the cache.
        """
        with self.lock:
            self._cleanup_expired()
            if key in self.cache:
                value = self.cache.pop(key)
                self.cache[key] = value
                return value

    def set(self, key, value):
        """
        Sets the value of a key in the cache. If the key already exists, its value is updated. 
        If the cache is full, the least recently used item is removed.
        
        :param key: The key to set the value for.
        :type key: Any hashable type.
        :param value: The value to set for the key.
        :type value: Any type.
        :return: None
        """
        with self.lock:
            self._cleanup_expired()
            version = self.expected_versions.get(key, 0) + 1
            if key in self.cache:
                self.cache.pop(key)
            elif len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)
            self.cache[key] = value
            self.expected_versions[key] = version
            if self.ttl:
                self.expiration_times[key] = datetime.now() + timedelta(seconds=self.ttl)
        self._send_update_async(key, value, version)

    def _cleanup_expired(self):
        """
        Cleans up expired entries from the cache.

        This method checks for expired entries in the cache and removes them.
        If the time-to-live (ttl) is not set, the method does nothing.
        Otherwise, it iterates over the keys in the cache and checks if the
        expiration time for each key is less than or equal to the current time.
        If an expired entry is found, it is removed from the cache and its
        corresponding expiration time is deleted.

        Parameters:
            self (DistributedLRUCache): The instance of the DistributedLRUCache class.

        Returns:
            None
        """
        if not self.ttl:
            return
        now = datetime.now()
        for key in list(self.cache.keys()):
            if self.expiration_times.get(key, datetime.max) <= now:
                self.cache.pop(key)
                del self.expiration_times[key]

    def _run_server(self):
        """
        Runs a server that listens for incoming client connections and handles them in separate threads.

        This method creates a socket and binds it to the specified address and port. 
        It then listens for incoming connections and accepts them using the `accept()` method. 
        For each accepted connection, a new thread is created to handle the client using the `handle_client()` method.

        Parameters:
            self (DistributedLRUCache): The instance of the DistributedLRUCache class.

        Returns:
            None
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', self.port))
        server.listen(5)
        while True:
            conn, addr = server.accept()
            threading.Thread(target=self._handle_client, args=(conn,)).start()

    def _handle_client(self, conn):
        """
        Handles a client connection by receiving data from the connection and adding it to the message queue.

        Parameters:
            conn (socket.socket): The client connection.

        Returns:
            None
        """
        try:
            data = conn.recv(4096).decode('utf-8')
            if data:
                message = json.loads(data)
                self.message_queue.put(Message(message['key'], message['value'], message['version'], message['region']))
        except Exception as e:
            print(f"Error: {e}")
        finally:
            conn.close()

    def _consume_updates(self):
        """
        Consumes messages from the message queue and updates the cache with the received data.

        This method runs in an infinite loop and continuously retrieves messages from the message queue.
        Each message contains a key-value pair, along with a version number and a region.
        The method checks if the received key is already present in the cache and if the received version is greater than the current version.
        If either of these conditions is true, the method updates the cache with the new value,
        sets the expiration time for the key if a time-to-live (TTL) is specified,
        and updates the expected version for the key.

        Parameters:
            self (DistributedLRUCache): The instance of the DistributedLRUCache class.

        Returns:
            None
        """
        while True:
            try:
                message = self.message_queue.get()
                with self.lock:
                    if (key := message.key) not in self.expected_versions or message.version > self.expected_versions[key]:
                        self.cache[key] = message.value
                        if self.ttl:
                            self.expiration_times[key] = datetime.now() + timedelta(seconds=self.ttl)
                        self.expected_versions[key] = message.version
                self.message_queue.task_done()
            except Exception as e:
                print(f"Error consuming message: {e}")

    def _send_update_async(self, key, value, version):
        message = json.dumps({'action': 'set', 'key': key, 'value': value, 'version': version, 'region': self.region}).encode('utf-8')
        for peer_port in self.peer_ports:
            threading.Thread(target=self._send_message, args=(peer_port, message)).start()

    def _send_message(self, port, message):
        """
        Sends a message to a peer at the specified port.

        Args:
            port (int): The port number of the peer.
            message (bytes): The message to send.

        Raises:
            Exception: If there is an error connecting to the peer or if the message cannot be sent after the specified number of retries.

        Returns:
            None: If the message is successfully sent.

        """
        retries = 9
        for _ in range(retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(('localhost', port))
                    s.sendall(message)
                    return
            except Exception as e:
                print(f"Error connecting to peer at port {port}: {e}")
            time.sleep(5)
        print(f"Failed to send message to port {port} after {retries} retries")


import unittest

class TestDistributedLRUCache(unittest.TestCase):
    def test_set_get(self):
        cache = DistributedLRUCache(max_size=3, port=5001, peer_ports=[], region="us-east", ttl=None)
        cache.set('a', '1')
        cache.set('b', '2')
        cache.set('c', '3')
        self.assertEqual(cache.get('a'), '1')
        self.assertEqual(cache.get('b'), '2')
        self.assertEqual(cache.get('c'), '3')
        self.assertIsNone(cache.get('d'))
        cache.set('d', '4')
        self.assertIsNone(cache.get('a')) # a was added first so evicted first.
        self.assertEqual(cache.get('b'), '2')
        self.assertEqual(cache.get('c'), '3')
        self.assertEqual(cache.get('d'), '4')


    def test_eviction(self):
        cache = DistributedLRUCache(max_size=3, port=5002, peer_ports=[], region="us-east", ttl=None)
        cache.set('a', '1')
        cache.set('b', '2')
        cache.set('c', '3')
        cache.set('d', '4') # a will be evicted first.
        self.assertIsNone(cache.get('a'))
        self.assertEqual(cache.get('b'), '2')
        self.assertEqual(cache.get('c'), '3')
        self.assertEqual(cache.get('d'), '4')

    def test_eviction_multiple(self):
        cache = DistributedLRUCache(max_size=3, port=5012, peer_ports=[], region="us-east", ttl=None)
        cache.set('a', '1')
        cache.set('b', '2')
        cache.set('c', '3')
        cache.set('d', '4') # a will be evicted first.
        self.assertIsNone(cache.get('a'))
        self.assertEqual(cache.get('b'), '2')
        self.assertEqual(cache.get('c'), '3')
        self.assertEqual(cache.get('d'), '4')
        cache.set('e', '5')
        self.assertIsNone(cache.get('b'))
        cache.set('f', '6')
        self.assertIsNone(cache.get('c'))


    def test_ttl(self):
        cache = DistributedLRUCache(max_size=3, port=5003, peer_ports=[], region="us-east", ttl=1)
        cache.set('a', '1')
        time.sleep(1.5)
        self.assertIsNone(cache.get('a'))


    def test_concurrent_access(self):
        cache = DistributedLRUCache(max_size=3, port=5004, peer_ports=[5005], region="us-east", ttl=None)
        cache2 = DistributedLRUCache(max_size=3, port=5005, peer_ports=[5004], region="us-west", ttl=None)
        cache.set('a', '1')
        cache.set('b', '2')
        cache.set('c', '3')
        time.sleep(1)
        self.assertEqual(cache2.get('a'), '1')
        self.assertEqual(cache2.get('b'), '2')
        self.assertEqual(cache2.get('c'), '3')

        self.assertEqual(cache2.get('a'), '1')
        self.assertEqual(cache2.get('b'), '2')
        self.assertEqual(cache2.get('c'), '3')
    

    def test_send_message(self):
        cache = DistributedLRUCache(max_size=3, port=5007, peer_ports=[5006], region="us-east", ttl=None)
        cache.set('a', '1')
        time.sleep(1)
        self.assertEqual(cache.get('a'), '1')
        cache2 = DistributedLRUCache(max_size=3, port=5006, peer_ports=[5007], region="us-west", ttl=None)
        cache._send_message(5006, b'{"action": "set", "key": "a", "value": "2", "version": 1, "region": "us-east"}')
        self.assertEqual(cache.get('a'), '1')
        self.assertEqual(cache2.get('a'), '2')

if __name__ == "__main__":
    unittest.main()