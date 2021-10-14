from grpc_requests import StubClient
from databaserep_pb2 import DESCRIPTOR
import psycopg2
from psycopg2.extras import LogicalReplicationConnection, StopReplication
import sys
sys.path.append("./")
from credentials import postgresUserID, postgresPassword

service_descriptor = DESCRIPTOR.services_by_name['Replicator']


client = StubClient.get_by_endpoint(
    "localhost:50051", service_descriptors=[service_descriptor, ])
assert client.service_names == ["Replicator"]
replicator = client.service("Replicator")

try:
    username, password = postgresUserID, postgresPassword
    my_connection = psycopg2.connect(
        "dbname='college' user=" + username + " password=" + password,
        connection_factory=LogicalReplicationConnection)
    cur = my_connection.cursor()
    cur.drop_replication_slot('wal2json_slot')
    cur.create_replication_slot('wal2json_slot', output_plugin='wal2json')
    cur.start_replication(slot_name='wal2json_slot', options={
        'pretty-print': 1}, decode=True)

    def consume(request_data):
        if 'stop_repl' in request_data.payload:
            # cur.drop_replication_slot('wal2json_slot')
            raise StopReplication()
        result = replicator.Replicate({"query": request_data.payload})
        print(result["status"])

    try:
        cur.consume_stream(consume)
    except StopReplication:
        # cur.close()
        print('Stopping Replication')

except KeyboardInterrupt:
    cur.drop_replication_slot('wal2json_slot')
    print('Interrupted')
