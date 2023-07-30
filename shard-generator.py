def get_valid_host_port():
    while True:
        try:
            host = input("Enter the host for the replica: ")
            port = int(input("Enter the port for the replica: "))

            # Validate host and port (you can add more checks based on your requirements)
            if not host or not port or port <= 0 or port > 65535:
                raise ValueError

            return host, port
        except ValueError:
            print("Invalid input. Please enter a valid host and a valid port (1-65535).")


def generate_sharding_config(data_volume, replication_factor, num_shards):
    config_template = """
<yandex>
    <profiles>
        <!-- Define your query profiles here -->
    </profiles>
    <remote_servers>
        <!-- Define remote servers here if needed -->
    </remote_servers>
    <clickhouse_remote_servers>
        <!-- Define ClickHouse remote servers here if needed -->
    </clickhouse_remote_servers>
</yandex>

<databases>
    <default>
        {shard_configs}
    </default>
</databases>
"""

    shard_config_template = """
        <shard>
            {replica_configs}
        </shard>
"""

    replica_config_template = """
            <replica>
                <host>{replica_host}</host>
                <port>{replica_port}</port>
            </replica>
"""

    shard_configs = ""
    for shard_index in range(1, num_shards + 1):
        replica_configs = ""
        for replica_index in range(1, replication_factor + 1):
            print(f"Enter details for Shard {shard_index}, Replica {replica_index}:")
            replica_host, replica_port = get_valid_host_port()
            replica_config = replica_config_template.format(replica_host=replica_host, replica_port=replica_port)
            replica_configs += replica_config

        shard_config = shard_config_template.format(replica_configs=replica_configs)
        shard_configs += shard_config

    full_config = config_template.format(shard_configs=shard_configs)
    return full_config


if __name__ == "__main__":
    print("Welcome to ClickHouse Sharding Configuration Generator!")

    try:
        data_volume = int(input("Enter the total data volume (in GB): "))
        replication_factor = int(input("Enter the replication factor (number of replicas per shard): "))
        num_shards = int(input("Enter the number of shards: "))

        generated_config = generate_sharding_config(data_volume, replication_factor, num_shards)

        with open("clickhouse_sharding_config.xml", "w") as f:
            f.write(generated_config)

        print("Sharding configuration has been generated and saved to 'clickhouse_sharding_config.xml'.")
    except ValueError:
        print("Invalid input. Please enter valid integers for data volume, replication factor, and number of shards.")
    except Exception as e:
        print("An error occurred while generating the sharding configuration:", str(e))
