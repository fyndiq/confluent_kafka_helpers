def get_bootstrap_servers(config):
    bootstrap_servers = config['bootstrap.servers']
    enable_mock = any(
        [
            server.startswith('mock://')
            for server in bootstrap_servers.split(',')
        ]
    )
    return bootstrap_servers, enable_mock
