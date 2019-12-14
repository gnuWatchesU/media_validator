import transmissionrpc

class TransmissionClient:
    def __init__(self, address='localhost', port=9091, user=None, password=None, http_handler=None, timeout=None):
        self.client = transmissionrpc.Client(address=address, port=port, user=user, password=password,
                                             http_handler=http_handler, timeout=timeout)

    def find_torrent(self, torrent_name):
        for tor in self.client.get_torrents():
            if torrent_name is "{}/{}".format(tor.downloadDir, tor.name):
                return tor.id
            else:
                continue
        return False

    def recheck_torrent(self, torrent_id):
        self.client.verify_torrent(torrent_id)

    def is_torrent_ready(self, torrent_id):
        tor = self.client.get_torrent(torrent_id)
        tor_status = tor.status
        if tor_status is "seeding":
            return True
        elif tor_status is "stopped":
            return tor.doneDate is not 0
        else:
            return False