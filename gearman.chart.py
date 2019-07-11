# Description: dovecot netdata python.d module
# Author: Kyle Agronick
# SPDX-License-Identifier: GPL-3.0+

# Gearman Netdata Plugin

from bases.FrameworkServices.SocketService import SocketService


class Service(SocketService):
    def __init__(self, configuration=None, name=None):
        SocketService.__init__(self, configuration=configuration, name=name)
        self.request = "status\n"
        self._keep_alive = True

    def create(self):
        self.order = []
        self.definitions = {}

        for worker in sorted([row[0] for row in self._get_worker_data()]):
            self.order.append(worker)
            self.definitions[worker] = {
                'options': [None, worker, 'workers', 'workers', 'gearman.' + worker, 'stacked'],
                'lines': [
                    [worker + '_queued', 'Queued', 'absolute'],
                    [worker + '_idle', 'Idle', 'absolute'],
                    [worker + '_active', 'Active', 'absolute'],
                ]
            }

        return super(Service, self).create()

    def _get_data(self):
        """
        Format data received from socket
        :return: dict
        """

        workers = self._get_worker_data()

        if getattr(self, 'definitions', None) is None:
            self.definitions = {}

        if getattr(self, 'order', None) is None:
            self.order = []

        total = {}

        for worker in workers:
            total.update(self._build_worker(worker))
            if worker[0] not in self.order:
                self.order.append(worker[0])

        return total

    def _get_worker_data(self):
        try:
            raw = self._get_raw_data()
        except (ValueError, AttributeError):
            return None

        if raw is None:
            self.debug("Gearman returned no data")
            return None

        return [worker.split() for worker in raw.splitlines() if '.' not in worker]

    def _build_worker(self, worker):

        total, running, available = map(int, worker[1:])

        idle = available - running
        pending = total - running

        return {
            worker[0] + '_queued': pending,
            worker[0] + '_idle': idle,
            worker[0] + '_active': running,
        }


