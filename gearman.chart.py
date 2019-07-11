# Description: dovecot netdata python.d module
# Author: Kyle Agronick
# SPDX-License-Identifier: GPL-3.0+

# Gearman Netdata Plugin

from bases.FrameworkServices.SocketService import SocketService
from copy import deepcopy

update_every = 3
priority = 60500


CHARTS = {
    'workers': {
        'options': [None, 'Total Workers', 'workers', 'workers', 'gearman.workers', 'stacked'],
        'lines': [
            ['total_queued', 'Queued', 'absolute'],
            ['total_idle', 'Idle', 'absolute'],
            ['total_active', 'Active', 'absolute'],
        ]
    },
}


class Service(SocketService):
    def __init__(self, configuration=None, name=None):
        SocketService.__init__(self, configuration=configuration, name=name)
        self.request = "status\n"
        self._keep_alive = True

        self.host = self.configuration.get('host', 'localhost')
        self.port = self.configuration.get('port', 4730)

        self.hide_total = self.configuration.get('hide_total')
        self.definitions = deepcopy(CHARTS) if not self.hide_total else {}
        self.monitor_jobs = self.configuration.get('jobs', [])

        if self.configuration.get('autodetect_jobs', len(self.monitor_jobs) == 0):
            self._auto_add_jobs()

        self.order = ['workers'] + self.monitor_jobs

        for job in self.monitor_jobs:
            self.order.append(job)
            self.definitions[job] = {
                'options': [None, job, 'workers', 'workers', 'gearman.{0}'.format(job), 'stacked'],
                'lines': [
                    ['{0}_queued'.format(job), 'Queued', 'absolute'],
                    ['{0}_idle'.format(job), 'Idle', 'absolute'],
                    ['{0}_active'.format(job), 'Active', 'absolute'],
                ]
            }

    def _auto_add_jobs(self):

        """
        Get a list of active jobs from Gearman
        and make tables for them
        :return: None
        """

        tasks = sorted([task[0] for task in self._get_worker_data() if any(task[1:])])

        # Don't make a per-task table if there is only one task - it would be redundant
        if len(tasks) > 1:
            for task in tasks:
                if task not in self.monitor_jobs:
                    self.monitor_jobs.append(task)

    def _get_data(self):
        """
        Format data received from socket
        :return: dict
        """

        jobs = self._get_worker_data()

        output = {
            'total_queued': 0,
            'total_idle': 0,
            'total_active': 0,
        }

        for job in jobs:
            job_data = self._build_job(job)
            job_name = job[0]
            if job_name in self.monitor_jobs:
                output.update(job_data)

            if not self.hide_total:
                for sum_value in ('queued', 'idle', 'active'):
                    output['total_{0}'.format(sum_value)] += job_data['{0}_{1}'.format(job_name, sum_value)]

        return output

    def _get_worker_data(self):
        """
        Split the data returned from Gearman into a list of lists
        :return: list
        """

        try:
            raw = self._get_raw_data()
        except (ValueError, AttributeError):
            return None

        if raw is None:
            self.debug("Gearman returned no data")
            return None

        return [job.split() for job in raw.splitlines()][:-1]

    def _build_job(self, job):
        """
        Get the status for each job
        :return: dict
        """

        total, running, available = map(int, job[1:])

        idle = available - running
        pending = total - running

        return {
            '{0}_queued'.format(job[0]): pending,
            '{0}_idle'.format(job[0]): idle,
            '{0}_active'.format(job[0]): running,
        }


